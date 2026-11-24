use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;

use super::{Claim, Fence, Hold, HolderId, Lease, LeaseEntry, LeaseRules, Renewal, Revision};
use crate::clock::Clock;
use crate::event::{Failure, Observer, TaskStep};
use crate::ports::Ports;
use crate::task::{InFlight, TaskMessage, WorkerKey};

#[async_trait]
pub(crate) trait LeaseStore: Send + Sync {
    async fn read(&self, key: &WorkerKey) -> anyhow::Result<LeaseEntry>;
    async fn write(
        &self,
        key: &WorkerKey,
        expected: Revision,
        lease: Option<&Lease>,
    ) -> anyhow::Result<LeaseWrite>;
}

pub(crate) enum LeaseWrite {
    Written(Fence),
    Conflict,
}

pub(crate) struct Leases {
    store: Arc<dyn LeaseStore>,
    clock: Arc<dyn Clock>,
    observer: Arc<dyn Observer>,
    holder: HolderId,
    rules: LeaseRules,
}

impl Leases {
    pub(crate) fn new(ports: &Ports, holder: HolderId, rules: LeaseRules) -> Self {
        Self {
            store: Arc::clone(&ports.leases),
            clock: Arc::clone(&ports.clock),
            observer: Arc::clone(&ports.observer),
            holder,
            rules,
        }
    }

    pub(crate) async fn acquire_or_give_back(
        &self,
        message: &TaskMessage,
        in_flight: &dyn InFlight,
    ) -> anyhow::Result<Option<Hold>> {
        let now = self.clock.now().await;
        let claim = Claim::new(self.holder.clone(), message.task);
        match self.take_if_free(claim, &message.worker_key(), now).await? {
            Some(hold) => {
                let acquired = TaskStep::LeaseAcquired(hold.clone());
                self.observer.observe(&message.build_event(now, acquired));
                Ok(Some(hold))
            }
            None => {
                let delay = self.rules.renew_every;
                if let Err(error) = in_flight.negative_acknowledge(delay).await {
                    let failed =
                        TaskStep::PortCallFailed(Failure::new("negative_ack_failed", error));
                    self.observer.observe(&message.build_event(now, failed));
                }
                let given_back = TaskStep::GivenBack { delay };
                self.observer.observe(&message.build_event(now, given_back));
                Ok(None)
            }
        }
    }

    pub(crate) async fn renew_while_running(
        &self,
        hold: &mut Hold,
        message: &TaskMessage,
        in_flight: &dyn InFlight,
        started_at: SystemTime,
    ) {
        let key = message.worker_key();
        let mut now = started_at;
        loop {
            let wait = hold.renew_at().duration_since(now).unwrap_or_default();
            self.clock.sleep(wait).await;
            now = self.clock.now().await;
            match self.extend_if_ours(hold, &key, now).await {
                Ok(true) => {
                    if let Err(error) = in_flight.acknowledge_progress().await {
                        let failure = Failure::new("progress_ack_failed", error);
                        let failed = TaskStep::PortCallFailed(failure);
                        self.observer.observe(&message.build_event(now, failed));
                    }
                    let renewed = TaskStep::Renewed(hold.clone());
                    self.observer.observe(&message.build_event(now, renewed));
                }
                Ok(false) => {
                    let lost = TaskStep::RenewalLost;
                    self.observer.observe(&message.build_event(now, lost));
                    return;
                }
                Err(error) => {
                    let failure = Failure::new("lease_renewal_failed", error);
                    let failed = TaskStep::RenewalFailed(failure);
                    self.observer.observe(&message.build_event(now, failed));
                    return;
                }
            }
        }
    }

    pub(crate) async fn release(&self, hold: &Hold, key: &WorkerKey) -> anyhow::Result<bool> {
        let entry = self.store.read(key).await?;
        let Some(revision) = hold.revision_to_release(&entry) else {
            return Ok(false);
        };
        let written = self.store.write(key, revision, None).await?;
        Ok(matches!(written, LeaseWrite::Written(_)))
    }

    async fn take_if_free(
        &self,
        claim: Claim,
        key: &WorkerKey,
        now: SystemTime,
    ) -> anyhow::Result<Option<Hold>> {
        let entry = self.store.read(key).await?;
        let Some(lease) = claim.lease_if_free(&self.rules, &entry, now) else {
            return Ok(None);
        };
        let written = self.store.write(key, entry.revision, Some(&lease)).await?;
        let LeaseWrite::Written(fence) = written else {
            return Ok(None);
        };
        Ok(Some(claim.into_hold(&self.rules, &lease, fence, now)))
    }

    async fn extend_if_ours(
        &self,
        hold: &mut Hold,
        key: &WorkerKey,
        now: SystemTime,
    ) -> anyhow::Result<bool> {
        let entry = self.store.read(key).await?;
        let lease = match hold.build_renewal(&self.rules, &entry, now) {
            Renewal::Extend(lease) => lease,
            Renewal::Lost => return Ok(false),
        };
        let written = self.store.write(key, entry.revision, Some(&lease)).await?;
        let LeaseWrite::Written(fence) = written else {
            return Ok(false);
        };
        hold.record_renewal(&self.rules, &lease, fence.revision, now);
        Ok(true)
    }
}
