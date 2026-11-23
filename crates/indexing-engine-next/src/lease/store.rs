use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;

use super::{Claim, Fence, Hold, HolderId, Lease, LeaseEntry, LeaseRules, Renewal, Revision};
use crate::clock::Clock;
use crate::event::{ProtocolEvent, ProtocolLog};
use crate::ports::{Ports, warn_port_failure};
use crate::task::{InFlight, TaskId, WorkerKey};

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
    log: Arc<dyn ProtocolLog>,
    holder: HolderId,
    rules: LeaseRules,
}

impl Leases {
    pub(crate) fn new(ports: &Ports, holder: HolderId, rules: LeaseRules) -> Self {
        Self {
            store: Arc::clone(&ports.leases),
            clock: Arc::clone(&ports.clock),
            log: Arc::clone(&ports.log),
            holder,
            rules,
        }
    }

    pub(crate) async fn acquire_or_give_back(
        &self,
        key: &WorkerKey,
        task: TaskId,
        in_flight: &dyn InFlight,
    ) -> anyhow::Result<Option<Hold>> {
        let now = self.clock.now().await;
        let claim = Claim::new(self.holder.clone(), task);
        match self.take_if_free(claim, key, now).await? {
            Some(hold) => {
                self.log
                    .record(now, ProtocolEvent::LeaseAcquired(hold.clone()));
                Ok(Some(hold))
            }
            None => {
                let delay = self.rules.renew_every;
                if let Err(error) = in_flight.negative_acknowledge(delay).await {
                    warn_port_failure("negative_ack_failed", &error);
                }
                self.log.record(now, ProtocolEvent::GivenBack(task));
                Ok(None)
            }
        }
    }

    pub(crate) async fn renew_while_running(
        &self,
        hold: &mut Hold,
        key: &WorkerKey,
        in_flight: &dyn InFlight,
        started_at: SystemTime,
    ) {
        let mut now = started_at;
        loop {
            let wait = hold.renew_at().duration_since(now).unwrap_or_default();
            self.clock.sleep(wait).await;
            now = self.clock.now().await;
            match self.extend_if_ours(hold, key, now).await {
                Ok(true) => {
                    if let Err(error) = in_flight.acknowledge_progress().await {
                        warn_port_failure("progress_ack_failed", &error);
                    }
                    self.log.record(now, ProtocolEvent::Renewed(hold.clone()));
                }
                Ok(false) => {
                    self.log
                        .record(now, ProtocolEvent::RenewalLost(hold.task()));
                    return;
                }
                Err(error) => {
                    warn_port_failure("lease_renewal_failed", &error);
                    self.log
                        .record(now, ProtocolEvent::RenewalFailed(hold.task()));
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
