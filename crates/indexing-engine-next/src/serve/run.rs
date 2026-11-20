use std::sync::Arc;
use std::time::SystemTime;

use crate::builder::ErasedWorker;
use crate::port::{Delivery, LeaseWrite, MarkerWrite, TaskMessage, WorkerKey};
use crate::protocol::{Claim, Fence, Hold, ProtocolEvent, Renewal, TaskId, start_clears_marker};
use crate::serve::{Process, submit_task};
use crate::worker::{EncodedOutcome, TaskContext, TaskError};

impl Process {
    pub(super) async fn run_delivery(self: Arc<Self>, delivery: Delivery) {
        let message = &delivery.message;
        let Some(worker) = self.registry.find_worker(&message.worker).cloned() else {
            orbit_log_fields::warn!(
                safe(ERROR_TYPE) = "unknown_worker",
                "no module registered the worker of a delivered task"
            );
            return;
        };
        let key = message.worker_key();
        let mut hold = if self.registry.is_exclusive(&message.worker) {
            Some(self.acquire_lease(&key, message.task).await)
        } else {
            None
        };
        let fence = hold.as_ref().map(Hold::fence);

        let attempt = tokio::select! {
            biased;
            () = self.keep_lease(hold.as_mut(), &key, &delivery) => return,
            attempt = self.start_and_run(worker.as_ref(), &key, message, fence) => attempt,
        };
        match attempt {
            Ok(outcome) => self.finish(hold.as_ref(), &key, &delivery, outcome).await,
            Err(error) => self.abandon(message.task, &error).await,
        }
    }

    async fn acquire_lease(&self, key: &WorkerKey, task: TaskId) -> Hold {
        let claim = Claim::deliver(self.holder.clone(), task);
        loop {
            let now = self.ports.clock.now().await;
            if let Some(hold) = self.try_acquire_lease(&claim, key, now).await {
                return hold;
            }
            self.ports
                .clock
                .sleep_until(now + self.rules.renew_every)
                .await;
        }
    }

    async fn try_acquire_lease(
        &self,
        claim: &Claim,
        key: &WorkerKey,
        now: SystemTime,
    ) -> Option<Hold> {
        let entry = self
            .ports
            .leases
            .read(key)
            .await
            .inspect_err(|error| {
                orbit_log_fields::warn!(
                    safe(ERROR_TYPE) = "lease_read_failed",
                    expanded(ERROR_MESSAGE) = %error,
                    "the lease read failed"
                )
            })
            .ok()?;
        let lease = claim.acquire_lease(&self.rules, &entry, now)?;
        match self
            .ports
            .leases
            .write(key, entry.revision, Some(lease.clone()))
            .await
        {
            Ok(LeaseWrite::Written {
                revision,
                generation,
            }) => {
                let hold = claim
                    .clone()
                    .hold(&self.rules, &lease, revision, generation, now);
                self.ports
                    .log
                    .record(now, ProtocolEvent::LeaseAcquired(hold.clone()));
                Some(hold)
            }
            Ok(LeaseWrite::Conflict) => None,
            Err(error) => {
                orbit_log_fields::warn!(
                    safe(ERROR_TYPE) = "lease_write_failed",
                    expanded(ERROR_MESSAGE) = %error,
                    "the lease write failed"
                );
                None
            }
        }
    }

    async fn keep_lease(&self, hold: Option<&mut Hold>, key: &WorkerKey, delivery: &Delivery) {
        let Some(hold) = hold else {
            return std::future::pending().await;
        };
        loop {
            self.ports.clock.sleep_until(hold.renew_at()).await;
            if !self.renew_lease(hold, key, delivery).await {
                return;
            }
        }
    }

    async fn renew_lease(&self, hold: &mut Hold, key: &WorkerKey, delivery: &Delivery) -> bool {
        loop {
            let now = self.ports.clock.now().await;
            let entry = match self.ports.leases.read(key).await {
                Ok(entry) => entry,
                Err(error) => return self.drop_failed_renewal(now, hold, &error),
            };
            let lease = match hold.renew(&self.rules, &entry, now) {
                Renewal::Extend(lease) => lease,
                Renewal::Lost => {
                    self.ports
                        .log
                        .record(now, ProtocolEvent::RenewalLost(hold.task()));
                    return false;
                }
            };
            match self
                .ports
                .leases
                .write(key, entry.revision, Some(lease.clone()))
                .await
            {
                Ok(LeaseWrite::Written { revision, .. }) => {
                    hold.renewed(&self.rules, &lease, revision, now);
                    if let Err(error) = delivery.handle.acknowledge_progress().await {
                        orbit_log_fields::warn!(
                            safe(ERROR_TYPE) = "progress_ack_failed",
                            expanded(ERROR_MESSAGE) = %error,
                            "the progress ack of a renewed task failed"
                        );
                    }
                    self.ports
                        .log
                        .record(now, ProtocolEvent::Renewed(hold.clone()));
                    return true;
                }
                Ok(LeaseWrite::Conflict) => continue,
                Err(error) => return self.drop_failed_renewal(now, hold, &error),
            }
        }
    }

    fn drop_failed_renewal(&self, now: SystemTime, hold: &Hold, error: &anyhow::Error) -> bool {
        orbit_log_fields::warn!(
            safe(ERROR_TYPE) = "lease_renewal_failed",
            expanded(ERROR_MESSAGE) = %error,
            "the lease renewal failed, so the run stops"
        );
        self.ports
            .log
            .record(now, ProtocolEvent::RenewalFailed(hold.task()));
        false
    }

    async fn start_and_run(
        &self,
        worker: &dyn ErasedWorker,
        key: &WorkerKey,
        message: &TaskMessage,
        fence: Option<Fence>,
    ) -> Result<EncodedOutcome, TaskError> {
        self.start_task(key, message.task).await?;
        let ctx = TaskContext::new(Arc::clone(&self.pools), fence);
        worker.run(&ctx, &message.input).await
    }

    async fn start_task(&self, key: &WorkerKey, task: TaskId) -> anyhow::Result<()> {
        loop {
            let now = self.ports.clock.now().await;
            let entry = self.ports.markers.read(key).await?;
            let marker = entry.pending.as_ref().map(|pending| pending.task);
            if !start_clears_marker(marker, task) {
                self.ports.log.record(
                    now,
                    ProtocolEvent::Started {
                        task,
                        cleared_marker: false,
                    },
                );
                return Ok(());
            }
            match self.ports.markers.write(key, entry.revision, None).await? {
                MarkerWrite::Written => {
                    self.ports.log.record(
                        now,
                        ProtocolEvent::Started {
                            task,
                            cleared_marker: true,
                        },
                    );
                    return Ok(());
                }
                MarkerWrite::Conflict => continue,
            }
        }
    }

    async fn finish(
        &self,
        hold: Option<&Hold>,
        key: &WorkerKey,
        delivery: &Delivery,
        outcome: EncodedOutcome,
    ) {
        let message = &delivery.message;
        for child in outcome.children {
            let child =
                TaskMessage::from_input(message.tenant.clone(), child, message.lane.clone());
            if let Err(error) = submit_task(&self.ports, child).await {
                return self.abandon(message.task, &TaskError::from(error)).await;
            }
        }

        let now = self.ports.clock.now().await;
        if let Err(error) = delivery.handle.acknowledge().await {
            orbit_log_fields::warn!(
                safe(ERROR_TYPE) = "ack_failed",
                expanded(ERROR_MESSAGE) = %error,
                "the ack of a finished task failed"
            );
        }
        let released = match hold {
            Some(hold) => self.release_lease(hold, key).await,
            None => false,
        };
        self.ports.log.record(
            now,
            ProtocolEvent::Finished {
                task: message.task,
                released,
            },
        );
    }

    async fn release_lease(&self, hold: &Hold, key: &WorkerKey) -> bool {
        loop {
            let entry = match self.ports.leases.read(key).await {
                Ok(entry) => entry,
                Err(error) => {
                    orbit_log_fields::warn!(
                        safe(ERROR_TYPE) = "lease_read_failed",
                        expanded(ERROR_MESSAGE) = %error,
                        "the lease read before the release failed"
                    );
                    return false;
                }
            };
            let Some(revision) = hold.release(&entry) else {
                return false;
            };
            match self.ports.leases.write(key, revision, None).await {
                Ok(LeaseWrite::Written { .. }) => return true,
                Ok(LeaseWrite::Conflict) => continue,
                Err(error) => {
                    orbit_log_fields::warn!(
                        safe(ERROR_TYPE) = "lease_release_failed",
                        expanded(ERROR_MESSAGE) = %error,
                        "the lease release failed"
                    );
                    return false;
                }
            }
        }
    }

    async fn abandon(&self, task: TaskId, error: &TaskError) {
        let now = self.ports.clock.now().await;
        orbit_log_fields::warn!(
            safe(ERROR_TYPE) = "run_abandoned",
            expanded(ERROR_MESSAGE) = %error,
            "a run stopped without an ack, so the broker delivers its task again"
        );
        self.ports.log.record(now, ProtocolEvent::Abandoned(task));
    }
}
