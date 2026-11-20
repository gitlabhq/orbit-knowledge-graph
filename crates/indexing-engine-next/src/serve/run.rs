use std::sync::Arc;
use std::time::SystemTime;

use crate::builder::ErasedWorker;
use crate::port::{Delivery, LeaseWrite, MarkerWrite, TaskMessage, WorkerKey};
use crate::protocol::{Claim, Hold, ProtocolEvent, Renewal, TaskId};
use crate::serve::{Process, submit_task};
use crate::worker::{EncodedOutcome, TaskContext, TaskError};

impl Process {
    pub(super) async fn run_delivered_task(self: Arc<Self>, delivery: Delivery) {
        let worker_name = &delivery.message.worker;
        let Some(worker) = self.registry.find_worker(worker_name).cloned() else {
            orbit_log_fields::warn!(
                safe(ERROR_TYPE) = "unknown_worker",
                "no module registered the worker of a delivered task"
            );
            return;
        };
        if self.registry.is_exclusive(worker_name) {
            self.run_with_lease(worker.as_ref(), delivery).await;
        } else {
            self.run_without_lease(worker.as_ref(), delivery).await;
        }
    }

    async fn run_with_lease(&self, worker: &dyn ErasedWorker, delivery: Delivery) {
        let message = &delivery.message;
        let key = message.worker_key();
        let mut hold = self.wait_for_lease(&key, message.task).await;
        if let Err(error) = self.start_task(&key, message.task).await {
            return self
                .abandon_run(message.task, &TaskError::from(error))
                .await;
        }

        let ctx = TaskContext::new(Arc::clone(&self.pools), Some(hold.fence()));
        let outcome = tokio::select! {
            biased;
            () = self.renew_lease_until_lost(&mut hold, &key, &delivery) => return,
            outcome = worker.run(&ctx, &message.input) => outcome,
        };
        match outcome {
            Ok(outcome) => {
                self.finish_task(Some(&hold), &key, &delivery, outcome)
                    .await
            }
            Err(error) => self.abandon_run(message.task, &error).await,
        }
    }

    async fn run_without_lease(&self, worker: &dyn ErasedWorker, delivery: Delivery) {
        let message = &delivery.message;
        let key = message.worker_key();
        if let Err(error) = self.start_task(&key, message.task).await {
            return self
                .abandon_run(message.task, &TaskError::from(error))
                .await;
        }

        let ctx = TaskContext::new(Arc::clone(&self.pools), None);
        match worker.run(&ctx, &message.input).await {
            Ok(outcome) => self.finish_task(None, &key, &delivery, outcome).await,
            Err(error) => self.abandon_run(message.task, &error).await,
        }
    }

    async fn wait_for_lease(&self, key: &WorkerKey, task: TaskId) -> Hold {
        let claim = Claim::new(self.holder.clone(), task);
        loop {
            let now = self.ports.clock.now().await;
            match self.try_take_lease(&claim, key, now).await {
                Ok(Some(hold)) => return hold,
                Ok(None) => {}
                Err(error) => orbit_log_fields::warn!(
                    safe(ERROR_TYPE) = "lease_take_failed",
                    expanded(ERROR_MESSAGE) = %error,
                    "the lease take failed"
                ),
            }
            self.ports
                .clock
                .sleep_until(now + self.rules.renew_every)
                .await;
        }
    }

    async fn try_take_lease(
        &self,
        claim: &Claim,
        key: &WorkerKey,
        now: SystemTime,
    ) -> anyhow::Result<Option<Hold>> {
        let entry = self.ports.leases.read(key).await?;
        let Some(lease) = claim.lease_if_free(&self.rules, &entry, now) else {
            return Ok(None);
        };
        let written = self
            .ports
            .leases
            .write(key, entry.revision, Some(lease.clone()))
            .await?;
        let LeaseWrite::Written {
            revision,
            generation,
        } = written
        else {
            return Ok(None);
        };

        let hold = claim
            .clone()
            .into_hold(&self.rules, &lease, revision, generation, now);
        self.ports
            .log
            .record(now, ProtocolEvent::LeaseAcquired(hold.clone()));
        Ok(Some(hold))
    }

    async fn start_task(&self, key: &WorkerKey, task: TaskId) -> anyhow::Result<()> {
        let now = self.ports.clock.now().await;
        let entry = self.ports.markers.read(key).await?;
        let pending_task = entry.pending.map(|pending| pending.task);
        let cleared_marker = pending_task == Some(task)
            && matches!(
                self.ports.markers.write(key, entry.revision, None).await?,
                MarkerWrite::Written
            );
        self.ports.log.record(
            now,
            ProtocolEvent::Started {
                task,
                cleared_marker,
            },
        );
        Ok(())
    }

    async fn renew_lease_until_lost(&self, hold: &mut Hold, key: &WorkerKey, delivery: &Delivery) {
        loop {
            self.ports.clock.sleep_until(hold.renew_at()).await;
            let now = self.ports.clock.now().await;
            match self.try_renew_lease(hold, key, now).await {
                Ok(true) => {
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
                }
                Ok(false) => {
                    self.ports
                        .log
                        .record(now, ProtocolEvent::RenewalLost(hold.task()));
                    return;
                }
                Err(error) => {
                    orbit_log_fields::warn!(
                        safe(ERROR_TYPE) = "lease_renewal_failed",
                        expanded(ERROR_MESSAGE) = %error,
                        "the lease renewal failed, so the run stops"
                    );
                    self.ports
                        .log
                        .record(now, ProtocolEvent::RenewalFailed(hold.task()));
                    return;
                }
            }
        }
    }

    async fn try_renew_lease(
        &self,
        hold: &mut Hold,
        key: &WorkerKey,
        now: SystemTime,
    ) -> anyhow::Result<bool> {
        let entry = self.ports.leases.read(key).await?;
        let lease = match hold.build_renewal(&self.rules, &entry, now) {
            Renewal::Extend(lease) => lease,
            Renewal::Lost => return Ok(false),
        };
        let written = self
            .ports
            .leases
            .write(key, entry.revision, Some(lease.clone()))
            .await?;
        let LeaseWrite::Written { revision, .. } = written else {
            return Ok(false);
        };
        hold.record_renewal(&self.rules, &lease, revision, now);
        Ok(true)
    }

    async fn finish_task(
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
                return self
                    .abandon_run(message.task, &TaskError::from(error))
                    .await;
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
            Some(hold) => self.release_lease(hold, key).await.unwrap_or_else(|error| {
                orbit_log_fields::warn!(
                    safe(ERROR_TYPE) = "lease_release_failed",
                    expanded(ERROR_MESSAGE) = %error,
                    "the lease release failed, so the lease ends at its expiry"
                );
                false
            }),
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

    async fn release_lease(&self, hold: &Hold, key: &WorkerKey) -> anyhow::Result<bool> {
        let entry = self.ports.leases.read(key).await?;
        let Some(revision) = hold.revision_to_release(&entry) else {
            return Ok(false);
        };
        let written = self.ports.leases.write(key, revision, None).await?;
        Ok(matches!(written, LeaseWrite::Written { .. }))
    }

    async fn abandon_run(&self, task: TaskId, error: &TaskError) {
        let now = self.ports.clock.now().await;
        orbit_log_fields::warn!(
            safe(ERROR_TYPE) = "run_abandoned",
            expanded(ERROR_MESSAGE) = %error,
            "a run stopped without an ack, so the broker delivers its task again"
        );
        self.ports.log.record(now, ProtocolEvent::Abandoned(task));
    }
}
