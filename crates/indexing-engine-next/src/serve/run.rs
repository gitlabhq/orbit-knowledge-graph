use std::fmt::Display;
use std::sync::Arc;
use std::time::SystemTime;

use crate::builder::ErasedWorker;
use crate::port::{Delivery, LeaseWrite, MarkerWrite, TaskMessage, WorkerKey};
use crate::protocol::{Claim, Hold, ProtocolEvent, Renewal, TaskId};
use crate::serve::{Process, submit_task};
use crate::worker::{EncodedOutcome, TaskContext};

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
        let task = delivery.message.task;
        let key = delivery.message.worker_key();
        let mut hold = self.wait_for_lease(&key, task).await;
        if let Err(error) = self.start_task(&key, task).await {
            return self.abandon_run(task, error).await;
        }

        let ctx = TaskContext::new(Arc::clone(&self.pools), Some(hold.fence()));
        let outcome = tokio::select! {
            biased;
            () = self.extend_running_lease(&mut hold, &key, &delivery) => return,
            outcome = worker.run(&ctx, &delivery.message.input) => outcome,
        };
        match outcome {
            Ok(outcome) => self.finish_task(&delivery, Some(&hold), outcome).await,
            Err(error) => self.abandon_run(task, error).await,
        }
    }

    async fn run_without_lease(&self, worker: &dyn ErasedWorker, delivery: Delivery) {
        let task = delivery.message.task;
        let key = delivery.message.worker_key();
        if let Err(error) = self.start_task(&key, task).await {
            return self.abandon_run(task, error).await;
        }

        let ctx = TaskContext::new(Arc::clone(&self.pools), None);
        match worker.run(&ctx, &delivery.message.input).await {
            Ok(outcome) => self.finish_task(&delivery, None, outcome).await,
            Err(error) => self.abandon_run(task, error).await,
        }
    }

    async fn wait_for_lease(&self, key: &WorkerKey, task: TaskId) -> Hold {
        let claim = Claim::new(self.holder.clone(), task);
        loop {
            let now = self.ports.clock.now().await;
            match self.take_lease_if_free(&claim, key, now).await {
                Ok(Some(hold)) => return hold,
                Ok(None) => {}
                Err(error) => warn_port_failure("lease_take_failed", &error),
            }
            let retry_at = now + self.rules.renew_every;
            self.ports.clock.sleep_until(retry_at).await;
        }
    }

    async fn take_lease_if_free(
        &self,
        claim: &Claim,
        key: &WorkerKey,
        now: SystemTime,
    ) -> anyhow::Result<Option<Hold>> {
        let leases = &self.ports.leases;
        let entry = leases.read(key).await?;
        let Some(lease) = claim.lease_if_free(&self.rules, &entry, now) else {
            return Ok(None);
        };
        let written = leases
            .write(key, entry.revision, Some(lease.clone()))
            .await?;
        let LeaseWrite::Written(fence) = written else {
            return Ok(None);
        };

        let hold = claim.clone().into_hold(&self.rules, &lease, fence, now);
        self.ports
            .log
            .record(now, ProtocolEvent::LeaseAcquired(hold.clone()));
        Ok(Some(hold))
    }

    async fn start_task(&self, key: &WorkerKey, task: TaskId) -> anyhow::Result<()> {
        let markers = &self.ports.markers;
        let now = self.ports.clock.now().await;
        let entry = markers.read(key).await?;
        let pending_task = entry.pending.map(|pending| pending.task);
        let cleared_marker = pending_task == Some(task)
            && matches!(
                markers.write(key, entry.revision, None).await?,
                MarkerWrite::Written
            );

        let started = ProtocolEvent::Started {
            task,
            cleared_marker,
        };
        self.ports.log.record(now, started);
        Ok(())
    }

    async fn extend_running_lease(&self, hold: &mut Hold, key: &WorkerKey, delivery: &Delivery) {
        let log = &self.ports.log;
        loop {
            self.ports.clock.sleep_until(hold.renew_at()).await;
            let now = self.ports.clock.now().await;
            match self.extend_lease_if_ours(hold, key, now).await {
                Ok(true) => {
                    if let Err(error) = delivery.handle.acknowledge_progress().await {
                        warn_port_failure("progress_ack_failed", &error);
                    }
                    log.record(now, ProtocolEvent::Renewed(hold.clone()));
                }
                Ok(false) => {
                    log.record(now, ProtocolEvent::RenewalLost(hold.task()));
                    return;
                }
                Err(error) => {
                    warn_port_failure("lease_renewal_failed", &error);
                    log.record(now, ProtocolEvent::RenewalFailed(hold.task()));
                    return;
                }
            }
        }
    }

    async fn extend_lease_if_ours(
        &self,
        hold: &mut Hold,
        key: &WorkerKey,
        now: SystemTime,
    ) -> anyhow::Result<bool> {
        let leases = &self.ports.leases;
        let entry = leases.read(key).await?;
        let lease = match hold.build_renewal(&self.rules, &entry, now) {
            Renewal::Extend(lease) => lease,
            Renewal::Lost => return Ok(false),
        };
        let written = leases
            .write(key, entry.revision, Some(lease.clone()))
            .await?;
        let LeaseWrite::Written(fence) = written else {
            return Ok(false);
        };
        hold.record_renewal(&self.rules, &lease, fence.revision, now);
        Ok(true)
    }

    async fn finish_task(&self, delivery: &Delivery, hold: Option<&Hold>, outcome: EncodedOutcome) {
        let message = &delivery.message;
        for child in outcome.children {
            let child =
                TaskMessage::from_input(message.tenant.clone(), child, message.lane.clone());
            if let Err(error) = submit_task(&self.ports, child).await {
                return self.abandon_run(message.task, error).await;
            }
        }

        let now = self.ports.clock.now().await;
        if let Err(error) = delivery.handle.acknowledge().await {
            warn_port_failure("ack_failed", &error);
        }
        let released = match hold {
            Some(hold) => self.release_lease(hold, &message.worker_key()).await,
            None => Ok(false),
        };
        if let Err(error) = &released {
            warn_port_failure("lease_release_failed", error);
        }
        let released = released.unwrap_or(false);
        let finished = ProtocolEvent::Finished {
            task: message.task,
            released,
        };
        self.ports.log.record(now, finished);
    }

    async fn release_lease(&self, hold: &Hold, key: &WorkerKey) -> anyhow::Result<bool> {
        let leases = &self.ports.leases;
        let entry = leases.read(key).await?;
        let Some(revision) = hold.revision_to_release(&entry) else {
            return Ok(false);
        };
        let written = leases.write(key, revision, None).await?;
        Ok(matches!(written, LeaseWrite::Written(_)))
    }

    async fn abandon_run(&self, task: TaskId, error: impl Display) {
        let now = self.ports.clock.now().await;
        orbit_log_fields::warn!(
            safe(ERROR_TYPE) = "run_abandoned",
            expanded(ERROR_MESSAGE) = %error,
            "a run stopped without an ack, so the broker delivers its task again"
        );
        self.ports.log.record(now, ProtocolEvent::Abandoned(task));
    }
}

fn warn_port_failure(failure: &'static str, error: &anyhow::Error) {
    orbit_log_fields::warn!(
        safe(ERROR_TYPE) = failure,
        expanded(ERROR_MESSAGE) = %error,
        "a port call of the lane loop failed"
    );
}
