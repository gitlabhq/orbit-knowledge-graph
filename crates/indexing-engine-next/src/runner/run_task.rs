use std::fmt::Display;
use std::sync::Arc;
use std::time::SystemTime;

use super::Runner;
use crate::builder::ErasedWorker;
use crate::event::ProtocolEvent;
use crate::lease::Hold;
use crate::pending::PendingWrite;
use crate::ports::warn_port_failure;
use crate::submit::submit_task;
use crate::task::{Delivery, TaskId, TaskMessage, WorkerKey};
use crate::worker::{EncodedOutcome, TaskContext};

impl Runner {
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
        let in_flight = delivery.in_flight.as_ref();

        let acquired = self
            .leases
            .acquire_or_give_back(&key, task, in_flight)
            .await;
        let mut hold = match acquired {
            Ok(Some(hold)) => hold,
            Ok(None) => return,
            Err(error) => return self.abandon_run(task, error).await,
        };
        let started_at = match self.start_task(&key, task).await {
            Ok(started_at) => started_at,
            Err(error) => return self.abandon_run(task, error).await,
        };

        let ctx = TaskContext::new(Arc::clone(&self.pools), Some(hold.fence()));
        let outcome = tokio::select! {
            biased;
            () = self.leases.renew_while_running(&mut hold, &key, in_flight, started_at) => return,
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

    async fn start_task(&self, key: &WorkerKey, task: TaskId) -> anyhow::Result<SystemTime> {
        let now = self.ports.clock.now().await;
        let pending = &self.ports.pending;

        let entry = pending.read(key).await?;
        let cleared_pending = if entry.task.is_some_and(|message| message.task == task) {
            let written = pending.write(key, entry.revision, None).await?;
            matches!(written, PendingWrite::Written)
        } else {
            false
        };

        let started = ProtocolEvent::Started {
            task,
            cleared_pending,
        };
        self.ports.log.record(now, started);
        Ok(now)
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
        if let Err(error) = delivery.in_flight.acknowledge().await {
            warn_port_failure("ack_failed", &error);
        }

        let released = match hold {
            Some(hold) => self.leases.release(hold, &message.worker_key()).await,
            None => Ok(false),
        };
        if let Err(error) = &released {
            warn_port_failure("lease_release_failed", error);
        }
        let finished = ProtocolEvent::Finished {
            task: message.task,
            released: released.unwrap_or(false),
        };
        self.ports.log.record(now, finished);
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
