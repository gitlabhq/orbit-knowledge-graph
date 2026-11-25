use std::fmt::Display;
use std::sync::Arc;
use std::time::SystemTime;

use super::Runner;
use crate::builder::ErasedWorker;
use crate::event::{Failure, TaskStep};
use crate::lease::{Acquisition, Hold};
use crate::submit::submit_task;
use crate::task::{Delivery, TaskMessage};
use crate::worker::{EncodedOutcome, TaskContext};

impl Runner {
    pub(super) async fn run_delivered_task(self: Arc<Self>, delivery: Delivery) {
        let worker_name = &delivery.message.worker;
        let Some(worker) = self.registry.find_worker(worker_name).cloned() else {
            let error = "no module registered the worker of a delivered task";
            return self
                .abandon_run(&delivery.message, "unknown_worker", error)
                .await;
        };
        if self.registry.is_exclusive(worker_name) {
            self.run_with_lease(worker.as_ref(), delivery).await;
        } else {
            self.run_without_lease(worker.as_ref(), delivery).await;
        }
    }

    async fn run_with_lease(&self, worker: &dyn ErasedWorker, delivery: Delivery) {
        let message = &delivery.message;
        let in_flight = delivery.in_flight.as_ref();

        let acquired = self.leases.acquire_or_give_back(message, in_flight).await;
        let mut hold = match acquired {
            Ok(Acquisition::Held(hold)) => hold,
            Ok(Acquisition::GivenBack) => return,
            Err(error) => return self.abandon_run(message, "lease_failed", error).await,
        };
        let started_at = match self.start_task(message).await {
            Ok(started_at) => started_at,
            Err(error) => return self.abandon_run(message, "start_failed", error).await,
        };

        let ctx = TaskContext::new(Arc::clone(&self.pools));
        let outcome = tokio::select! {
            biased;
            () = self.leases.renew_while_running(&mut hold, message, in_flight, started_at) => return,
            outcome = worker.run(&ctx, &message.input) => outcome,
        };

        match outcome {
            Ok(outcome) => self.finish_task(&delivery, Some(&hold), outcome).await,
            Err(error) => self.abandon_run(message, "worker_failed", error).await,
        }
    }

    async fn run_without_lease(&self, worker: &dyn ErasedWorker, delivery: Delivery) {
        let message = &delivery.message;
        if let Err(error) = self.start_task(message).await {
            return self.abandon_run(message, "start_failed", error).await;
        }

        let ctx = TaskContext::new(Arc::clone(&self.pools));
        match worker.run(&ctx, &message.input).await {
            Ok(outcome) => self.finish_task(&delivery, None, outcome).await,
            Err(error) => self.abandon_run(message, "worker_failed", error).await,
        }
    }

    async fn start_task(&self, message: &TaskMessage) -> anyhow::Result<SystemTime> {
        let now = self.ports.clock.now().await;
        let cleared_pending = self.ports.pending.clear_if_still_pending(message).await?;
        let started = TaskStep::Started { cleared_pending };
        self.observer.observe(&message.build_event(now, started));
        Ok(now)
    }

    async fn finish_task(&self, delivery: &Delivery, hold: Option<&Hold>, outcome: EncodedOutcome) {
        let message = &delivery.message;
        for child in outcome.children {
            let child =
                TaskMessage::from_input(message.tenant.clone(), child, message.lane.clone());
            if let Err(error) = submit_task(&self.ports, self.observer.as_ref(), child).await {
                return self
                    .abandon_run(message, "child_submit_failed", error)
                    .await;
            }
        }

        let now = self.ports.clock.now().await;
        if let Err(error) = delivery.in_flight.ack().await {
            self.observe_port_call_failure(message, now, Failure::new("ack_failed", error));
        }

        let released_lease = match hold {
            Some(hold) => self.leases.release(hold, &message.worker_key()).await,
            None => Ok(false),
        };
        let released_lease = released_lease.unwrap_or_else(|error| {
            let failure = Failure::new("lease_release_failed", error);
            self.observe_port_call_failure(message, now, failure);
            false
        });
        let finished = TaskStep::Finished { released_lease };
        self.observer.observe(&message.build_event(now, finished));
    }

    async fn abandon_run(&self, message: &TaskMessage, kind: &'static str, error: impl Display) {
        let now = self.ports.clock.now().await;
        let abandoned = TaskStep::Abandoned(Failure::new(kind, error));
        self.observer.observe(&message.build_event(now, abandoned));
    }

    fn observe_port_call_failure(&self, message: &TaskMessage, at: SystemTime, failure: Failure) {
        let failed = TaskStep::PortCallFailed(failure);
        self.observer.observe(&message.build_event(at, failed));
    }
}
