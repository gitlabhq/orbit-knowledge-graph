use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use tracing::info;

use crate::nats::NatsServices;
use crate::scheduler::ScheduledTaskMetrics;
use crate::scheduler::{ScheduledTask, TaskError};
use crate::topic::GlobalIndexingRequest;
use crate::types::{Envelope, Event};
use gkg_server_config::{GlobalDispatcherConfig, ScheduleConfiguration};

pub struct GlobalDispatcher {
    nats: Arc<dyn NatsServices>,
    metrics: ScheduledTaskMetrics,
    config: GlobalDispatcherConfig,
}

impl GlobalDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        metrics: ScheduledTaskMetrics,
        config: GlobalDispatcherConfig,
    ) -> Self {
        Self {
            nats,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl ScheduledTask for GlobalDispatcher {
    fn name(&self) -> &str {
        "dispatch.sdlc.global"
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let start = Instant::now();

        let result = self.dispatch_inner().await;

        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics.record_run(self.name(), outcome, duration);

        result
    }
}

impl GlobalDispatcher {
    async fn dispatch_inner(&self) -> Result<(), TaskError> {
        let envelope = Envelope::new(&GlobalIndexingRequest {
            watermark: Utc::now(),
        })
        .map_err(|error| {
            self.metrics.record_error(self.name(), "publish");
            TaskError::new(error)
        })?;

        match self
            .nats
            .publish(&GlobalIndexingRequest::subscription(), &envelope)
            .await
        {
            Ok(()) => {
                self.metrics.record_requests_published(self.name(), 1);
                info!("dispatched global indexing request");
            }
            Err(crate::nats::NatsError::PublishDuplicate) => {
                self.metrics.record_requests_skipped(self.name(), 1);
                info!("skipping global indexing request, already in-flight");
            }
            Err(error) => {
                self.metrics.record_error(self.name(), "publish");
                return Err(TaskError::new(error));
            }
        }

        Ok(())
    }
}
