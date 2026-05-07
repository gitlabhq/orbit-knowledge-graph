use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use tracing::{debug, info};

use crate::nats::NatsServices;
use crate::scheduler::ScheduledTaskMetrics;
use crate::scheduler::{ScheduledTask, TaskError};
use crate::topic::EntityIndexingRequest;
use crate::types::Envelope;
use gkg_server_config::{GlobalDispatcherConfig, ScheduleConfiguration};

pub struct GlobalDispatcher {
    entity_names: Vec<String>,
    nats: Arc<dyn NatsServices>,
    metrics: ScheduledTaskMetrics,
    config: GlobalDispatcherConfig,
}

impl GlobalDispatcher {
    pub fn new(
        entity_names: Vec<String>,
        nats: Arc<dyn NatsServices>,
        metrics: ScheduledTaskMetrics,
        config: GlobalDispatcherConfig,
    ) -> Self {
        Self {
            entity_names,
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
        let watermark = Utc::now();
        let mut dispatched: u64 = 0;
        let mut skipped: u64 = 0;

        for entity in &self.entity_names {
            let request = EntityIndexingRequest {
                entity: entity.clone(),
                namespace: None,
                traversal_path: None,
                range: None,
                watermark,
            };

            let subscription = request.publish_subscription();
            let envelope = Envelope::new(&request).map_err(|error| {
                self.metrics.record_error(self.name(), "publish");
                TaskError::new(error)
            })?;

            match self.nats.publish(&subscription, &envelope).await {
                Ok(()) => {
                    dispatched += 1;
                    debug!(entity = %entity, "dispatched global entity indexing request");
                }
                Err(crate::nats::NatsError::PublishDuplicate) => {
                    skipped += 1;
                    debug!(entity = %entity, "skipped global entity indexing request, already in-flight");
                }
                Err(error) => {
                    self.metrics.record_error(self.name(), "publish");
                    return Err(TaskError::new(error));
                }
            }
        }

        self.metrics
            .record_requests_published(self.name(), dispatched);
        self.metrics.record_requests_skipped(self.name(), skipped);

        info!(
            dispatched,
            skipped, "dispatched global entity indexing requests"
        );
        Ok(())
    }
}
