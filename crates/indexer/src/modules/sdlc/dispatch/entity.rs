use async_trait::async_trait;
use tracing::info;

use crate::scheduler::ScheduledTaskMetrics;
use crate::scheduler::{ScheduledTask, TaskError};
use gkg_server_config::{EntityDispatcherConfig, ScheduleConfiguration};

pub struct EntityDispatcher {
    metrics: ScheduledTaskMetrics,
    config: EntityDispatcherConfig,
}

impl EntityDispatcher {
    pub fn new(metrics: ScheduledTaskMetrics, config: EntityDispatcherConfig) -> Self {
        Self { metrics, config }
    }
}

#[async_trait]
impl ScheduledTask for EntityDispatcher {
    fn name(&self) -> &str {
        "dispatch.sdlc.entity"
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        info!("entity dispatcher run (no-op, pipelines not yet wired)");
        self.metrics.record_run(self.name(), "success", 0.0);
        Ok(())
    }
}
