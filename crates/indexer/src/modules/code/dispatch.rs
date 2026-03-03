use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::clickhouse::ArrowClickHouseClient;
use crate::configuration::DispatcherConfiguration;
use crate::dispatcher::{DispatchError, Dispatcher};
use crate::modules::sdlc::dispatch::DispatchMetrics;
use crate::nats::NatsServices;
use crate::topic::ProjectCodeIndexingRequest;
use crate::types::{Envelope, Event};
use clickhouse_client::FromArrowColumn;

const PENDING_PROJECTS_QUERY: &str = r#"
SELECT project.id AS project_id
FROM gl_project AS project
LEFT ANTI JOIN (SELECT project_id FROM project_code_indexing_watermark FINAL) AS watermark
  ON project.id = watermark.project_id
WHERE project._deleted = false
LIMIT {batch_size:UInt64}
"#;

fn default_batch_size() -> u64 {
    1000
}

fn default_interval_secs() -> Option<u64> {
    Some(1800)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectCodeDispatcherConfig {
    #[serde(flatten)]
    pub dispatcher: DispatcherConfiguration,

    #[serde(default = "default_batch_size")]
    pub batch_size: u64,
}

impl Default for ProjectCodeDispatcherConfig {
    fn default() -> Self {
        Self {
            dispatcher: DispatcherConfiguration {
                interval_secs: default_interval_secs(),
            },
            batch_size: default_batch_size(),
        }
    }
}

pub struct ProjectCodeDispatcher {
    nats: Arc<dyn NatsServices>,
    graph: ArrowClickHouseClient,
    metrics: DispatchMetrics,
    config: ProjectCodeDispatcherConfig,
}

impl ProjectCodeDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        graph: ArrowClickHouseClient,
        metrics: DispatchMetrics,
        config: ProjectCodeDispatcherConfig,
    ) -> Self {
        Self {
            nats,
            graph,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl Dispatcher for ProjectCodeDispatcher {
    fn name(&self) -> &str {
        "code.project"
    }

    fn dispatcher_config(&self) -> &DispatcherConfiguration {
        &self.config.dispatcher
    }

    async fn dispatch(&self) -> Result<(), DispatchError> {
        let start = Instant::now();

        let result = self.dispatch_inner().await;

        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics.record_run(self.name(), outcome, duration);

        result
    }
}

impl ProjectCodeDispatcher {
    async fn dispatch_inner(&self) -> Result<(), DispatchError> {
        let project_ids = self.fetch_pending_project_ids().await?;

        if project_ids.is_empty() {
            info!("no projects pending code indexing");
            return Ok(());
        }

        debug!(
            count = project_ids.len(),
            "found projects pending code indexing"
        );

        let mut dispatched: u64 = 0;

        for project_id in &project_ids {
            let envelope = Envelope::new(&ProjectCodeIndexingRequest {
                project_id: *project_id,
            })
            .map_err(|error| {
                self.metrics.record_error(self.name(), "publish");
                DispatchError::new(error)
            })?;

            self.nats
                .publish(&ProjectCodeIndexingRequest::topic(), &envelope)
                .await
                .map_err(|error| {
                    self.metrics.record_error(self.name(), "publish");
                    DispatchError::new(error)
                })?;

            dispatched += 1;
            debug!(project_id, "dispatched code indexing request");
        }

        self.metrics
            .record_requests_published(self.name(), dispatched);

        info!(dispatched, "dispatched code indexing requests");
        Ok(())
    }

    async fn fetch_pending_project_ids(&self) -> Result<Vec<i64>, DispatchError> {
        let query_start = Instant::now();
        let batches = self
            .graph
            .query(PENDING_PROJECTS_QUERY)
            .param("batch_size", self.config.batch_size)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "query");
                DispatchError::new(error)
            })?;
        self.metrics
            .record_query_duration(query_start.elapsed().as_secs_f64());

        i64::extract_column(&batches, 0).map_err(DispatchError::new)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::*;
    use crate::clickhouse::ClickHouseConfiguration;
    use crate::testkit::MockNatsServices;

    fn test_metrics() -> DispatchMetrics {
        DispatchMetrics::with_meter(&crate::testkit::test_meter())
    }

    fn test_client() -> ArrowClickHouseClient {
        ClickHouseConfiguration::default().build_client()
    }

    #[test]
    fn dispatcher_name_is_code_project() {
        let nats = Arc::new(MockNatsServices::new());
        let dispatcher = ProjectCodeDispatcher::new(
            nats,
            test_client(),
            test_metrics(),
            ProjectCodeDispatcherConfig::default(),
        );

        assert_eq!(dispatcher.name(), "code.project");
    }

    #[test]
    fn defaults_to_thirty_minute_interval() {
        let config = ProjectCodeDispatcherConfig::default();

        assert_eq!(
            config.dispatcher.interval(),
            Some(Duration::from_secs(1800))
        );
    }

    #[test]
    fn defaults_to_batch_size_1000() {
        let config = ProjectCodeDispatcherConfig::default();

        assert_eq!(config.batch_size, 1000);
    }
}
