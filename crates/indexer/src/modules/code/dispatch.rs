use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use tracing::{debug, info};

use crate::clickhouse::ArrowClickHouseClient;
use crate::dispatcher::{DispatchError, Dispatcher};
use crate::modules::sdlc::dispatch::DispatchMetrics;
use crate::nats::NatsServices;
use crate::topic::ProjectCodeIndexingRequest;
use crate::types::{Envelope, Event};
use clickhouse_client::FromArrowColumn;

// Capped at 1000 per cycle so we don't flood NATS after a large backfill.
// The backlog drains over successive hourly runs.
const PENDING_PROJECTS_QUERY: &str = r#"
SELECT project.id AS project_id
FROM gl_project AS project
LEFT ANTI JOIN (SELECT project_id FROM project_code_indexing_watermark FINAL) AS watermark
  ON project.id = watermark.project_id
WHERE project._deleted = false
LIMIT 1000
"#;

pub struct ProjectCodeDispatcher {
    nats: Arc<dyn NatsServices>,
    graph: ArrowClickHouseClient,
    metrics: DispatchMetrics,
}

impl ProjectCodeDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        graph: ArrowClickHouseClient,
        metrics: DispatchMetrics,
    ) -> Self {
        Self {
            nats,
            graph,
            metrics,
        }
    }
}

#[async_trait]
impl Dispatcher for ProjectCodeDispatcher {
    fn name(&self) -> &str {
        "code.project"
    }

    fn interval(&self) -> Option<Duration> {
        Some(Duration::from_secs(3600))
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
    use super::*;
    use crate::clickhouse::ClickHouseConfiguration;
    use crate::testkit::MockNatsServices;
    use std::sync::Arc;

    fn test_metrics() -> DispatchMetrics {
        let provider = opentelemetry::global::meter_provider();
        let meter = provider.meter("test");
        DispatchMetrics::with_meter(&meter)
    }

    fn test_client() -> ArrowClickHouseClient {
        ClickHouseConfiguration::default().build_client()
    }

    #[test]
    fn dispatcher_name_is_code_project() {
        let nats = Arc::new(MockNatsServices::new());
        let dispatcher = ProjectCodeDispatcher::new(nats, test_client(), test_metrics());

        assert_eq!(dispatcher.name(), "code.project");
    }

    #[test]
    fn defaults_to_one_hour_interval() {
        let nats = Arc::new(MockNatsServices::new());
        let dispatcher = ProjectCodeDispatcher::new(nats, test_client(), test_metrics());

        assert_eq!(dispatcher.interval(), Some(Duration::from_secs(3600)));
    }
}
