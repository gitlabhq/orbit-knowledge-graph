use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use crate::clickhouse::ArrowClickHouseClient;
use crate::configuration::ScheduleConfiguration;
use crate::nats::NatsServices;
use crate::scheduler::ScheduledTaskMetrics;
use crate::scheduler::{ScheduledTask, TaskError};
use crate::topic::NamespaceIndexingRequest;
use crate::types::Envelope;
use clickhouse_client::FromArrowColumn;

const ENABLED_NAMESPACE_QUERY: &str = r#"
SELECT root_namespace_id, organization_id
FROM siphon_knowledge_graph_enabled_namespaces
INNER JOIN siphon_namespaces on siphon_knowledge_graph_enabled_namespaces.root_namespace_id = siphon_namespaces.id
WHERE _siphon_deleted = false
"#;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NamespaceDispatcherConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
}

pub struct NamespaceDispatcher {
    nats: Arc<dyn NatsServices>,
    datalake: ArrowClickHouseClient,
    metrics: ScheduledTaskMetrics,
    config: NamespaceDispatcherConfig,
}

impl NamespaceDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        datalake: ArrowClickHouseClient,
        metrics: ScheduledTaskMetrics,
        config: NamespaceDispatcherConfig,
    ) -> Self {
        Self {
            nats,
            datalake,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl ScheduledTask for NamespaceDispatcher {
    fn name(&self) -> &str {
        "dispatch.sdlc.namespace"
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

impl NamespaceDispatcher {
    async fn dispatch_inner(&self) -> Result<(), TaskError> {
        let query_start = Instant::now();
        let arrow_batches = self
            .datalake
            .query(ENABLED_NAMESPACE_QUERY)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "query");
                TaskError::new(error)
            })?;
        self.metrics
            .record_query_duration("enabled_namespaces", query_start.elapsed().as_secs_f64());

        let namespace_ids = i64::extract_column(&arrow_batches, 0).map_err(TaskError::new)?;
        let organization_ids = i64::extract_column(&arrow_batches, 1).map_err(TaskError::new)?;

        debug!(
            enabled_namespaces = namespace_ids.len(),
            "found enabled namespaces to dispatch indexing requests for"
        );

        let watermark = Utc::now();
        let mut dispatched: u64 = 0;
        let mut skipped: u64 = 0;

        for (namespace_id, organization_id) in namespace_ids.iter().zip(organization_ids.iter()) {
            let request = NamespaceIndexingRequest {
                organization: *organization_id,
                namespace: *namespace_id,
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
                    debug!(
                        namespace_id = *namespace_id,
                        organization_id = *organization_id,
                        "dispatched namespace indexing request"
                    );
                }
                Err(crate::nats::NatsError::PublishDuplicate) => {
                    skipped += 1;
                    debug!(
                        namespace_id = *namespace_id,
                        organization_id = *organization_id,
                        "skipped namespace indexing request, already in-flight"
                    );
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
            skipped, "dispatched namespace indexing requests"
        );
        Ok(())
    }
}
