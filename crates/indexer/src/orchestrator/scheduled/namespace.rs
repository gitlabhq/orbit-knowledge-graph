use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use tracing::{debug, info};

use crate::campaign::CampaignState;
use crate::clickhouse::ArrowClickHouseClient;
use crate::nats::NatsServices;
use crate::orchestrator::dispatch::NamespaceIndexingDispatch;
use crate::orchestrator::scheduled::ScheduledTaskMetrics;
use crate::orchestrator::scheduled::{ScheduledTask, TaskError};
use clickhouse_client::FromArrowColumn;
use std::sync::LazyLock;

use gkg_server_config::{NamespaceDispatcherConfig, ScheduleConfiguration};

static ENABLED_NAMESPACE_QUERY: LazyLock<String> = LazyLock::new(|| {
    let del = ontology::siphon_deleted_column();
    format!(
        "SELECT root_namespace_id, traversal_path \
         FROM siphon_knowledge_graph_enabled_namespaces \
         WHERE {del} = false AND traversal_path != ''"
    )
});

pub struct NamespaceDispatcher {
    datalake: ArrowClickHouseClient,
    publisher: NamespaceIndexingDispatch,
    metrics: ScheduledTaskMetrics,
    config: NamespaceDispatcherConfig,
    campaign: Arc<CampaignState>,
}

impl NamespaceDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        datalake: ArrowClickHouseClient,
        metrics: ScheduledTaskMetrics,
        config: NamespaceDispatcherConfig,
        campaign: Arc<CampaignState>,
    ) -> Self {
        Self {
            datalake,
            publisher: NamespaceIndexingDispatch::new(nats),
            metrics,
            config,
            campaign,
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
            .query(&ENABLED_NAMESPACE_QUERY)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "query");
                TaskError::new(error)
            })?;
        self.metrics
            .record_query_duration("enabled_namespaces", query_start.elapsed().as_secs_f64());

        let namespace_ids = i64::extract_column(&arrow_batches, 0).map_err(TaskError::new)?;
        let traversal_paths = String::extract_column(&arrow_batches, 1).map_err(TaskError::new)?;

        debug!(
            enabled_namespaces = namespace_ids.len(),
            "found enabled namespaces to dispatch indexing requests for"
        );

        let watermark = Utc::now();
        let namespaces: Vec<_> = namespace_ids.into_iter().zip(traversal_paths).collect();
        let outcome = self
            .publisher
            .dispatch_for_namespaces(&namespaces, watermark, self.campaign.current())
            .await
            .inspect_err(|_error| {
                self.metrics.record_error(self.name(), "publish");
            })?;

        self.metrics
            .record_requests_published(self.name(), outcome.dispatched);
        self.metrics
            .record_requests_skipped(self.name(), outcome.skipped);

        info!(
            dispatched = outcome.dispatched,
            skipped = outcome.skipped,
            "dispatched namespace indexing requests"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::ENABLED_NAMESPACE_QUERY;

    #[test]
    fn enabled_namespace_query_excludes_empty_traversal_paths() {
        assert!(ENABLED_NAMESPACE_QUERY.contains("traversal_path != ''"));
    }
}
