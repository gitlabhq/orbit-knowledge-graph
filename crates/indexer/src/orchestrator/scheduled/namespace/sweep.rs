use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use clickhouse_client::FromArrowColumn;
use tracing::info;

use super::DispatchNamespace;
use super::publisher::{NamespacePublisher, NamespaceRequestPublisher};
use crate::campaign::CampaignState;
use crate::clickhouse::ArrowClickHouseClient;
use crate::nats::NatsServices;
use crate::orchestrator::scheduled::ScheduledTaskMetrics;
use crate::orchestrator::scheduled::{ScheduledTask, TaskError};
use gkg_server_config::{NamespaceSweepConfig, ScheduleConfiguration};

const ENABLED_NAMESPACE_TABLE: &str = "siphon_knowledge_graph_enabled_namespaces";

#[async_trait]
pub(super) trait EnabledNamespaceReader: Send + Sync {
    async fn enabled_namespaces(&self) -> Result<Vec<DispatchNamespace>, TaskError>;
}

pub(super) struct DatalakeEnabledNamespaceReader {
    datalake: ArrowClickHouseClient,
    sql: String,
}

impl DatalakeEnabledNamespaceReader {
    fn new(datalake: ArrowClickHouseClient) -> Self {
        let deleted_column = ontology::siphon_deleted_column();
        Self {
            datalake,
            sql: format!(
                "SELECT root_namespace_id, traversal_path \
                 FROM {ENABLED_NAMESPACE_TABLE} \
                 WHERE {deleted_column} = false AND traversal_path != ''"
            ),
        }
    }
}

#[async_trait]
impl EnabledNamespaceReader for DatalakeEnabledNamespaceReader {
    async fn enabled_namespaces(&self) -> Result<Vec<DispatchNamespace>, TaskError> {
        let batches = self
            .datalake
            .query(&self.sql)
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;

        let namespace_ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        let traversal_paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;

        Ok(namespace_ids
            .into_iter()
            .zip(traversal_paths)
            .map(|(namespace_id, traversal_path)| DispatchNamespace {
                namespace_id,
                traversal_path,
            })
            .collect())
    }
}

/// Backstops [`super::NamespaceDispatcher`] for the cases its watermark window
/// cannot catch: schema-migration backfill into new-prefix tables (the change
/// checkpoint is global, not per-prefix), clock skew, late arrivals, and missed
/// ticks. NATS publish-dedup keeps it from duplicating in-flight requests.
pub struct NamespaceSweepDispatcher {
    reader: Arc<dyn EnabledNamespaceReader>,
    publisher: Arc<dyn NamespacePublisher>,
    metrics: ScheduledTaskMetrics,
    config: NamespaceSweepConfig,
}

impl NamespaceSweepDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        datalake: ArrowClickHouseClient,
        metrics: ScheduledTaskMetrics,
        config: NamespaceSweepConfig,
        campaign: Arc<CampaignState>,
    ) -> Self {
        Self {
            reader: Arc::new(DatalakeEnabledNamespaceReader::new(datalake)),
            publisher: Arc::new(NamespaceRequestPublisher::new(nats, campaign)),
            metrics,
            config,
        }
    }

    #[cfg(test)]
    fn with_reader_and_publisher(
        reader: Arc<dyn EnabledNamespaceReader>,
        publisher: Arc<dyn NamespacePublisher>,
        metrics: ScheduledTaskMetrics,
        config: NamespaceSweepConfig,
    ) -> Self {
        Self {
            reader,
            publisher,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl ScheduledTask for NamespaceSweepDispatcher {
    fn name(&self) -> &str {
        "dispatch.sdlc.namespace.sweep"
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let start = Instant::now();
        let result = self.sweep_inner().await;
        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics.record_run(self.name(), outcome, duration);
        result
    }
}

impl NamespaceSweepDispatcher {
    async fn sweep_inner(&self) -> Result<(), TaskError> {
        let query_start = Instant::now();
        let namespaces = self.reader.enabled_namespaces().await.inspect_err(|_| {
            self.metrics.record_error(self.name(), "query");
        })?;
        self.metrics
            .record_query_duration("enabled_namespaces", query_start.elapsed().as_secs_f64());

        let report = self
            .publisher
            .publish(&namespaces, Utc::now())
            .await
            .inspect_err(|_| {
                self.metrics.record_error(self.name(), "publish");
            })?;

        self.metrics
            .record_requests_published(self.name(), report.dispatched);
        self.metrics
            .record_requests_skipped(self.name(), report.skipped);

        info!(
            dispatched = report.dispatched,
            skipped = report.skipped,
            enabled_namespaces = namespaces.len(),
            "swept enabled namespace indexing requests"
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orchestrator::scheduled::namespace::publisher::PublishReport;
    use chrono::{DateTime, Utc};
    use std::sync::Mutex;

    struct StubReader {
        result: Result<Vec<DispatchNamespace>, &'static str>,
    }

    #[async_trait]
    impl EnabledNamespaceReader for StubReader {
        async fn enabled_namespaces(&self) -> Result<Vec<DispatchNamespace>, TaskError> {
            self.result.clone().map_err(TaskError::new)
        }
    }

    #[derive(Default)]
    struct RecordingPublisher {
        published: Mutex<Vec<usize>>,
    }

    #[async_trait]
    impl NamespacePublisher for RecordingPublisher {
        async fn publish(
            &self,
            namespaces: &[DispatchNamespace],
            _watermark: DateTime<Utc>,
        ) -> Result<PublishReport, TaskError> {
            self.published.lock().unwrap().push(namespaces.len());
            Ok(PublishReport {
                dispatched: namespaces.len() as u64,
                skipped: 0,
            })
        }
    }

    #[tokio::test]
    async fn sweep_publishes_every_enabled_namespace() {
        let publisher = Arc::new(RecordingPublisher::default());
        let sweep = NamespaceSweepDispatcher::with_reader_and_publisher(
            Arc::new(StubReader {
                result: Ok(vec![
                    DispatchNamespace {
                        namespace_id: 9,
                        traversal_path: "1/9/".to_string(),
                    },
                    DispatchNamespace {
                        namespace_id: 10,
                        traversal_path: "1/10/".to_string(),
                    },
                ]),
            }),
            publisher.clone(),
            ScheduledTaskMetrics::new(),
            NamespaceSweepConfig::default(),
        );

        sweep.sweep_inner().await.unwrap();

        assert_eq!(*publisher.published.lock().unwrap(), vec![2]);
    }

    #[tokio::test]
    async fn sweep_surfaces_query_errors() {
        let sweep = NamespaceSweepDispatcher::with_reader_and_publisher(
            Arc::new(StubReader {
                result: Err("query failed"),
            }),
            Arc::new(RecordingPublisher::default()),
            ScheduledTaskMetrics::new(),
            NamespaceSweepConfig::default(),
        );

        let err = sweep.sweep_inner().await.unwrap_err();

        assert!(err.to_string().contains("query failed"));
    }
}
