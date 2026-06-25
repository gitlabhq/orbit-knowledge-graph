mod change_detection;
mod publisher;
mod sweep;

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::{debug, info};

use change_detection::{DatalakeChangeDetector, NamespaceChangeDetector};
use publisher::{NamespacePublisher, NamespaceRequestPublisher};

use crate::campaign::CampaignState;
use crate::checkpoint::CheckpointStore;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::durability::WriteDurability;
use crate::nats::NatsServices;
use crate::orchestrator::scheduled::ScheduledTaskMetrics;
use crate::orchestrator::scheduled::{ScheduledTask, TaskError};
use gkg_server_config::{NamespaceDispatcherConfig, ScheduleConfiguration};

pub use sweep::NamespaceSweepDispatcher;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DispatchNamespace {
    pub namespace_id: i64,
    pub traversal_path: String,
}

const CHECKPOINT_KEY: &str = "dispatch.sdlc.namespace.changes";

pub struct NamespaceDispatcher {
    detector: Arc<dyn NamespaceChangeDetector>,
    publisher: Arc<dyn NamespacePublisher>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    metrics: ScheduledTaskMetrics,
    config: NamespaceDispatcherConfig,
}

impl NamespaceDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        datalake: ArrowClickHouseClient,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: ScheduledTaskMetrics,
        config: NamespaceDispatcherConfig,
        campaign: Arc<CampaignState>,
        ontology: &ontology::Ontology,
    ) -> Self {
        Self {
            detector: Arc::new(DatalakeChangeDetector::new(datalake, ontology)),
            publisher: Arc::new(NamespaceRequestPublisher::new(nats, campaign)),
            checkpoint_store,
            metrics,
            config,
        }
    }

    #[cfg(test)]
    fn with_detector_and_publisher(
        detector: Arc<dyn NamespaceChangeDetector>,
        publisher: Arc<dyn NamespacePublisher>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: ScheduledTaskMetrics,
        config: NamespaceDispatcherConfig,
    ) -> Self {
        Self {
            detector,
            publisher,
            checkpoint_store,
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
        let upper = Utc::now();
        let lower = self.load_lower_checkpoint().await?;

        let query_start = Instant::now();
        let namespaces = self
            .detector
            .changed_namespaces(lower, upper)
            .await
            .inspect_err(|_| {
                self.metrics.record_error(self.name(), "query");
            })?;
        self.metrics
            .record_query_duration("namespace_changes", query_start.elapsed().as_secs_f64());

        debug!(
            changed_namespaces = namespaces.len(),
            lower = %lower.format(TIMESTAMP_FORMAT),
            upper = %upper.format(TIMESTAMP_FORMAT),
            "found changed enabled root namespaces"
        );

        let report = self
            .publisher
            .publish(&namespaces, upper)
            .await
            .inspect_err(|_| {
                self.metrics.record_error(self.name(), "publish");
            })?;

        self.save_checkpoint(&upper).await?;

        self.metrics
            .record_requests_published(self.name(), report.dispatched);
        self.metrics
            .record_requests_skipped(self.name(), report.skipped);

        info!(
            dispatched = report.dispatched,
            skipped = report.skipped,
            changed_namespaces = namespaces.len(),
            "dispatched changed namespace indexing requests"
        );

        Ok(())
    }

    async fn load_lower_checkpoint(&self) -> Result<DateTime<Utc>, TaskError> {
        Ok(self
            .checkpoint_store
            .load(CHECKPOINT_KEY)
            .await
            .map_err(TaskError::new)?
            .map(|checkpoint| checkpoint.watermark)
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH))
    }

    async fn save_checkpoint(&self, upper: &DateTime<Utc>) -> Result<(), TaskError> {
        self.checkpoint_store
            .save_completed(CHECKPOINT_KEY, upper, WriteDurability::Durable)
            .await
            .map_err(TaskError::new)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::{Checkpoint, CheckpointError};
    use crate::orchestrator::scheduled::namespace::publisher::PublishReport;
    use std::sync::Mutex;

    #[derive(Default)]
    struct StubCheckpointStore {
        loaded: Mutex<Option<Checkpoint>>,
        saved: Mutex<Vec<DateTime<Utc>>>,
    }

    impl StubCheckpointStore {
        fn saved(&self) -> Vec<DateTime<Utc>> {
            self.saved.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl CheckpointStore for StubCheckpointStore {
        async fn load(&self, _key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
            Ok(self.loaded.lock().unwrap().clone())
        }

        async fn save_progress(
            &self,
            _key: &str,
            _checkpoint: &Checkpoint,
        ) -> Result<(), CheckpointError> {
            Ok(())
        }

        async fn save_completed(
            &self,
            _key: &str,
            watermark: &DateTime<Utc>,
            _durability: WriteDurability,
        ) -> Result<(), CheckpointError> {
            self.saved.lock().unwrap().push(*watermark);
            Ok(())
        }

        async fn load_by_prefix(
            &self,
            _prefix: &str,
        ) -> Result<Vec<(String, Checkpoint)>, CheckpointError> {
            Ok(Vec::new())
        }

        async fn consolidate(
            &self,
            _parent_key: &str,
            _watermark: &DateTime<Utc>,
        ) -> Result<(), CheckpointError> {
            Ok(())
        }
    }

    struct StubDetector {
        result: Result<Vec<DispatchNamespace>, &'static str>,
    }

    #[async_trait]
    impl NamespaceChangeDetector for StubDetector {
        async fn changed_namespaces(
            &self,
            _lower: DateTime<Utc>,
            _upper: DateTime<Utc>,
        ) -> Result<Vec<DispatchNamespace>, TaskError> {
            self.result.clone().map_err(TaskError::new)
        }
    }

    struct StubPublisher {
        result: Result<PublishReport, &'static str>,
    }

    #[async_trait]
    impl NamespacePublisher for StubPublisher {
        async fn publish(
            &self,
            _namespaces: &[DispatchNamespace],
            _watermark: DateTime<Utc>,
        ) -> Result<PublishReport, TaskError> {
            self.result.map_err(TaskError::new)
        }
    }

    fn dispatcher_with(
        detector: StubDetector,
        publisher: StubPublisher,
        checkpoint_store: Arc<StubCheckpointStore>,
    ) -> NamespaceDispatcher {
        NamespaceDispatcher::with_detector_and_publisher(
            Arc::new(detector),
            Arc::new(publisher),
            checkpoint_store,
            ScheduledTaskMetrics::new(),
            NamespaceDispatcherConfig::default(),
        )
    }

    #[tokio::test]
    async fn checkpoint_advances_after_successful_publish() {
        let checkpoint_store = Arc::new(StubCheckpointStore::default());
        let dispatcher = dispatcher_with(
            StubDetector {
                result: Ok(vec![DispatchNamespace {
                    namespace_id: 9,
                    traversal_path: "1/9/".to_string(),
                }]),
            },
            StubPublisher {
                result: Ok(PublishReport {
                    dispatched: 1,
                    skipped: 0,
                }),
            },
            checkpoint_store.clone(),
        );

        dispatcher.dispatch_inner().await.unwrap();

        assert_eq!(checkpoint_store.saved().len(), 1);
    }

    #[tokio::test]
    async fn checkpoint_does_not_advance_when_query_fails() {
        let checkpoint_store = Arc::new(StubCheckpointStore::default());
        let dispatcher = dispatcher_with(
            StubDetector {
                result: Err("query failed"),
            },
            StubPublisher {
                result: Ok(PublishReport::default()),
            },
            checkpoint_store.clone(),
        );

        let err = dispatcher.dispatch_inner().await.unwrap_err();

        assert!(err.to_string().contains("query failed"));
        assert!(checkpoint_store.saved().is_empty());
    }

    #[tokio::test]
    async fn checkpoint_does_not_advance_when_publish_fails() {
        let checkpoint_store = Arc::new(StubCheckpointStore::default());
        let dispatcher = dispatcher_with(
            StubDetector {
                result: Ok(vec![DispatchNamespace {
                    namespace_id: 9,
                    traversal_path: "1/9/".to_string(),
                }]),
            },
            StubPublisher {
                result: Err("publish failed"),
            },
            checkpoint_store.clone(),
        );

        let err = dispatcher.dispatch_inner().await.unwrap_err();

        assert!(err.to_string().contains("publish failed"));
        assert!(checkpoint_store.saved().is_empty());
    }
}
