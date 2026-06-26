mod change_detection;
mod sweep;

use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use tracing::{debug, info};

use change_detection::{DatalakeChangeDetector, NamespaceChangeDetector};

use crate::campaign::CampaignState;
use crate::checkpoint::CheckpointStore;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::durability::WriteDurability;
use crate::nats::NatsServices;
use crate::orchestrator::dispatch::NamespaceIndexingDispatch;
use crate::orchestrator::scheduled::ScheduledTaskMetrics;
use crate::orchestrator::scheduled::{ScheduledTask, TaskError};
use gkg_server_config::{NamespaceDispatcherConfig, ScheduleConfiguration};

pub use sweep::NamespaceSweepDispatcher;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct DispatchNamespace {
    pub namespace_id: i64,
    pub traversal_path: String,
}

impl DispatchNamespace {
    fn into_pair(self) -> (i64, String) {
        (self.namespace_id, self.traversal_path)
    }
}

const CHECKPOINT_KEY: &str = "dispatch.sdlc.namespace.changes";

pub struct NamespaceDispatcher {
    detector: Arc<dyn NamespaceChangeDetector>,
    publisher: NamespaceIndexingDispatch,
    campaign: Arc<CampaignState>,
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
            publisher: NamespaceIndexingDispatch::new(nats),
            campaign,
            checkpoint_store,
            metrics,
            config,
        }
    }

    #[cfg(test)]
    fn with_detector(
        detector: Arc<dyn NamespaceChangeDetector>,
        nats: Arc<dyn NatsServices>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: ScheduledTaskMetrics,
        config: NamespaceDispatcherConfig,
    ) -> Self {
        Self {
            detector,
            publisher: NamespaceIndexingDispatch::new(nats),
            campaign: Arc::new(CampaignState::new()),
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
        let lower = self.lower_bound(upper).await?;

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

        let changed_count = namespaces.len();
        let pairs: Vec<(i64, String)> = namespaces
            .into_iter()
            .map(DispatchNamespace::into_pair)
            .collect();
        let outcome = self
            .publisher
            .dispatch_for_namespaces(&pairs, upper, self.campaign.current())
            .await
            .inspect_err(|_| {
                self.metrics.record_error(self.name(), "publish");
            })?;

        self.save_checkpoint(&upper).await?;

        self.metrics
            .record_requests_published(self.name(), outcome.dispatched);
        self.metrics
            .record_requests_skipped(self.name(), outcome.skipped);

        info!(
            dispatched = outcome.dispatched,
            skipped = outcome.skipped,
            changed_namespaces = changed_count,
            "dispatched changed namespace indexing requests"
        );

        Ok(())
    }

    async fn lower_bound(&self, upper: DateTime<Utc>) -> Result<DateTime<Utc>, TaskError> {
        let floor = upper - Duration::seconds(self.config.max_lookback_secs as i64);
        let checkpoint = self
            .checkpoint_store
            .load(CHECKPOINT_KEY)
            .await
            .map_err(TaskError::new)?
            .map(|checkpoint| checkpoint.watermark);
        Ok(checkpoint.map_or(floor, |watermark| watermark.max(floor)))
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
    use crate::testkit::mocks::MockNatsServices;
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

    struct CapturingDetector {
        captured_lower: Mutex<Option<DateTime<Utc>>>,
    }

    impl CapturingDetector {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                captured_lower: Mutex::new(None),
            })
        }

        fn lower(&self) -> DateTime<Utc> {
            self.captured_lower
                .lock()
                .unwrap()
                .expect("detector was called")
        }
    }

    #[async_trait]
    impl NamespaceChangeDetector for CapturingDetector {
        async fn changed_namespaces(
            &self,
            lower: DateTime<Utc>,
            _upper: DateTime<Utc>,
        ) -> Result<Vec<DispatchNamespace>, TaskError> {
            *self.captured_lower.lock().unwrap() = Some(lower);
            Ok(Vec::new())
        }
    }

    fn checkpoint_at(watermark: DateTime<Utc>) -> Checkpoint {
        Checkpoint {
            watermark,
            cursor_values: None,
            resume_floor: None,
        }
    }

    fn dispatcher_with_detector(
        detector: Arc<CapturingDetector>,
        checkpoint_store: Arc<StubCheckpointStore>,
    ) -> NamespaceDispatcher {
        NamespaceDispatcher::with_detector(
            detector,
            Arc::new(MockNatsServices::new()),
            checkpoint_store,
            ScheduledTaskMetrics::new(),
            NamespaceDispatcherConfig::default(),
        )
    }

    fn dispatcher_with(
        detector: StubDetector,
        nats: Arc<MockNatsServices>,
        checkpoint_store: Arc<StubCheckpointStore>,
    ) -> NamespaceDispatcher {
        NamespaceDispatcher::with_detector(
            Arc::new(detector),
            nats,
            checkpoint_store,
            ScheduledTaskMetrics::new(),
            NamespaceDispatcherConfig::default(),
        )
    }

    fn one_namespace() -> Vec<DispatchNamespace> {
        vec![DispatchNamespace {
            namespace_id: 9,
            traversal_path: "1/9/".to_string(),
        }]
    }

    #[tokio::test]
    async fn checkpoint_advances_after_successful_publish() {
        let checkpoint_store = Arc::new(StubCheckpointStore::default());
        let dispatcher = dispatcher_with(
            StubDetector {
                result: Ok(one_namespace()),
            },
            Arc::new(MockNatsServices::new()),
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
            Arc::new(MockNatsServices::new()),
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
                result: Ok(one_namespace()),
            },
            Arc::new(MockNatsServices::failing()),
            checkpoint_store.clone(),
        );

        let err = dispatcher.dispatch_inner().await.unwrap_err();

        assert!(err.to_string().contains("mock publish failure"));
        assert!(checkpoint_store.saved().is_empty());
    }

    #[tokio::test]
    async fn first_run_without_checkpoint_uses_configured_lookback_floor() {
        let upper = Utc::now();
        let config = NamespaceDispatcherConfig {
            max_lookback_secs: 45,
            ..Default::default()
        };
        let dispatcher = NamespaceDispatcher::with_detector(
            CapturingDetector::new(),
            Arc::new(MockNatsServices::new()),
            Arc::new(StubCheckpointStore::default()),
            ScheduledTaskMetrics::new(),
            config,
        );

        let lower = dispatcher.lower_bound(upper).await.unwrap();

        assert_eq!(lower, upper - Duration::seconds(45));
        assert_ne!(lower, DateTime::<Utc>::UNIX_EPOCH);
    }

    #[tokio::test]
    async fn stale_checkpoint_clamps_lower_to_max_lookback() {
        let detector = CapturingDetector::new();
        let checkpoint_store = Arc::new(StubCheckpointStore::default());
        *checkpoint_store.loaded.lock().unwrap() = Some(checkpoint_at(DateTime::<Utc>::UNIX_EPOCH));
        let dispatcher = dispatcher_with_detector(detector.clone(), checkpoint_store);

        dispatcher.dispatch_inner().await.unwrap();

        let lookback = Utc::now() - detector.lower();
        assert!(lookback <= Duration::seconds(40), "not clamped: {lookback}");
    }

    #[tokio::test]
    async fn fresh_checkpoint_within_window_is_used_verbatim() {
        let detector = CapturingDetector::new();
        let watermark = Utc::now() - Duration::seconds(5);
        let checkpoint_store = Arc::new(StubCheckpointStore::default());
        *checkpoint_store.loaded.lock().unwrap() = Some(checkpoint_at(watermark));
        let dispatcher = dispatcher_with_detector(detector.clone(), checkpoint_store);

        dispatcher.dispatch_inner().await.unwrap();

        assert_eq!(detector.lower(), watermark);
    }
}
