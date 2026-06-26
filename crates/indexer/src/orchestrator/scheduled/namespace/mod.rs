mod change_detection;
mod sweep;

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use tracing::info;

use change_detection::{DatalakeChangeDetector, NamespaceChangeDetector};
use sweep::{DatalakeEnabledNamespaceReader, EnabledNamespaceReader};

use crate::campaign::CampaignState;
use crate::checkpoint::CheckpointStore;
use crate::clickhouse::ArrowClickHouseClient;
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

pub const CHECKPOINT_KEY: &str = "dispatch.sdlc.namespace.changes";

pub struct NamespaceDispatcher {
    detector: Arc<dyn NamespaceChangeDetector>,
    reader: Arc<dyn EnabledNamespaceReader>,
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
            detector: Arc::new(DatalakeChangeDetector::new(datalake.clone(), ontology)),
            reader: Arc::new(DatalakeEnabledNamespaceReader::new(datalake)),
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
        reader: Arc<dyn EnabledNamespaceReader>,
        nats: Arc<dyn NatsServices>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: ScheduledTaskMetrics,
        config: NamespaceDispatcherConfig,
    ) -> Self {
        Self {
            detector,
            reader,
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
        let namespaces = self.namespaces_to_dispatch(upper).await?;

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

    async fn namespaces_to_dispatch(
        &self,
        upper: DateTime<Utc>,
    ) -> Result<Vec<DispatchNamespace>, TaskError> {
        match self.resume_watermark(upper).await? {
            Some(watermark) => self.changed_since(watermark, upper).await,
            None => self.all_enabled_namespaces().await,
        }
    }

    async fn resume_watermark(
        &self,
        upper: DateTime<Utc>,
    ) -> Result<Option<DateTime<Utc>>, TaskError> {
        let floor = upper - Duration::seconds(self.config.max_lookback_secs as i64);
        let checkpoint = self
            .checkpoint_store
            .load(CHECKPOINT_KEY)
            .await
            .map_err(TaskError::new)?;
        Ok(checkpoint
            .map(|checkpoint| checkpoint.watermark)
            .filter(|watermark| *watermark >= floor))
    }

    async fn changed_since(
        &self,
        watermark: DateTime<Utc>,
        upper: DateTime<Utc>,
    ) -> Result<Vec<DispatchNamespace>, TaskError> {
        self.timed_query(
            "namespace_changes",
            self.detector.changed_namespaces(watermark, upper),
        )
        .await
    }

    async fn all_enabled_namespaces(&self) -> Result<Vec<DispatchNamespace>, TaskError> {
        self.timed_query("enabled_namespaces", self.reader.enabled_namespaces())
            .await
    }

    async fn timed_query(
        &self,
        metric: &str,
        query: impl Future<Output = Result<Vec<DispatchNamespace>, TaskError>>,
    ) -> Result<Vec<DispatchNamespace>, TaskError> {
        let started = Instant::now();
        let namespaces = query
            .await
            .inspect_err(|_| self.metrics.record_error(self.name(), "query"))?;
        self.metrics
            .record_query_duration(metric, started.elapsed().as_secs_f64());
        Ok(namespaces)
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

        fn was_called(&self) -> bool {
            self.captured_lower.lock().unwrap().is_some()
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

    struct StubReader {
        result: Result<Vec<DispatchNamespace>, &'static str>,
        called: Mutex<bool>,
    }

    impl StubReader {
        fn new(result: Result<Vec<DispatchNamespace>, &'static str>) -> Arc<Self> {
            Arc::new(Self {
                result,
                called: Mutex::new(false),
            })
        }

        fn was_called(&self) -> bool {
            *self.called.lock().unwrap()
        }
    }

    #[async_trait]
    impl EnabledNamespaceReader for StubReader {
        async fn enabled_namespaces(&self) -> Result<Vec<DispatchNamespace>, TaskError> {
            *self.called.lock().unwrap() = true;
            self.result.clone().map_err(TaskError::new)
        }
    }

    fn checkpoint_at(watermark: DateTime<Utc>) -> Checkpoint {
        Checkpoint {
            watermark,
            cursor_values: None,
            resume_floor: None,
        }
    }

    fn fresh_checkpoint_store() -> Arc<StubCheckpointStore> {
        let store = Arc::new(StubCheckpointStore::default());
        *store.loaded.lock().unwrap() = Some(checkpoint_at(Utc::now() - Duration::seconds(5)));
        store
    }

    fn dispatcher_with(
        detector: Arc<dyn NamespaceChangeDetector>,
        reader: Arc<dyn EnabledNamespaceReader>,
        nats: Arc<dyn NatsServices>,
        checkpoint_store: Arc<StubCheckpointStore>,
    ) -> NamespaceDispatcher {
        NamespaceDispatcher::with_detector(
            detector,
            reader,
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
        let checkpoint_store = fresh_checkpoint_store();
        let dispatcher = dispatcher_with(
            Arc::new(StubDetector {
                result: Ok(one_namespace()),
            }),
            StubReader::new(Ok(Vec::new())),
            Arc::new(MockNatsServices::new()),
            checkpoint_store.clone(),
        );

        dispatcher.dispatch_inner().await.unwrap();

        assert_eq!(checkpoint_store.saved().len(), 1);
    }

    #[tokio::test]
    async fn checkpoint_does_not_advance_when_query_fails() {
        let checkpoint_store = fresh_checkpoint_store();
        let dispatcher = dispatcher_with(
            Arc::new(StubDetector {
                result: Err("query failed"),
            }),
            StubReader::new(Ok(Vec::new())),
            Arc::new(MockNatsServices::new()),
            checkpoint_store.clone(),
        );

        let err = dispatcher.dispatch_inner().await.unwrap_err();

        assert!(err.to_string().contains("query failed"));
        assert!(checkpoint_store.saved().is_empty());
    }

    #[tokio::test]
    async fn checkpoint_does_not_advance_when_publish_fails() {
        let checkpoint_store = fresh_checkpoint_store();
        let dispatcher = dispatcher_with(
            Arc::new(StubDetector {
                result: Ok(one_namespace()),
            }),
            StubReader::new(Ok(Vec::new())),
            Arc::new(MockNatsServices::failing()),
            checkpoint_store.clone(),
        );

        let err = dispatcher.dispatch_inner().await.unwrap_err();

        assert!(err.to_string().contains("mock publish failure"));
        assert!(checkpoint_store.saved().is_empty());
    }

    #[tokio::test]
    async fn first_run_without_checkpoint_dispatches_all_enabled_namespaces() {
        let detector = CapturingDetector::new();
        let reader = StubReader::new(Ok(one_namespace()));
        let nats = Arc::new(MockNatsServices::new());
        let checkpoint_store = Arc::new(StubCheckpointStore::default());
        let dispatcher = dispatcher_with(
            detector.clone(),
            reader.clone(),
            nats.clone(),
            checkpoint_store.clone(),
        );

        dispatcher.dispatch_inner().await.unwrap();

        assert!(
            reader.was_called(),
            "reader should drive the cold-start sweep"
        );
        assert!(
            !detector.was_called(),
            "detector should be skipped on cold start"
        );
        assert_eq!(nats.get_published().len(), 1);
        assert_eq!(checkpoint_store.saved().len(), 1);
    }

    #[tokio::test]
    async fn stale_checkpoint_falls_back_to_full_sweep() {
        let detector = CapturingDetector::new();
        let reader = StubReader::new(Ok(one_namespace()));
        let checkpoint_store = Arc::new(StubCheckpointStore::default());
        *checkpoint_store.loaded.lock().unwrap() = Some(checkpoint_at(DateTime::<Utc>::UNIX_EPOCH));
        let dispatcher = dispatcher_with(
            detector.clone(),
            reader.clone(),
            Arc::new(MockNatsServices::new()),
            checkpoint_store,
        );

        dispatcher.dispatch_inner().await.unwrap();

        assert!(
            reader.was_called(),
            "stale checkpoint should fall back to sweep"
        );
        assert!(
            !detector.was_called(),
            "detector should be skipped when stale"
        );
    }

    #[tokio::test]
    async fn fresh_checkpoint_within_window_is_used_verbatim() {
        let detector = CapturingDetector::new();
        let reader = StubReader::new(Ok(Vec::new()));
        let watermark = Utc::now() - Duration::seconds(5);
        let checkpoint_store = Arc::new(StubCheckpointStore::default());
        *checkpoint_store.loaded.lock().unwrap() = Some(checkpoint_at(watermark));
        let dispatcher = dispatcher_with(
            detector.clone(),
            reader.clone(),
            Arc::new(MockNatsServices::new()),
            checkpoint_store,
        );

        dispatcher.dispatch_inner().await.unwrap();

        assert_eq!(detector.lower(), watermark);
        assert!(
            !reader.was_called(),
            "fresh checkpoint should use incremental path"
        );
    }
}
