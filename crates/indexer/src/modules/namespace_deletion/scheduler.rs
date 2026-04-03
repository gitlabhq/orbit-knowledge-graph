use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use tracing::{debug, info};

use super::NamespaceDeletionStore;
use crate::checkpoint::CheckpointStore;
use crate::clickhouse::TIMESTAMP_FORMAT;
use crate::nats::NatsServices;
use crate::scheduler::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::topic::NamespaceDeletionRequest;
use crate::types::Envelope;
use gkg_server_config::{NamespaceDeletionSchedulerConfig, ScheduleConfiguration};

const CHECKPOINT_KEY: &str = "namespace_deletion_scheduler";
const GRACE_PERIOD_DAYS: i64 = 30;

pub struct NamespaceDeletionScheduler {
    store: Arc<dyn NamespaceDeletionStore>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    nats: Arc<dyn NatsServices>,
    metrics: ScheduledTaskMetrics,
    config: NamespaceDeletionSchedulerConfig,
}

impl NamespaceDeletionScheduler {
    pub fn new(
        store: Arc<dyn NamespaceDeletionStore>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        nats: Arc<dyn NatsServices>,
        metrics: ScheduledTaskMetrics,
        config: NamespaceDeletionSchedulerConfig,
    ) -> Self {
        Self {
            store,
            checkpoint_store,
            nats,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl ScheduledTask for NamespaceDeletionScheduler {
    fn name(&self) -> &str {
        "dispatch.namespace_deletion"
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let start = Instant::now();
        let result = self.run_inner().await;

        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics.record_run(self.name(), outcome, duration);

        result
    }
}

impl NamespaceDeletionScheduler {
    async fn run_inner(&self) -> Result<(), TaskError> {
        self.record_newly_deleted_namespaces().await?;
        self.dispatch_due_deletions().await?;
        Ok(())
    }

    async fn record_newly_deleted_namespaces(&self) -> Result<(), TaskError> {
        let checkpoint = self
            .checkpoint_store
            .load(CHECKPOINT_KEY)
            .await
            .map_err(TaskError::new)?;

        let last_watermark = checkpoint
            .as_ref()
            .map(|cp| cp.watermark)
            .unwrap_or(DateTime::UNIX_EPOCH);
        let watermark = Utc::now();

        let last_watermark_str = last_watermark.format(TIMESTAMP_FORMAT).to_string();
        let watermark_str = watermark.format(TIMESTAMP_FORMAT).to_string();

        let entries = self
            .store
            .find_newly_deleted_namespaces(&last_watermark_str, &watermark_str)
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "query");
                TaskError::new(error)
            })?;

        let scheduled_deletion_date = (Utc::now() + Duration::days(GRACE_PERIOD_DAYS))
            .format(TIMESTAMP_FORMAT)
            .to_string();

        let mut recorded = 0u64;

        for entry in &entries {
            self.store
                .schedule_deletion(
                    entry.namespace_id,
                    &entry.traversal_path,
                    &scheduled_deletion_date,
                )
                .await
                .map_err(|error| {
                    self.metrics.record_error(self.name(), "insert");
                    TaskError::new(error)
                })?;
            recorded += 1;
        }

        self.checkpoint_store
            .save_completed(CHECKPOINT_KEY, &watermark)
            .await
            .map_err(TaskError::new)?;

        info!(
            recorded,
            "recorded newly deleted namespaces for scheduled deletion"
        );
        Ok(())
    }

    async fn dispatch_due_deletions(&self) -> Result<(), TaskError> {
        let entries = self.store.find_due_deletions().await.map_err(|error| {
            self.metrics.record_error(self.name(), "query");
            TaskError::new(error)
        })?;

        let mut dispatched = 0u64;
        let mut skipped = 0u64;

        for entry in &entries {
            let request = NamespaceDeletionRequest {
                namespace_id: entry.namespace_id,
                traversal_path: entry.traversal_path.clone(),
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
                        namespace_id = entry.namespace_id,
                        "dispatched namespace deletion request"
                    );
                }
                Err(crate::nats::NatsError::PublishDuplicate) => {
                    skipped += 1;
                    debug!(
                        namespace_id = entry.namespace_id,
                        "skipped, deletion request already in-flight"
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

        info!(dispatched, skipped, "dispatched due namespace deletions");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::namespace_deletion::store::NamespaceScheduleEntry;
    use crate::modules::namespace_deletion::store::test_utils::MockNamespaceDeletionStore;
    use crate::testkit::mocks::MockNatsServices;

    use crate::checkpoint::{Checkpoint, CheckpointError};
    use crate::nats::{NatsError, NatsServices};
    use crate::types::{Envelope, Subscription};

    struct MockCheckpointStore;

    #[async_trait]
    impl CheckpointStore for MockCheckpointStore {
        async fn load(&self, _key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
            Ok(None)
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
            _watermark: &chrono::DateTime<Utc>,
        ) -> Result<(), CheckpointError> {
            Ok(())
        }
    }

    fn scheduler_with_store(store: Arc<dyn NamespaceDeletionStore>) -> NamespaceDeletionScheduler {
        NamespaceDeletionScheduler::new(
            store,
            Arc::new(MockCheckpointStore),
            Arc::new(MockNatsServices::new()),
            ScheduledTaskMetrics::new(),
            NamespaceDeletionSchedulerConfig::default(),
        )
    }

    #[tokio::test]
    async fn schedules_newly_deleted_namespaces() {
        let store = Arc::new(MockNamespaceDeletionStore::new().with_newly_deleted(vec![
            NamespaceScheduleEntry {
                namespace_id: 100,
                traversal_path: "1/100/".to_string(),
            },
        ]));

        let scheduler = scheduler_with_store(store.clone());
        scheduler.run().await.unwrap();

        let calls = store.schedule_calls();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, 100);
        assert_eq!(calls[0].1, "1/100/");
    }

    #[tokio::test]
    async fn dispatches_due_deletions_via_nats() {
        let store = Arc::new(MockNamespaceDeletionStore::new().with_due_deletions(vec![
            NamespaceScheduleEntry {
                namespace_id: 200,
                traversal_path: "1/200/".to_string(),
            },
        ]));

        let nats = Arc::new(MockNatsServices::new());
        let scheduler = NamespaceDeletionScheduler::new(
            store,
            Arc::new(MockCheckpointStore),
            nats.clone(),
            ScheduledTaskMetrics::new(),
            NamespaceDeletionSchedulerConfig::default(),
        );

        scheduler.run().await.unwrap();

        let published = nats.get_published();
        assert_eq!(published.len(), 1);
        assert!(published[0].0.subject.contains("namespace.deletion"));
    }

    #[tokio::test]
    async fn handles_empty_results() {
        let store = Arc::new(MockNamespaceDeletionStore::new());
        let scheduler = scheduler_with_store(store.clone());

        scheduler.run().await.unwrap();

        assert!(store.schedule_calls().is_empty());
    }

    #[tokio::test]
    async fn schedule_deletion_failure_returns_error() {
        let store = Arc::new(
            MockNamespaceDeletionStore::new()
                .failing_schedule()
                .with_newly_deleted(vec![NamespaceScheduleEntry {
                    namespace_id: 100,
                    traversal_path: "1/100/".to_string(),
                }]),
        );

        let scheduler = scheduler_with_store(store);
        let result = scheduler.run().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn skips_duplicate_publish_without_error() {
        let store = Arc::new(MockNamespaceDeletionStore::new().with_due_deletions(vec![
            NamespaceScheduleEntry {
                namespace_id: 200,
                traversal_path: "1/200/".to_string(),
            },
        ]));

        let nats = Arc::new(DuplicateNatsServices);
        let scheduler = NamespaceDeletionScheduler::new(
            store,
            Arc::new(MockCheckpointStore),
            nats,
            ScheduledTaskMetrics::new(),
            NamespaceDeletionSchedulerConfig::default(),
        );

        scheduler.run().await.unwrap();
    }

    /// A NatsServices mock that always returns PublishDuplicate.
    struct DuplicateNatsServices;

    #[async_trait]
    impl NatsServices for DuplicateNatsServices {
        async fn publish(
            &self,
            _subscription: &Subscription,
            _envelope: &Envelope,
        ) -> Result<(), NatsError> {
            Err(NatsError::PublishDuplicate)
        }

        async fn kv_get(
            &self,
            _bucket: &str,
            _key: &str,
        ) -> Result<Option<crate::nats::KvEntry>, NatsError> {
            unimplemented!()
        }

        async fn kv_put(
            &self,
            _bucket: &str,
            _key: &str,
            _value: bytes::Bytes,
            _options: crate::nats::KvPutOptions,
        ) -> Result<crate::nats::KvPutResult, NatsError> {
            unimplemented!()
        }

        async fn kv_delete(&self, _bucket: &str, _key: &str) -> Result<(), NatsError> {
            unimplemented!()
        }

        async fn kv_keys(&self, _bucket: &str) -> Result<Vec<String>, NatsError> {
            unimplemented!()
        }

        async fn consume_pending(
            &self,
            _subscription: &Subscription,
            _batch_size: usize,
        ) -> Result<Vec<crate::nats::NatsMessage>, NatsError> {
            unimplemented!()
        }
    }
}
