use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ontology::EtlScope;
use tracing::{debug, info, warn};

use crate::checkpoint::{CheckpointStore, entity_checkpoint_key};
use crate::clickhouse::ArrowClickHouseClient;
use crate::nats::NatsServices;
use crate::scheduler::ScheduledTaskMetrics;
use crate::scheduler::{ScheduledTask, TaskError};
use crate::topic::{EntityIndexingRequest, IndexingScope, PartitionSpec, PartitionStrategy};
use crate::types::Envelope;
use clickhouse_client::FromArrowColumn;
use gkg_server_config::{EntityDispatcherConfig, ScheduleConfiguration};

use super::boundaries;

const ENABLED_NAMESPACE_QUERY: &str = r#"
SELECT root_namespace_id, traversal_path
FROM siphon_knowledge_graph_enabled_namespaces
WHERE _siphon_deleted = false
  AND traversal_path != '0/'
"#;

const MAX_PARTITION_UPPER_BOUND: &str = "99999999999999999999";

pub struct EntityDescriptor {
    pub entity_kind: String,
    pub scope: EtlScope,
    pub source_table: Option<String>,
    pub partition_column: Option<String>,
}

pub struct EntityDispatcher {
    nats: Arc<dyn NatsServices>,
    datalake: ArrowClickHouseClient,
    metrics: ScheduledTaskMetrics,
    config: EntityDispatcherConfig,
    entities: Vec<EntityDescriptor>,
    checkpoint_store: Arc<dyn CheckpointStore>,
}

impl EntityDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        datalake: ArrowClickHouseClient,
        metrics: ScheduledTaskMetrics,
        config: EntityDispatcherConfig,
        entities: Vec<EntityDescriptor>,
        checkpoint_store: Arc<dyn CheckpointStore>,
    ) -> Self {
        Self {
            nats,
            datalake,
            metrics,
            config,
            entities,
            checkpoint_store,
        }
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
        let start = Instant::now();

        let result = self.dispatch_inner().await;

        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics.record_run(self.name(), outcome, duration);

        result
    }
}

struct EnabledNamespace {
    namespace_id: i64,
    traversal_path: String,
}

#[derive(Debug, PartialEq)]
enum DispatchStrategy {
    Incremental,
    InitialPartitioned {
        partition_count: u32,
    },
    ResumePartitions {
        incomplete_indices: Vec<u32>,
        total: u32,
    },
    Consolidate {
        watermark: DateTime<Utc>,
        total: u32,
    },
}

impl EntityDispatcher {
    async fn dispatch_inner(&self) -> Result<(), TaskError> {
        let namespaces = self.load_enabled_namespaces().await?;

        let watermark = Utc::now();
        let mut dispatched: u64 = 0;
        let mut skipped: u64 = 0;

        for entity in &self.entities {
            let partition_count = self
                .config
                .partition_overrides
                .get(&entity.entity_kind)
                .copied()
                .unwrap_or(1);

            let scopes: Vec<IndexingScope> = match entity.scope {
                EtlScope::Global => vec![IndexingScope::Global],
                EtlScope::Namespaced => namespaces
                    .iter()
                    .map(|ns| IndexingScope::Namespace {
                        namespace_id: ns.namespace_id,
                        traversal_path: ns.traversal_path.clone(),
                    })
                    .collect(),
            };

            for scope in scopes {
                let strategy = self
                    .determine_strategy(&entity.entity_kind, &scope, partition_count)
                    .await?;

                let (published, was_skipped) = self
                    .execute_strategy(entity, &scope, watermark, &strategy)
                    .await?;
                dispatched += published;
                skipped += was_skipped;
            }
        }

        self.metrics
            .record_requests_published(self.name(), dispatched);
        self.metrics.record_requests_skipped(self.name(), skipped);

        info!(
            dispatched,
            skipped,
            entity_count = self.entities.len(),
            "dispatched entity indexing requests"
        );
        Ok(())
    }

    async fn determine_strategy(
        &self,
        entity_kind: &str,
        scope: &IndexingScope,
        partition_count: u32,
    ) -> Result<DispatchStrategy, TaskError> {
        let unpartitioned_key = entity_checkpoint_key(scope, entity_kind, None);
        let unpartitioned_checkpoint = self
            .checkpoint_store
            .load(&unpartitioned_key)
            .await
            .map_err(TaskError::new)?;

        if let Some(checkpoint) = unpartitioned_checkpoint
            && checkpoint.watermark.timestamp_micros() > 0
        {
            return Ok(DispatchStrategy::Incremental);
        }

        if partition_count <= 1 {
            return Ok(DispatchStrategy::Incremental);
        }

        let mut incomplete_indices = Vec::new();
        let mut all_completed = true;
        let mut any_exist = false;
        let mut min_watermark: Option<DateTime<Utc>> = None;

        for i in 0..partition_count {
            let spec = PartitionSpec {
                partition_index: i,
                total_partitions: partition_count,
                strategy: PartitionStrategy::Range {
                    lower_bound: String::new(),
                    upper_bound: String::new(),
                },
            };
            let key = entity_checkpoint_key(scope, entity_kind, Some(&spec));
            let checkpoint = self
                .checkpoint_store
                .load(&key)
                .await
                .map_err(TaskError::new)?;

            match checkpoint {
                Some(cp) => {
                    any_exist = true;
                    if cp.cursor_values.is_some() {
                        all_completed = false;
                        incomplete_indices.push(i);
                    } else {
                        let wm = cp.watermark;
                        min_watermark = Some(match min_watermark {
                            Some(current) if wm < current => wm,
                            Some(current) => current,
                            None => wm,
                        });
                    }
                }
                None => {
                    all_completed = false;
                    incomplete_indices.push(i);
                }
            }
        }

        if !any_exist {
            return Ok(DispatchStrategy::InitialPartitioned { partition_count });
        }

        if all_completed {
            let watermark = min_watermark.unwrap_or_else(Utc::now);
            return Ok(DispatchStrategy::Consolidate {
                watermark,
                total: partition_count,
            });
        }

        Ok(DispatchStrategy::ResumePartitions {
            incomplete_indices,
            total: partition_count,
        })
    }

    async fn execute_strategy(
        &self,
        entity: &EntityDescriptor,
        scope: &IndexingScope,
        watermark: DateTime<Utc>,
        strategy: &DispatchStrategy,
    ) -> Result<(u64, u64), TaskError> {
        match strategy {
            DispatchStrategy::Incremental => {
                self.publish_unpartitioned(entity, scope, watermark).await
            }
            DispatchStrategy::InitialPartitioned { partition_count } => {
                let (source_table, partition_column) = match (
                    &entity.source_table,
                    &entity.partition_column,
                ) {
                    (Some(table), Some(column)) => (table.as_str(), column.as_str()),
                    _ => {
                        debug!(
                            entity_kind = %entity.entity_kind,
                            "partition override set but entity lacks source_table or partition_column, falling back to incremental"
                        );
                        return self.publish_unpartitioned(entity, scope, watermark).await;
                    }
                };

                let boundary_key = boundaries::boundaries_key(&entity.entity_kind, scope);
                let boundary_values = boundaries::compute_boundaries(
                    &self.datalake,
                    source_table,
                    partition_column,
                    *partition_count,
                    scope,
                )
                .await?;

                boundaries::save_boundaries(self.nats.as_ref(), &boundary_key, &boundary_values)
                    .await?;

                let requests = build_partition_requests(
                    &entity.entity_kind,
                    watermark,
                    scope,
                    &boundary_values,
                    *partition_count,
                );
                self.publish_all(&requests).await
            }
            DispatchStrategy::ResumePartitions {
                incomplete_indices,
                total,
            } => {
                let boundary_key = boundaries::boundaries_key(&entity.entity_kind, scope);
                let boundary_values =
                    boundaries::load_boundaries(self.nats.as_ref(), &boundary_key).await?;

                let Some(boundary_values) = boundary_values else {
                    warn!(
                        entity_kind = %entity.entity_kind,
                        scope = ?scope,
                        "partition boundaries not found in KV, falling back to incremental"
                    );
                    return self.publish_unpartitioned(entity, scope, watermark).await;
                };

                let requests: Vec<_> = build_partition_requests(
                    &entity.entity_kind,
                    watermark,
                    scope,
                    &boundary_values,
                    *total,
                )
                .into_iter()
                .filter(|r| {
                    r.partition
                        .as_ref()
                        .is_some_and(|s| incomplete_indices.contains(&s.partition_index))
                })
                .collect();
                self.publish_all(&requests).await
            }
            DispatchStrategy::Consolidate { watermark, total } => {
                self.checkpoint_store
                    .save_completed(
                        &entity_checkpoint_key(scope, &entity.entity_kind, None),
                        watermark,
                    )
                    .await
                    .map_err(TaskError::new)?;

                let boundary_key = boundaries::boundaries_key(&entity.entity_kind, scope);
                boundaries::delete_boundaries(self.nats.as_ref(), &boundary_key).await?;

                info!(
                    entity_kind = %entity.entity_kind,
                    scope = ?scope,
                    total_partitions = total,
                    consolidated_watermark = %watermark,
                    "consolidated partitioned checkpoints into incremental"
                );

                self.publish_unpartitioned(entity, scope, *watermark).await
            }
        }
    }

    async fn publish_unpartitioned(
        &self,
        entity: &EntityDescriptor,
        scope: &IndexingScope,
        watermark: DateTime<Utc>,
    ) -> Result<(u64, u64), TaskError> {
        let request = EntityIndexingRequest {
            entity_kind: entity.entity_kind.clone(),
            watermark,
            scope: scope.clone(),
            partition: None,
        };
        match self.publish_request(&request).await? {
            PublishOutcome::Published => Ok((1, 0)),
            PublishOutcome::Skipped => Ok((0, 1)),
        }
    }

    async fn publish_all(
        &self,
        requests: &[EntityIndexingRequest],
    ) -> Result<(u64, u64), TaskError> {
        let mut published = 0u64;
        let mut skipped = 0u64;
        for request in requests {
            match self.publish_request(request).await? {
                PublishOutcome::Published => published += 1,
                PublishOutcome::Skipped => skipped += 1,
            }
        }
        Ok((published, skipped))
    }

    async fn load_enabled_namespaces(&self) -> Result<Vec<EnabledNamespace>, TaskError> {
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
        let traversal_paths = String::extract_column(&arrow_batches, 1).map_err(TaskError::new)?;

        let namespaces: Vec<EnabledNamespace> = namespace_ids
            .into_iter()
            .zip(traversal_paths)
            .filter(|(id, path)| {
                let valid = gkg_utils::traversal_path::is_valid(path);
                if !valid {
                    warn!(
                        namespace_id = *id,
                        traversal_path = %path,
                        "skipping enabled namespace with invalid traversal_path"
                    );
                }
                valid
            })
            .map(|(id, path)| EnabledNamespace {
                namespace_id: id,
                traversal_path: path,
            })
            .collect();

        debug!(
            enabled_namespaces = namespaces.len(),
            "loaded enabled namespaces"
        );
        Ok(namespaces)
    }

    async fn publish_request(
        &self,
        request: &EntityIndexingRequest,
    ) -> Result<PublishOutcome, TaskError> {
        let subscription = request.publish_subscription();
        let envelope = Envelope::new(request).map_err(|error| {
            self.metrics.record_error(self.name(), "publish");
            TaskError::new(error)
        })?;

        match self.nats.publish(&subscription, &envelope).await {
            Ok(()) => {
                debug!(
                    entity_kind = %request.entity_kind,
                    scope = ?request.scope,
                    partition = ?request.partition,
                    "dispatched entity indexing request"
                );
                Ok(PublishOutcome::Published)
            }
            Err(crate::nats::NatsError::PublishDuplicate) => {
                debug!(
                    entity_kind = %request.entity_kind,
                    scope = ?request.scope,
                    partition = ?request.partition,
                    "skipped entity indexing request, already in-flight"
                );
                Ok(PublishOutcome::Skipped)
            }
            Err(error) => {
                self.metrics.record_error(self.name(), "publish");
                Err(TaskError::new(error))
            }
        }
    }
}

enum PublishOutcome {
    Published,
    Skipped,
}

fn build_partition_requests(
    entity_kind: &str,
    watermark: DateTime<Utc>,
    scope: &IndexingScope,
    boundaries: &[String],
    partition_count: u32,
) -> Vec<EntityIndexingRequest> {
    let mut requests = Vec::with_capacity(partition_count as usize);

    for i in 0..partition_count {
        let lower_bound = if i == 0 {
            String::new()
        } else {
            boundaries[(i - 1) as usize].clone()
        };

        let upper_bound = if i == partition_count - 1 {
            MAX_PARTITION_UPPER_BOUND.to_string()
        } else {
            boundaries[i as usize].clone()
        };

        requests.push(EntityIndexingRequest {
            entity_kind: entity_kind.to_string(),
            watermark,
            scope: scope.clone(),
            partition: Some(PartitionSpec {
                partition_index: i,
                total_partitions: partition_count,
                strategy: PartitionStrategy::Range {
                    lower_bound,
                    upper_bound,
                },
            }),
        });
    }

    requests
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use async_trait::async_trait;
    use chrono::{DateTime, Utc};
    use parking_lot::Mutex;

    use crate::checkpoint::{Checkpoint, CheckpointError, CheckpointStore};
    use crate::topic::{IndexingScope, PartitionSpec, PartitionStrategy};

    use super::*;

    struct MapCheckpointStore {
        data: Mutex<HashMap<String, Checkpoint>>,
    }

    impl MapCheckpointStore {
        fn new() -> Self {
            Self {
                data: Mutex::new(HashMap::new()),
            }
        }

        fn insert(&self, key: &str, checkpoint: Checkpoint) {
            self.data.lock().insert(key.to_string(), checkpoint);
        }
    }

    #[async_trait]
    impl CheckpointStore for MapCheckpointStore {
        async fn load(&self, key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
            Ok(self.data.lock().get(key).cloned())
        }

        async fn save_progress(
            &self,
            key: &str,
            checkpoint: &Checkpoint,
        ) -> Result<(), CheckpointError> {
            self.data.lock().insert(key.to_string(), checkpoint.clone());
            Ok(())
        }

        async fn save_completed(
            &self,
            key: &str,
            watermark: &DateTime<Utc>,
        ) -> Result<(), CheckpointError> {
            self.data.lock().insert(
                key.to_string(),
                Checkpoint {
                    watermark: *watermark,
                    cursor_values: None,
                },
            );
            Ok(())
        }
    }

    fn stub_dispatcher(checkpoint_store: Arc<dyn CheckpointStore>) -> EntityDispatcher {
        let nats: Arc<dyn NatsServices> = Arc::new(crate::testkit::MockNatsServices::new());
        let datalake = clickhouse_client::ArrowClickHouseClient::new(
            "http://localhost:8123",
            "default",
            "default",
            None,
            &std::collections::HashMap::new(),
        );
        let metrics = ScheduledTaskMetrics::with_meter(&crate::testkit::test_meter());
        let config = EntityDispatcherConfig::default();
        let entities = vec![];
        EntityDispatcher::new(nats, datalake, metrics, config, entities, checkpoint_store)
    }

    #[tokio::test]
    async fn determine_strategy_incremental_when_completed_checkpoint_exists() {
        let store = Arc::new(MapCheckpointStore::new());
        store.insert(
            "global.User",
            Checkpoint {
                watermark: "2024-06-15T12:00:00Z".parse().unwrap(),
                cursor_values: None,
            },
        );

        let dispatcher = stub_dispatcher(store);
        let strategy = dispatcher
            .determine_strategy("User", &IndexingScope::Global, 4)
            .await
            .unwrap();

        assert_eq!(strategy, DispatchStrategy::Incremental);
    }

    #[tokio::test]
    async fn determine_strategy_incremental_when_in_progress_checkpoint_exists() {
        let store = Arc::new(MapCheckpointStore::new());
        store.insert(
            "global.User",
            Checkpoint {
                watermark: "2024-06-15T12:00:00Z".parse().unwrap(),
                cursor_values: Some(vec!["42".to_string()]),
            },
        );

        let dispatcher = stub_dispatcher(store);
        let strategy = dispatcher
            .determine_strategy("User", &IndexingScope::Global, 4)
            .await
            .unwrap();

        assert_eq!(strategy, DispatchStrategy::Incremental);
    }

    #[tokio::test]
    async fn determine_strategy_incremental_when_no_checkpoint_and_partition_count_one() {
        let store = Arc::new(MapCheckpointStore::new());
        let dispatcher = stub_dispatcher(store);
        let strategy = dispatcher
            .determine_strategy("User", &IndexingScope::Global, 1)
            .await
            .unwrap();

        assert_eq!(strategy, DispatchStrategy::Incremental);
    }

    #[tokio::test]
    async fn determine_strategy_initial_partitioned_when_no_checkpoints() {
        let store = Arc::new(MapCheckpointStore::new());
        let dispatcher = stub_dispatcher(store);
        let strategy = dispatcher
            .determine_strategy("MergeRequest", &IndexingScope::Global, 4)
            .await
            .unwrap();

        assert_eq!(
            strategy,
            DispatchStrategy::InitialPartitioned { partition_count: 4 }
        );
    }

    #[tokio::test]
    async fn determine_strategy_resume_when_some_partitions_incomplete() {
        let store = Arc::new(MapCheckpointStore::new());
        let scope = IndexingScope::Namespace {
            namespace_id: 100,
            traversal_path: "42/100/".to_string(),
        };

        let completed_spec = PartitionSpec {
            partition_index: 0,
            total_partitions: 3,
            strategy: PartitionStrategy::Range {
                lower_bound: String::new(),
                upper_bound: String::new(),
            },
        };
        store.insert(
            &entity_checkpoint_key(&scope, "MergeRequest", Some(&completed_spec)),
            Checkpoint {
                watermark: "2024-06-15T12:00:00Z".parse().unwrap(),
                cursor_values: None,
            },
        );

        let in_progress_spec = PartitionSpec {
            partition_index: 1,
            total_partitions: 3,
            strategy: PartitionStrategy::Range {
                lower_bound: String::new(),
                upper_bound: String::new(),
            },
        };
        store.insert(
            &entity_checkpoint_key(&scope, "MergeRequest", Some(&in_progress_spec)),
            Checkpoint {
                watermark: "2024-06-15T12:00:00Z".parse().unwrap(),
                cursor_values: Some(vec!["50".to_string()]),
            },
        );

        let dispatcher = stub_dispatcher(store);
        let strategy = dispatcher
            .determine_strategy("MergeRequest", &scope, 3)
            .await
            .unwrap();

        assert_eq!(
            strategy,
            DispatchStrategy::ResumePartitions {
                incomplete_indices: vec![1, 2],
                total: 3,
            }
        );
    }

    #[tokio::test]
    async fn determine_strategy_consolidate_when_all_partitions_completed() {
        let store = Arc::new(MapCheckpointStore::new());
        let scope = IndexingScope::Global;
        let watermark: DateTime<Utc> = "2024-06-15T12:00:00Z".parse().unwrap();

        for i in 0..3u32 {
            let spec = PartitionSpec {
                partition_index: i,
                total_partitions: 3,
                strategy: PartitionStrategy::Range {
                    lower_bound: String::new(),
                    upper_bound: String::new(),
                },
            };
            store.insert(
                &entity_checkpoint_key(&scope, "User", Some(&spec)),
                Checkpoint {
                    watermark,
                    cursor_values: None,
                },
            );
        }

        let dispatcher = stub_dispatcher(store);
        let strategy = dispatcher
            .determine_strategy("User", &scope, 3)
            .await
            .unwrap();

        assert_eq!(
            strategy,
            DispatchStrategy::Consolidate {
                watermark,
                total: 3,
            }
        );
    }

    #[test]
    fn build_partition_requests_produces_correct_ranges() {
        let watermark: DateTime<Utc> = "2024-01-01T00:00:00Z".parse().unwrap();
        let scope = IndexingScope::Global;
        let boundaries = vec![
            "25000000".to_string(),
            "50000000".to_string(),
            "75000000".to_string(),
        ];

        let requests = build_partition_requests("MergeRequest", watermark, &scope, &boundaries, 4);

        assert_eq!(requests.len(), 4);

        let spec0 = requests[0].partition.as_ref().unwrap();
        assert_eq!(spec0.partition_index, 0);
        assert_eq!(spec0.total_partitions, 4);
        match &spec0.strategy {
            PartitionStrategy::Range {
                lower_bound,
                upper_bound,
            } => {
                assert_eq!(lower_bound, "");
                assert_eq!(upper_bound, "25000000");
            }
        }

        let spec1 = requests[1].partition.as_ref().unwrap();
        assert_eq!(spec1.partition_index, 1);
        match &spec1.strategy {
            PartitionStrategy::Range {
                lower_bound,
                upper_bound,
            } => {
                assert_eq!(lower_bound, "25000000");
                assert_eq!(upper_bound, "50000000");
            }
        }

        let spec2 = requests[2].partition.as_ref().unwrap();
        assert_eq!(spec2.partition_index, 2);
        match &spec2.strategy {
            PartitionStrategy::Range {
                lower_bound,
                upper_bound,
            } => {
                assert_eq!(lower_bound, "50000000");
                assert_eq!(upper_bound, "75000000");
            }
        }

        let spec3 = requests[3].partition.as_ref().unwrap();
        assert_eq!(spec3.partition_index, 3);
        match &spec3.strategy {
            PartitionStrategy::Range {
                lower_bound,
                upper_bound,
            } => {
                assert_eq!(lower_bound, "75000000");
                assert_eq!(upper_bound, MAX_PARTITION_UPPER_BOUND);
            }
        }
    }

    #[test]
    fn build_partition_requests_single_partition() {
        let watermark: DateTime<Utc> = "2024-01-01T00:00:00Z".parse().unwrap();
        let scope = IndexingScope::Global;
        let boundaries: Vec<String> = vec![];

        let requests = build_partition_requests("User", watermark, &scope, &boundaries, 1);

        assert_eq!(requests.len(), 1);
        let spec = requests[0].partition.as_ref().unwrap();
        assert_eq!(spec.partition_index, 0);
        assert_eq!(spec.total_partitions, 1);
        match &spec.strategy {
            PartitionStrategy::Range {
                lower_bound,
                upper_bound,
            } => {
                assert_eq!(lower_bound, "");
                assert_eq!(upper_bound, MAX_PARTITION_UPPER_BOUND);
            }
        }
    }
}
