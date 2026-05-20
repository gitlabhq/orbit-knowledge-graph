use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse_client::FromArrowColumn;
use tracing::{debug, info, warn};

use super::partitioning::Partitioner;
use crate::checkpoint::{Checkpoint, CheckpointStore, EntityCheckpointKey};
use crate::clickhouse::ArrowClickHouseClient;
use crate::nats::NatsServices;
use crate::scheduler::ScheduledTaskMetrics;
use crate::scheduler::{ScheduledTask, TaskError};
use crate::topic::{EntityIndexingRequest, IndexingScope, PartitionAssignment};
use crate::types::{Envelope, Subscription};
use gkg_server_config::{EntityDispatcherConfig, ScheduleConfiguration};
use ontology::{EdgeSourceEtlConfig, EtlConfig, EtlScope, NodeEntity, Ontology};

const INDEXER_STREAM: &str = crate::topic::INDEXER_STREAM;

const ENABLED_NAMESPACE_QUERY: &str = r#"
SELECT root_namespace_id, traversal_path
FROM siphon_knowledge_graph_enabled_namespaces
WHERE _siphon_deleted = false
  AND traversal_path != ''
  AND traversal_path != '0/'
"#;

#[derive(Debug, Clone)]
struct PartitionConfig {
    count: u32,
    source_table: String,
    column: String,
}

#[derive(Debug, Clone)]
struct DispatchableEntity {
    name: String,
    scope: EtlScope,
    partition: Option<PartitionConfig>,
}

impl DispatchableEntity {
    fn from_node(node: &NodeEntity, partition_overrides: &HashMap<String, u32>) -> Option<Self> {
        let etl = node.etl.as_ref()?;
        let source_table = match etl {
            EtlConfig::Table { source, .. } => Some(source.as_str()),
            EtlConfig::Query { .. } => None,
        };
        let partition = Self::build_partition(
            &node.name,
            partition_overrides,
            source_table,
            etl.order_by(),
            etl.scope(),
        );
        Some(Self {
            name: node.name.clone(),
            scope: etl.scope(),
            partition,
        })
    }

    fn from_edge(
        relationship_kind: &str,
        etl: &EdgeSourceEtlConfig,
        partition_overrides: &HashMap<String, u32>,
    ) -> Self {
        let name = format!("{relationship_kind}_{}", etl.source);
        let partition = Self::build_partition(
            &name,
            partition_overrides,
            Some(&etl.source),
            &etl.order_by,
            etl.scope,
        );
        Self {
            name,
            scope: etl.scope,
            partition,
        }
    }

    fn build_partition(
        name: &str,
        overrides: &HashMap<String, u32>,
        source_table: Option<&str>,
        order_by: &[String],
        scope: EtlScope,
    ) -> Option<PartitionConfig> {
        let count = overrides.get(name).copied().filter(|&n| n > 1)?;
        let source_table = source_table?.to_owned();
        let skip = match scope {
            EtlScope::Namespaced => 1,
            EtlScope::Global => 0,
        };
        let column = order_by.get(skip)?.to_owned();
        Some(PartitionConfig {
            count,
            source_table,
            column,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum DispatchDecision {
    Single,
    AllPartitions,
    PendingPartitions(Vec<u32>),
}

pub struct EntityDispatcher {
    nats: Arc<dyn NatsServices>,
    datalake: ArrowClickHouseClient,
    checkpoint_store: Arc<dyn CheckpointStore>,
    partitioner: Arc<dyn Partitioner>,
    entities: Vec<DispatchableEntity>,
    metrics: ScheduledTaskMetrics,
    config: EntityDispatcherConfig,
}

impl EntityDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        datalake: ArrowClickHouseClient,
        checkpoint_store: Arc<dyn CheckpointStore>,
        partitioner: Arc<dyn Partitioner>,
        ontology: &Ontology,
        metrics: ScheduledTaskMetrics,
        config: EntityDispatcherConfig,
    ) -> Self {
        let overrides = &config.partition_overrides;
        let entities: Vec<_> = ontology
            .nodes()
            .filter_map(|n| DispatchableEntity::from_node(n, overrides))
            .chain(
                ontology
                    .edge_etl_configs()
                    .map(|(name, etl)| DispatchableEntity::from_edge(name, etl, overrides)),
            )
            .collect();

        info!(
            global_entities = entities.iter().filter(|e| e.scope == EtlScope::Global).count(),
            namespaced_entities = entities.iter().filter(|e| e.scope == EtlScope::Namespaced).count(),
            partition_overrides = ?config.partition_overrides,
            "entity dispatcher initialized"
        );

        Self {
            nats,
            datalake,
            checkpoint_store,
            partitioner,
            entities,
            metrics,
            config,
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
        let result = self.dispatch_all().await;
        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics.record_run(self.name(), outcome, duration);
        result
    }
}

impl EntityDispatcher {
    async fn dispatch_all(&self) -> Result<(), TaskError> {
        let watermark = Utc::now();
        let dispatch_id = uuid::Uuid::new_v4().to_string();
        let mut dispatched: u64 = 0;
        let mut skipped: u64 = 0;

        let (namespaced_entities, global_entities): (Vec<_>, Vec<_>) = self
            .entities
            .iter()
            .partition(|e| e.scope == EtlScope::Namespaced);

        for entity in &global_entities {
            let (d, s) = self
                .dispatch_entity(&dispatch_id, entity, &IndexingScope::Global, watermark)
                .await?;
            dispatched += d;
            skipped += s;
        }

        if namespaced_entities.is_empty() {
            self.metrics
                .record_requests_published(self.name(), dispatched);
            self.metrics.record_requests_skipped(self.name(), skipped);
            info!(
                dispatch_id,
                dispatched, skipped, "entity dispatcher completed (no namespaced entities)"
            );
            return Ok(());
        }

        let namespaces = self.load_enabled_namespaces().await?;

        for (namespace_id, traversal_path) in &namespaces {
            let scope = IndexingScope::Namespace {
                namespace_id: *namespace_id,
                traversal_path: traversal_path.clone(),
            };
            for entity in &namespaced_entities {
                let (d, s) = self
                    .dispatch_entity(&dispatch_id, entity, &scope, watermark)
                    .await?;
                dispatched += d;
                skipped += s;
            }
        }

        self.metrics
            .record_requests_published(self.name(), dispatched);
        self.metrics.record_requests_skipped(self.name(), skipped);

        info!(
            dispatch_id,
            dispatched,
            skipped,
            namespaces = namespaces.len(),
            "entity dispatcher completed"
        );
        Ok(())
    }

    async fn dispatch_entity(
        &self,
        dispatch_id: &str,
        entity: &DispatchableEntity,
        scope: &IndexingScope,
        watermark: DateTime<Utc>,
    ) -> Result<(u64, u64), TaskError> {
        let Some(ref partition) = entity.partition else {
            return self
                .publish_single(dispatch_id, entity, scope, watermark)
                .await;
        };
        let key = EntityCheckpointKey::new(scope, &entity.name);
        let checkpoints = self
            .checkpoint_store
            .load_by_prefix(key.prefix())
            .await
            .map_err(|err| {
                self.metrics.record_error(self.name(), "checkpoint");
                TaskError::new(err)
            })?;

        match Self::plan_entity_dispatch(&checkpoints, &key, partition.count) {
            DispatchDecision::Single => {
                self.publish_single(dispatch_id, entity, scope, watermark)
                    .await
            }
            DispatchDecision::AllPartitions => {
                self.publish_partitions(
                    dispatch_id,
                    entity,
                    scope,
                    watermark,
                    partition,
                    &(0..partition.count).collect::<Vec<_>>(),
                )
                .await
            }
            DispatchDecision::PendingPartitions(pending) => {
                self.publish_partitions(dispatch_id, entity, scope, watermark, partition, &pending)
                    .await
            }
        }
    }

    fn plan_entity_dispatch(
        checkpoints: &[(String, Checkpoint)],
        key: &EntityCheckpointKey,
        partition_count: u32,
    ) -> DispatchDecision {
        let all_completed = || checkpoints.iter().all(|(_, cp)| cp.cursor_values.is_none());

        match checkpoints.len() {
            0 => DispatchDecision::AllPartitions,
            1 => DispatchDecision::Single,
            n if all_completed() && n as u32 == partition_count => DispatchDecision::Single,
            _ => {
                let pending: Vec<u32> = (0..partition_count)
                    .filter(|idx| {
                        let partition_key = key.partition_key(*idx, partition_count);
                        match checkpoints.iter().find(|(k, _)| k == &partition_key) {
                            Some((_, cp)) => cp.cursor_values.is_some(),
                            None => true,
                        }
                    })
                    .collect();
                DispatchDecision::PendingPartitions(pending)
            }
        }
    }

    async fn publish_single(
        &self,
        dispatch_id: &str,
        entity: &DispatchableEntity,
        scope: &IndexingScope,
        watermark: DateTime<Utc>,
    ) -> Result<(u64, u64), TaskError> {
        let request = EntityIndexingRequest {
            dispatch_id: dispatch_id.to_owned(),
            entity_kind: entity.name.clone(),
            watermark,
            scope: scope.clone(),
            partition: None,
        };
        self.publish_request(&request).await
    }

    async fn publish_partitions(
        &self,
        dispatch_id: &str,
        entity: &DispatchableEntity,
        scope: &IndexingScope,
        watermark: DateTime<Utc>,
        partition: &PartitionConfig,
        partition_indices: &[u32],
    ) -> Result<(u64, u64), TaskError> {
        let query_start = Instant::now();
        let boundaries = self
            .partitioner
            .compute_boundaries(
                &partition.source_table,
                &partition.column,
                partition.count,
                scope,
            )
            .await
            .inspect_err(|_| {
                self.metrics
                    .record_error(self.name(), "partition_boundaries");
            })?;
        self.metrics.record_query_duration(
            &format!("entity_quantiles.{}", entity.name),
            query_start.elapsed().as_secs_f64(),
        );

        let mut dispatched = 0u64;
        let mut skipped = 0u64;

        for &idx in partition_indices {
            let Some(bounds) = boundaries.get(idx as usize) else {
                warn!(
                    entity = entity.name,
                    partition_index = idx,
                    total = partition.count,
                    "partition index out of bounds, skipping"
                );
                continue;
            };

            let request = EntityIndexingRequest {
                dispatch_id: dispatch_id.to_owned(),
                entity_kind: entity.name.clone(),
                watermark,
                scope: scope.clone(),
                partition: Some(PartitionAssignment {
                    index: idx,
                    total: partition.count,
                    column: partition.column.clone(),
                    bounds: bounds.clone(),
                }),
            };

            let (d, s) = self.publish_request(&request).await?;
            dispatched += d;
            skipped += s;
        }

        Ok((dispatched, skipped))
    }

    async fn publish_request(
        &self,
        request: &EntityIndexingRequest,
    ) -> Result<(u64, u64), TaskError> {
        let subscription = Subscription::new(INDEXER_STREAM, request.publish_subject());
        let envelope = Envelope::new(request).map_err(|error| {
            self.metrics.record_error(self.name(), "publish");
            TaskError::new(error)
        })?;

        match self.nats.publish(&subscription, &envelope).await {
            Ok(()) => {
                debug!(
                    entity = request.entity_kind,
                    partition = ?request.partition.as_ref().map(|p| p.index),
                    "dispatched entity indexing request"
                );
                Ok((1, 0))
            }
            Err(crate::nats::NatsError::PublishDuplicate) => {
                debug!(
                    entity = request.entity_kind,
                    partition = ?request.partition.as_ref().map(|p| p.index),
                    "skipped entity indexing request, already in-flight"
                );
                Ok((0, 1))
            }
            Err(error) => {
                self.metrics.record_error(self.name(), "publish");
                Err(TaskError::new(error))
            }
        }
    }

    async fn load_enabled_namespaces(&self) -> Result<Vec<(i64, String)>, TaskError> {
        let query_start = Instant::now();
        let batches = self
            .datalake
            .query(ENABLED_NAMESPACE_QUERY)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "query");
                TaskError::new(error)
            })?;
        self.metrics.record_query_duration(
            "entity_enabled_namespaces",
            query_start.elapsed().as_secs_f64(),
        );

        let namespace_ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        let traversal_paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;

        let namespaces: Vec<_> = namespace_ids.into_iter().zip(traversal_paths).collect();

        debug!(
            count = namespaces.len(),
            "loaded enabled namespaces for entity dispatch"
        );
        Ok(namespaces)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::sdlc::test_helpers::{MockCheckpointStore, MockPartitioner};
    use crate::testkit::MockNatsServices;
    use crate::topic::PartitionBounds;

    fn completed_checkpoint(watermark: &str) -> Checkpoint {
        Checkpoint {
            watermark: watermark.parse().unwrap(),
            cursor_values: None,
        }
    }

    fn in_progress_checkpoint(watermark: &str, cursor: Vec<&str>) -> Checkpoint {
        Checkpoint {
            watermark: watermark.parse().unwrap(),
            cursor_values: Some(cursor.into_iter().map(String::from).collect()),
        }
    }

    fn ns_key(namespace_id: i64, entity_kind: &str) -> EntityCheckpointKey {
        let scope = IndexingScope::Namespace {
            namespace_id,
            traversal_path: format!("42/{namespace_id}/"),
        };
        EntityCheckpointKey::new(&scope, entity_kind)
    }

    fn four_boundaries() -> Vec<PartitionBounds> {
        vec![
            PartitionBounds::Range {
                lower_bound: "1".into(),
                upper_bound: "25".into(),
            },
            PartitionBounds::Range {
                lower_bound: "25".into(),
                upper_bound: "50".into(),
            },
            PartitionBounds::Range {
                lower_bound: "50".into(),
                upper_bound: "75".into(),
            },
            PartitionBounds::Range {
                lower_bound: "75".into(),
                upper_bound: "100".into(),
            },
        ]
    }

    fn dummy_datalake() -> ArrowClickHouseClient {
        ArrowClickHouseClient::new(
            "http://localhost:0",
            "test",
            "default",
            None,
            &HashMap::new(),
        )
    }

    fn build_dispatcher(
        nats: Arc<MockNatsServices>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        partitioner: Arc<dyn Partitioner>,
        partition_overrides: HashMap<String, u32>,
    ) -> EntityDispatcher {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let config = EntityDispatcherConfig {
            partition_overrides,
            ..Default::default()
        };
        EntityDispatcher::new(
            nats,
            dummy_datalake(),
            checkpoint_store,
            partitioner,
            &ontology,
            ScheduledTaskMetrics::new(),
            config,
        )
    }

    fn published_entity_requests(nats: &MockNatsServices) -> Vec<EntityIndexingRequest> {
        nats.get_published()
            .into_iter()
            .map(|(_, envelope)| serde_json::from_slice(&envelope.payload).unwrap())
            .collect()
    }

    #[test]
    fn single_checkpoint_dispatches_single() {
        let key = ns_key(100, "MergeRequest");
        let checkpoints = vec![(
            key.prefix().to_owned(),
            completed_checkpoint("2024-01-01T00:00:00Z"),
        )];

        assert_eq!(
            EntityDispatcher::plan_entity_dispatch(&checkpoints, &key, 4),
            DispatchDecision::Single
        );
    }

    #[test]
    fn all_partitions_completed_dispatches_single() {
        let key = ns_key(100, "MR");
        let checkpoints: Vec<_> = (0..4)
            .map(|i| {
                (
                    key.partition_key(i, 4),
                    completed_checkpoint("2024-01-01T00:00:00Z"),
                )
            })
            .collect();

        assert_eq!(
            EntityDispatcher::plan_entity_dispatch(&checkpoints, &key, 4),
            DispatchDecision::Single
        );
    }

    #[test]
    fn some_partitions_incomplete_dispatches_pending() {
        let key = ns_key(100, "MR");
        let checkpoints = vec![
            (
                key.partition_key(0, 4),
                completed_checkpoint("2024-01-01T00:00:00Z"),
            ),
            (
                key.partition_key(1, 4),
                in_progress_checkpoint("2024-01-01T00:00:00Z", vec!["42"]),
            ),
            (
                key.partition_key(2, 4),
                completed_checkpoint("2024-01-01T00:00:00Z"),
            ),
        ];

        assert_eq!(
            EntityDispatcher::plan_entity_dispatch(&checkpoints, &key, 4),
            DispatchDecision::PendingPartitions(vec![1, 3])
        );
    }

    #[test]
    fn no_checkpoints_dispatches_all_partitions() {
        let key = ns_key(100, "MR");
        let checkpoints: Vec<(String, Checkpoint)> = vec![];

        assert_eq!(
            EntityDispatcher::plan_entity_dispatch(&checkpoints, &key, 4),
            DispatchDecision::AllPartitions
        );
    }

    #[test]
    fn completed_count_mismatch_dispatches_pending() {
        let key = ns_key(100, "MR");
        let old_checkpoints = vec![
            (
                key.partition_key(0, 2),
                completed_checkpoint("2024-01-01T00:00:00Z"),
            ),
            (
                key.partition_key(1, 2),
                completed_checkpoint("2024-01-01T00:00:00Z"),
            ),
        ];

        // Partition count changed from 2 to 4 — old keys don't match new format
        assert_eq!(
            EntityDispatcher::plan_entity_dispatch(&old_checkpoints, &key, 4),
            DispatchDecision::PendingPartitions(vec![0, 1, 2, 3])
        );
    }

    #[test]
    fn collects_entities_from_embedded_ontology() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let no_overrides = HashMap::new();
        let entities: Vec<_> = ontology
            .nodes()
            .filter_map(|n| DispatchableEntity::from_node(n, &no_overrides))
            .collect();

        let global: Vec<_> = entities
            .iter()
            .filter(|e| e.scope == EtlScope::Global)
            .map(|e| e.name.as_str())
            .collect();
        assert!(
            global.contains(&"User"),
            "should include global User entity"
        );

        let namespaced: Vec<_> = entities
            .iter()
            .filter(|e| e.scope == EtlScope::Namespaced)
            .map(|e| e.name.as_str())
            .collect();
        assert!(
            namespaced.contains(&"MergeRequest"),
            "should include MergeRequest"
        );
    }

    #[tokio::test]
    async fn dispatches_single_message_for_non_partitioned_entity() {
        let nats = Arc::new(MockNatsServices::new());
        let dispatcher = build_dispatcher(
            nats.clone(),
            Arc::new(MockCheckpointStore::new()),
            Arc::new(MockPartitioner::new(vec![])),
            HashMap::new(),
        );

        let entity = DispatchableEntity {
            name: "User".into(),
            scope: EtlScope::Global,
            partition: None,
        };

        let (dispatched, skipped) = dispatcher
            .dispatch_entity("test-dispatch", &entity, &IndexingScope::Global, Utc::now())
            .await
            .unwrap();

        assert_eq!((dispatched, skipped), (1, 0));
        let requests = published_entity_requests(&nats);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].entity_kind, "User");
        assert!(requests[0].partition.is_none());
    }

    #[tokio::test]
    async fn dispatches_all_partitions_when_no_checkpoints_exist() {
        let nats = Arc::new(MockNatsServices::new());
        let dispatcher = build_dispatcher(
            nats.clone(),
            Arc::new(MockCheckpointStore::new()),
            Arc::new(MockPartitioner::new(four_boundaries())),
            HashMap::from([("Runner".into(), 4)]),
        );

        let entity = DispatchableEntity {
            name: "Runner".into(),
            scope: EtlScope::Global,
            partition: Some(PartitionConfig {
                count: 4,
                source_table: "siphon_ci_runners".into(),
                column: "id".into(),
            }),
        };

        let (dispatched, _) = dispatcher
            .dispatch_entity("test-dispatch", &entity, &IndexingScope::Global, Utc::now())
            .await
            .unwrap();

        assert_eq!(dispatched, 4);
        let requests = published_entity_requests(&nats);
        let indices: Vec<u32> = requests
            .iter()
            .map(|r| r.partition.as_ref().unwrap().index)
            .collect();
        assert_eq!(indices, vec![0, 1, 2, 3]);
    }

    #[tokio::test]
    async fn dispatches_single_after_unified_checkpoint() {
        let key = EntityCheckpointKey::new(&IndexingScope::Global, "Runner");
        let checkpoint_store = MockCheckpointStore::with_checkpoints(vec![(
            key.prefix().to_owned(),
            completed_checkpoint("2024-01-01T00:00:00Z"),
        )]);

        let nats = Arc::new(MockNatsServices::new());
        let dispatcher = build_dispatcher(
            nats.clone(),
            Arc::new(checkpoint_store),
            Arc::new(MockPartitioner::new(four_boundaries())),
            HashMap::from([("Runner".into(), 4)]),
        );

        let entity = DispatchableEntity {
            name: "Runner".into(),
            scope: EtlScope::Global,
            partition: Some(PartitionConfig {
                count: 4,
                source_table: "siphon_ci_runners".into(),
                column: "id".into(),
            }),
        };

        let (dispatched, _) = dispatcher
            .dispatch_entity("test-dispatch", &entity, &IndexingScope::Global, Utc::now())
            .await
            .unwrap();

        assert_eq!(dispatched, 1);
        let requests = published_entity_requests(&nats);
        assert_eq!(requests.len(), 1);
        assert!(requests[0].partition.is_none());
    }

    #[tokio::test]
    async fn dispatches_only_pending_partitions() {
        let key = EntityCheckpointKey::new(&IndexingScope::Global, "Runner");
        let checkpoint_store = MockCheckpointStore::with_checkpoints(vec![
            (
                key.partition_key(0, 4),
                completed_checkpoint("2024-01-01T00:00:00Z"),
            ),
            (
                key.partition_key(1, 4),
                in_progress_checkpoint("2024-01-01T00:00:00Z", vec!["42"]),
            ),
            (
                key.partition_key(2, 4),
                completed_checkpoint("2024-01-01T00:00:00Z"),
            ),
        ]);

        let nats = Arc::new(MockNatsServices::new());
        let dispatcher = build_dispatcher(
            nats.clone(),
            Arc::new(checkpoint_store),
            Arc::new(MockPartitioner::new(four_boundaries())),
            HashMap::from([("Runner".into(), 4)]),
        );

        let entity = DispatchableEntity {
            name: "Runner".into(),
            scope: EtlScope::Global,
            partition: Some(PartitionConfig {
                count: 4,
                source_table: "siphon_ci_runners".into(),
                column: "id".into(),
            }),
        };

        let (dispatched, _) = dispatcher
            .dispatch_entity("test-dispatch", &entity, &IndexingScope::Global, Utc::now())
            .await
            .unwrap();

        assert_eq!(dispatched, 2);
        let requests = published_entity_requests(&nats);
        let indices: Vec<u32> = requests
            .iter()
            .map(|r| r.partition.as_ref().unwrap().index)
            .collect();
        assert_eq!(indices, vec![1, 3]);
    }
}
