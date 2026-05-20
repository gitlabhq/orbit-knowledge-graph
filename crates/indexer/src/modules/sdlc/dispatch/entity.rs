use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse_client::FromArrowColumn;
use tracing::{debug, info, warn};

use super::partition_strategy::PartitionStrategy;
use crate::checkpoint::{Checkpoint, CheckpointStore, entity_checkpoint_prefix};
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

    fn partition_column(order_by: &[String], scope: EtlScope) -> Option<&str> {
        let skip = match scope {
            EtlScope::Namespaced => 1,
            EtlScope::Global => 0,
        };
        order_by.get(skip).map(String::as_str)
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
        let column = Self::partition_column(order_by, scope)?.to_owned();
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
    partition_strategy: Arc<dyn PartitionStrategy>,
    entities: Vec<DispatchableEntity>,
    metrics: ScheduledTaskMetrics,
    config: EntityDispatcherConfig,
}

impl EntityDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        datalake: ArrowClickHouseClient,
        checkpoint_store: Arc<dyn CheckpointStore>,
        partition_strategy: Arc<dyn PartitionStrategy>,
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
            partition_strategy,
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
        let mut dispatched: u64 = 0;
        let mut skipped: u64 = 0;

        let (namespaced_entities, global_entities): (Vec<_>, Vec<_>) = self
            .entities
            .iter()
            .partition(|e| e.scope == EtlScope::Namespaced);

        for entity in &global_entities {
            let (d, s) = self
                .dispatch_entity(entity, &IndexingScope::Global, watermark)
                .await?;
            dispatched += d;
            skipped += s;
        }

        if namespaced_entities.is_empty() {
            self.metrics
                .record_requests_published(self.name(), dispatched);
            self.metrics.record_requests_skipped(self.name(), skipped);
            info!(
                dispatched,
                skipped, "entity dispatcher completed (no namespaced entities)"
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
                let (d, s) = self.dispatch_entity(entity, &scope, watermark).await?;
                dispatched += d;
                skipped += s;
            }
        }

        self.metrics
            .record_requests_published(self.name(), dispatched);
        self.metrics.record_requests_skipped(self.name(), skipped);

        info!(
            dispatched,
            skipped,
            namespaces = namespaces.len(),
            "entity dispatcher completed"
        );
        Ok(())
    }

    async fn dispatch_entity(
        &self,
        entity: &DispatchableEntity,
        scope: &IndexingScope,
        watermark: DateTime<Utc>,
    ) -> Result<(u64, u64), TaskError> {
        let Some(ref partition) = entity.partition else {
            return self.publish_single(entity, scope, watermark).await;
        };
        let prefix = entity_checkpoint_prefix(scope, &entity.name);
        let checkpoints = self
            .checkpoint_store
            .load_by_prefix(&prefix)
            .await
            .map_err(|err| {
                self.metrics.record_error(self.name(), "checkpoint");
                TaskError::new(err)
            })?;

        match Self::plan_entity_dispatch(&checkpoints, &prefix, partition.count) {
            DispatchDecision::Single => self.publish_single(entity, scope, watermark).await,
            DispatchDecision::AllPartitions => {
                self.publish_partitions(
                    entity,
                    scope,
                    watermark,
                    partition,
                    &(0..partition.count).collect::<Vec<_>>(),
                )
                .await
            }
            DispatchDecision::PendingPartitions(pending) => {
                self.publish_partitions(entity, scope, watermark, partition, &pending)
                    .await
            }
        }
    }

    fn plan_entity_dispatch(
        checkpoints: &[(String, Checkpoint)],
        entity_prefix: &str,
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
                        let partition_key = format!("{entity_prefix}.p{idx}of{partition_count}");
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
        entity: &DispatchableEntity,
        scope: &IndexingScope,
        watermark: DateTime<Utc>,
    ) -> Result<(u64, u64), TaskError> {
        let request = EntityIndexingRequest {
            entity_kind: entity.name.clone(),
            watermark,
            scope: scope.clone(),
            partition: None,
        };
        self.publish_request(&request).await
    }

    async fn publish_partitions(
        &self,
        entity: &DispatchableEntity,
        scope: &IndexingScope,
        watermark: DateTime<Utc>,
        partition: &PartitionConfig,
        partition_indices: &[u32],
    ) -> Result<(u64, u64), TaskError> {
        let query_start = Instant::now();
        let boundaries = self
            .partition_strategy
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

    #[test]
    fn single_checkpoint_dispatches_single() {
        let checkpoints = vec![(
            "ns.100.MergeRequest".to_string(),
            completed_checkpoint("2024-01-01T00:00:00Z"),
        )];

        assert_eq!(
            EntityDispatcher::plan_entity_dispatch(&checkpoints, "ns.100.MergeRequest", 4),
            DispatchDecision::Single
        );
    }

    #[test]
    fn all_partitions_completed_dispatches_single() {
        let checkpoints = vec![
            (
                "ns.100.MR.p0of4".to_string(),
                completed_checkpoint("2024-01-01T00:00:00Z"),
            ),
            (
                "ns.100.MR.p1of4".to_string(),
                completed_checkpoint("2024-01-01T00:00:00Z"),
            ),
            (
                "ns.100.MR.p2of4".to_string(),
                completed_checkpoint("2024-01-01T00:00:00Z"),
            ),
            (
                "ns.100.MR.p3of4".to_string(),
                completed_checkpoint("2024-01-01T00:00:00Z"),
            ),
        ];

        assert_eq!(
            EntityDispatcher::plan_entity_dispatch(&checkpoints, "ns.100.MR", 4),
            DispatchDecision::Single
        );
    }

    #[test]
    fn some_partitions_incomplete_dispatches_pending() {
        let checkpoints = vec![
            (
                "ns.100.MR.p0of4".to_string(),
                completed_checkpoint("2024-01-01T00:00:00Z"),
            ),
            (
                "ns.100.MR.p1of4".to_string(),
                in_progress_checkpoint("2024-01-01T00:00:00Z", vec!["42"]),
            ),
            (
                "ns.100.MR.p2of4".to_string(),
                completed_checkpoint("2024-01-01T00:00:00Z"),
            ),
        ];

        assert_eq!(
            EntityDispatcher::plan_entity_dispatch(&checkpoints, "ns.100.MR", 4),
            DispatchDecision::PendingPartitions(vec![1, 3])
        );
    }

    #[test]
    fn no_checkpoints_dispatches_all_partitions() {
        let checkpoints: Vec<(String, Checkpoint)> = vec![];

        assert_eq!(
            EntityDispatcher::plan_entity_dispatch(&checkpoints, "ns.100.MR", 4),
            DispatchDecision::AllPartitions
        );
    }

    #[test]
    fn completed_count_mismatch_dispatches_pending() {
        let checkpoints = vec![
            (
                "ns.100.MR.p0of2".to_string(),
                completed_checkpoint("2024-01-01T00:00:00Z"),
            ),
            (
                "ns.100.MR.p1of2".to_string(),
                completed_checkpoint("2024-01-01T00:00:00Z"),
            ),
        ];

        // Partition count changed from 2 to 4 — old keys don't match new format
        assert_eq!(
            EntityDispatcher::plan_entity_dispatch(&checkpoints, "ns.100.MR", 4),
            DispatchDecision::PendingPartitions(vec![0, 1, 2, 3])
        );
    }

    #[test]
    fn checkpoint_prefix_global() {
        assert_eq!(
            entity_checkpoint_prefix(&IndexingScope::Global, "User"),
            "global.User"
        );
    }

    #[test]
    fn checkpoint_prefix_namespaced() {
        let scope = IndexingScope::Namespace {
            namespace_id: 100,
            traversal_path: "42/100/".to_string(),
        };
        assert_eq!(
            entity_checkpoint_prefix(&scope, "MergeRequest"),
            "ns.100.MergeRequest"
        );
    }

    #[test]
    fn partition_column_namespaced_skips_traversal_path() {
        let order_by = vec!["traversal_path".into(), "id".into()];
        assert_eq!(
            DispatchableEntity::partition_column(&order_by, EtlScope::Namespaced),
            Some("id")
        );
    }

    #[test]
    fn partition_column_global_uses_first() {
        let order_by = vec!["id".into()];
        assert_eq!(
            DispatchableEntity::partition_column(&order_by, EtlScope::Global),
            Some("id")
        );
    }

    #[test]
    fn partition_column_none_when_no_non_scope_columns() {
        let order_by = vec!["traversal_path".into()];
        assert_eq!(
            DispatchableEntity::partition_column(&order_by, EtlScope::Namespaced),
            None
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
}
