use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse_client::FromArrowColumn;
use tracing::{debug, info, warn};

use super::partition_strategy::{self, PartitionStrategy};
use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::clickhouse::ArrowClickHouseClient;
use crate::nats::NatsServices;
use crate::scheduler::ScheduledTaskMetrics;
use crate::scheduler::{ScheduledTask, TaskError};
use crate::topic::{EntityIndexingRequest, IndexingScope, PartitionAssignment};
use crate::types::{Envelope, Subscription};
use gkg_server_config::{EntityDispatcherConfig, ScheduleConfiguration};
use ontology::{EtlConfig, EtlScope, Ontology};

const INDEXER_STREAM: &str = crate::topic::INDEXER_STREAM;

const ENABLED_NAMESPACE_QUERY: &str = r#"
SELECT root_namespace_id, traversal_path
FROM siphon_knowledge_graph_enabled_namespaces
WHERE _siphon_deleted = false
  AND traversal_path != ''
"#;

// ── Entity metadata derived from the ontology ───────────────────────

#[derive(Debug, Clone)]
struct DispatchableEntity {
    name: String,
    scope: EtlScope,
    source_table: Option<String>,
    order_by: Vec<String>,
    deleted_column: String,
}

impl DispatchableEntity {
    fn partition_column(&self) -> Option<&str> {
        partition_strategy::partition_column(&self.order_by, self.scope)
    }
}

fn collect_dispatchable_entities(ontology: &Ontology) -> Vec<DispatchableEntity> {
    ontology
        .nodes()
        .filter_map(|node| {
            let etl = node.etl.as_ref()?;
            let source_table = match etl {
                EtlConfig::Table { source, .. } => Some(source.clone()),
                EtlConfig::Query { .. } => None,
            };
            Some(DispatchableEntity {
                name: node.name.clone(),
                scope: etl.scope(),
                source_table,
                order_by: etl.order_by().to_vec(),
                deleted_column: etl.deleted().to_string(),
            })
        })
        .collect()
}

// ── Checkpoint key helpers ──────────────────────────────────────────

fn entity_checkpoint_prefix(scope: &IndexingScope, entity_kind: &str) -> String {
    let base = match scope {
        IndexingScope::Global => "global".to_string(),
        IndexingScope::Namespace { namespace_id, .. } => format!("ns.{namespace_id}"),
    };
    format!("{base}.{entity_kind}")
}

// ── Dispatch decision (pure, testable) ──────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq)]
enum DispatchDecision {
    Single,
    AllPartitions,
    PendingPartitions(Vec<u32>),
}

fn plan_entity_dispatch(
    checkpoints: &[(String, Checkpoint)],
    entity_prefix: &str,
    partition_count: u32,
) -> DispatchDecision {
    if checkpoints.len() == 1 {
        return DispatchDecision::Single;
    }

    let all_completed = checkpoints.iter().all(|(_, cp)| cp.cursor_values.is_none());

    if checkpoints.len() > 1 && all_completed && checkpoints.len() as u32 == partition_count {
        return DispatchDecision::Single;
    }

    if checkpoints.len() > 1 {
        let pending: Vec<u32> = (0..partition_count)
            .filter(|idx| {
                let partition_key = format!("{entity_prefix}.p{idx}of{partition_count}");
                match checkpoints.iter().find(|(k, _)| k == &partition_key) {
                    Some((_, cp)) => cp.cursor_values.is_some(),
                    None => true,
                }
            })
            .collect();
        return DispatchDecision::PendingPartitions(pending);
    }

    DispatchDecision::AllPartitions
}

// ── EntityDispatcher ────────────────────────────────────────────────

pub struct EntityDispatcher {
    nats: Arc<dyn NatsServices>,
    datalake: ArrowClickHouseClient,
    checkpoint_store: Arc<dyn CheckpointStore>,
    partition_strategy: Arc<dyn PartitionStrategy>,
    entities: Vec<DispatchableEntity>,
    partition_overrides: HashMap<String, u32>,
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
        let entities = collect_dispatchable_entities(ontology);
        let partition_overrides = config.partition_overrides.clone();

        info!(
            global_entities = entities.iter().filter(|e| e.scope == EtlScope::Global).count(),
            namespaced_entities = entities.iter().filter(|e| e.scope == EtlScope::Namespaced).count(),
            partition_overrides = ?partition_overrides,
            "entity dispatcher initialized"
        );

        Self {
            nats,
            datalake,
            checkpoint_store,
            partition_strategy,
            entities,
            partition_overrides,
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

        let global_entities: Vec<_> = self
            .entities
            .iter()
            .filter(|e| e.scope == EtlScope::Global)
            .collect();

        for entity in &global_entities {
            let (d, s) = self
                .dispatch_entity(entity, &IndexingScope::Global, watermark)
                .await?;
            dispatched += d;
            skipped += s;
        }

        let namespaced_entities: Vec<_> = self
            .entities
            .iter()
            .filter(|e| e.scope == EtlScope::Namespaced)
            .collect();

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
        let partition_count = self.partition_overrides.get(&entity.name).copied();
        let is_partitioned =
            partition_count.is_some_and(|n| n > 1) && entity.partition_column().is_some();

        if !is_partitioned {
            return self.publish_single(entity, scope, watermark).await;
        }

        let partition_count = partition_count.unwrap();
        let prefix = entity_checkpoint_prefix(scope, &entity.name);
        let checkpoints = self
            .checkpoint_store
            .load_by_prefix(&prefix)
            .await
            .map_err(|err| {
                self.metrics.record_error(self.name(), "checkpoint");
                TaskError::new(err)
            })?;

        let decision = plan_entity_dispatch(&checkpoints, &prefix, partition_count);

        match decision {
            DispatchDecision::Single => self.publish_single(entity, scope, watermark).await,
            DispatchDecision::AllPartitions => {
                self.publish_partitions(
                    entity,
                    scope,
                    watermark,
                    &(0..partition_count).collect::<Vec<_>>(),
                )
                .await
            }
            DispatchDecision::PendingPartitions(pending) => {
                self.publish_partitions(entity, scope, watermark, &pending)
                    .await
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
        partition_indices: &[u32],
    ) -> Result<(u64, u64), TaskError> {
        let partition_count = self.partition_overrides[&entity.name];

        let source_table = entity.source_table.as_deref().ok_or_else(|| {
            TaskError::new(format!(
                "cannot compute partition boundaries for {} (Query-type ETL)",
                entity.name
            ))
        })?;
        let partition_column = entity.partition_column().ok_or_else(|| {
            TaskError::new(format!(
                "cannot derive partition column for {}",
                entity.name
            ))
        })?;

        let query_start = Instant::now();
        let boundaries = self
            .partition_strategy
            .compute_boundaries(
                source_table,
                partition_column,
                &entity.deleted_column,
                partition_count,
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
                    total = partition_count,
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
                    total: partition_count,
                    column: partition_column.to_string(),
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

        let namespaces: Vec<_> = namespace_ids
            .into_iter()
            .zip(traversal_paths)
            .filter(|(_, path)| gkg_utils::traversal_path::is_valid(path))
            .collect();

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

    // ── plan_entity_dispatch tests ──────────────────────────────────

    #[test]
    fn single_checkpoint_dispatches_single() {
        let checkpoints = vec![(
            "ns.100.MergeRequest".to_string(),
            completed_checkpoint("2024-01-01T00:00:00Z"),
        )];

        assert_eq!(
            plan_entity_dispatch(&checkpoints, "ns.100.MergeRequest", 4),
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
            plan_entity_dispatch(&checkpoints, "ns.100.MR", 4),
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
            plan_entity_dispatch(&checkpoints, "ns.100.MR", 4),
            DispatchDecision::PendingPartitions(vec![1, 3])
        );
    }

    #[test]
    fn no_checkpoints_dispatches_all_partitions() {
        let checkpoints: Vec<(String, Checkpoint)> = vec![];

        assert_eq!(
            plan_entity_dispatch(&checkpoints, "ns.100.MR", 4),
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
            plan_entity_dispatch(&checkpoints, "ns.100.MR", 4),
            DispatchDecision::PendingPartitions(vec![0, 1, 2, 3])
        );
    }

    // ── entity_checkpoint_prefix tests ──────────────────────────────

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

    // ── collect_dispatchable_entities test ───────────────────────────

    #[test]
    fn collects_entities_from_embedded_ontology() {
        let ontology = Ontology::load_embedded().expect("should load ontology");
        let entities = collect_dispatchable_entities(&ontology);

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

        for entity in &entities {
            assert!(
                !entity.order_by.is_empty(),
                "{} should have non-empty order_by",
                entity.name
            );
            assert!(
                !entity.deleted_column.is_empty(),
                "{} should have a deleted column",
                entity.name
            );
        }
    }
}
