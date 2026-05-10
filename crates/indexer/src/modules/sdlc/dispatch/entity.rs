use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse_client::FromArrowColumn;
use ontology::EtlScope;
use tracing::{debug, info};

use crate::checkpoint::{Checkpoint, CheckpointStore, EntityCheckpointKey};
use crate::clickhouse::ArrowClickHouseClient;
use crate::nats::NatsServices;
use crate::scheduler::ScheduledTaskMetrics;
use crate::scheduler::{ScheduledTask, TaskError};
use crate::topic::{EntityIndexingRequest, IndexingScope, PartitionAssignment};
use crate::types::Envelope;
use gkg_server_config::{EntityDispatcherConfig, ScheduleConfiguration};

use crate::modules::sdlc::partition_strategy::PartitionStrategy;

const ENABLED_NAMESPACE_QUERY: &str = r#"
SELECT root_namespace_id, traversal_path
FROM siphon_knowledge_graph_enabled_namespaces
WHERE _siphon_deleted = false
  AND traversal_path != ''
  AND traversal_path != '0/'
"#;

pub struct PartitionConfig {
    pub count: u32,
    pub column: String,
    pub source_table: String,
}

pub struct EntityDescriptor {
    pub entity_kind: String,
    pub scope: EtlScope,
    pub partition: Option<PartitionConfig>,
}

pub struct EntityDispatcher {
    nats: Arc<dyn NatsServices>,
    datalake: ArrowClickHouseClient,
    checkpoint_store: Arc<dyn CheckpointStore>,
    partition_strategies: HashMap<String, Arc<dyn PartitionStrategy>>,
    metrics: ScheduledTaskMetrics,
    config: EntityDispatcherConfig,
    entities: Vec<EntityDescriptor>,
}

impl EntityDispatcher {
    pub fn new(
        nats: Arc<dyn NatsServices>,
        datalake: ArrowClickHouseClient,
        checkpoint_store: Arc<dyn CheckpointStore>,
        partition_strategies: HashMap<String, Arc<dyn PartitionStrategy>>,
        metrics: ScheduledTaskMetrics,
        config: EntityDispatcherConfig,
        entities: Vec<EntityDescriptor>,
    ) -> Self {
        Self {
            nats,
            datalake,
            checkpoint_store,
            partition_strategies,
            metrics,
            config,
            entities,
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

enum PublishOutcome {
    Published,
    Skipped,
}

impl EntityDispatcher {
    async fn dispatch_inner(&self) -> Result<(), TaskError> {
        let namespaces = self.load_enabled_namespaces().await?;

        let watermark = Utc::now();
        let mut dispatched: u64 = 0;
        let mut skipped: u64 = 0;

        for entity in &self.entities {
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
                let requests = self
                    .plan_dispatch(entity, &scope, watermark)
                    .await
                    .map_err(TaskError::new)?;

                for request in &requests {
                    match self.publish_request(request).await? {
                        PublishOutcome::Published => dispatched += 1,
                        PublishOutcome::Skipped => skipped += 1,
                    }
                }
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

    async fn plan_dispatch(
        &self,
        entity: &EntityDescriptor,
        scope: &IndexingScope,
        watermark: DateTime<Utc>,
    ) -> Result<Vec<EntityIndexingRequest>, crate::handler::HandlerError> {
        let strategy = match self.partition_strategies.get(&entity.entity_kind) {
            Some(s) => s,
            None => {
                return Ok(vec![EntityIndexingRequest {
                    entity_kind: entity.entity_kind.clone(),
                    watermark,
                    scope: scope.clone(),
                    partition: None,
                }]);
            }
        };

        let checkpoint_key = EntityCheckpointKey::new(scope);
        let checkpoints = self
            .checkpoint_store
            .load_by_prefix(checkpoint_key.prefix())
            .await
            .map_err(|err| {
                crate::handler::HandlerError::Processing(format!(
                    "failed to load checkpoints: {err}"
                ))
            })?;

        let unified_key = checkpoint_key.full_key(&entity.entity_kind);

        if checkpoints
            .get(&unified_key)
            .is_some_and(|cp| cp.is_completed())
        {
            return Ok(vec![EntityIndexingRequest {
                entity_kind: entity.entity_kind.clone(),
                watermark,
                scope: scope.clone(),
                partition: None,
            }]);
        }

        let partition_checkpoints: Vec<(&String, &Checkpoint)> = checkpoints
            .iter()
            .filter(|(key, _)| key.as_str() != unified_key && key.starts_with(&unified_key))
            .collect();

        if !partition_checkpoints.is_empty()
            && partition_checkpoints
                .iter()
                .all(|(_, cp)| cp.is_completed())
        {
            let min_watermark = partition_checkpoints
                .iter()
                .map(|(_, cp)| cp.watermark)
                .min()
                .unwrap_or(watermark);

            self.checkpoint_store
                .save_completed(&unified_key, &min_watermark)
                .await
                .map_err(|err| {
                    crate::handler::HandlerError::Processing(format!(
                        "failed to save consolidated checkpoint: {err}"
                    ))
                })?;

            info!(
                entity_kind = %entity.entity_kind,
                consolidated_watermark = %min_watermark,
                "consolidated partition checkpoints"
            );

            return Ok(vec![EntityIndexingRequest {
                entity_kind: entity.entity_kind.clone(),
                watermark,
                scope: scope.clone(),
                partition: None,
            }]);
        }

        let dummy_request = EntityIndexingRequest {
            entity_kind: entity.entity_kind.clone(),
            watermark,
            scope: scope.clone(),
            partition: None,
        };

        let specs = match strategy.compute_partitions(&dummy_request).await? {
            Some(specs) => specs,
            None => {
                info!(
                    entity_kind = %entity.entity_kind,
                    partition_count = strategy.partition_count(),
                    "insufficient quantiles, dispatching non-partitioned"
                );
                return Ok(vec![dummy_request]);
            }
        };

        let column = strategy.partition_column().to_string();
        let pending: Vec<EntityIndexingRequest> = specs
            .into_iter()
            .filter(|spec| {
                let key = EntityCheckpointKey::new(scope)
                    .with_partition(spec.partition_index, spec.total_partitions)
                    .full_key(&entity.entity_kind);
                !matches!(checkpoints.get(&key), Some(cp) if cp.is_completed())
            })
            .map(|spec| EntityIndexingRequest {
                entity_kind: entity.entity_kind.clone(),
                watermark,
                scope: scope.clone(),
                partition: Some(PartitionAssignment {
                    index: spec.partition_index,
                    total: spec.total_partitions,
                    column: column.clone(),
                    bounds: spec.bounds,
                }),
            })
            .collect();

        info!(
            entity_kind = %entity.entity_kind,
            pending = pending.len(),
            total = strategy.partition_count(),
            "dispatching partition jobs"
        );

        Ok(pending)
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
            .map(|(namespace_id, traversal_path)| EnabledNamespace {
                namespace_id,
                traversal_path,
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
                    partition = ?request.partition.as_ref().map(|p| p.index),
                    "dispatched entity indexing request"
                );
                Ok(PublishOutcome::Published)
            }
            Err(crate::nats::NatsError::PublishDuplicate) => {
                debug!(
                    entity_kind = %request.entity_kind,
                    scope = ?request.scope,
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
