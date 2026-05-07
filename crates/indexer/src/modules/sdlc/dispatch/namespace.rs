use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::{debug, info, warn};

use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::clickhouse::ArrowClickHouseClient;
use crate::nats::NatsServices;
use crate::scheduler::ScheduledTaskMetrics;
use crate::scheduler::{ScheduledTask, TaskError};
use crate::topic::{CursorRange, EntityIndexingRequest};
use crate::types::Envelope;
use clickhouse_client::FromArrowColumn;
use gkg_server_config::{NamespaceDispatcherConfig, ScheduleConfiguration};

use super::EntityInfo;

const ENABLED_NAMESPACE_QUERY: &str = r#"
SELECT root_namespace_id, traversal_path
FROM siphon_knowledge_graph_enabled_namespaces
WHERE _siphon_deleted = false
  AND traversal_path != ''
"#;

pub struct NamespaceDispatcher {
    entities: Vec<EntityInfo>,
    nats: Arc<dyn NatsServices>,
    datalake: ArrowClickHouseClient,
    checkpoint_store: Arc<dyn CheckpointStore>,
    metrics: ScheduledTaskMetrics,
    config: NamespaceDispatcherConfig,
}

impl NamespaceDispatcher {
    pub fn new(
        entities: Vec<EntityInfo>,
        nats: Arc<dyn NatsServices>,
        datalake: ArrowClickHouseClient,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: ScheduledTaskMetrics,
        config: NamespaceDispatcherConfig,
    ) -> Self {
        let backfill_set: HashSet<&str> = config
            .backfill_entities
            .iter()
            .map(|s| s.as_str())
            .collect();
        if !backfill_set.is_empty() {
            let available: Vec<&str> = entities
                .iter()
                .filter(|e| backfill_set.contains(e.name.as_str()))
                .map(|e| e.name.as_str())
                .collect();
            info!(
                backfill_entities = ?available,
                partitions = config.backfill_partitions,
                "namespace dispatcher backfill partitioning enabled"
            );
        }

        Self {
            entities,
            nats,
            datalake,
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

        debug!(
            enabled_namespaces = namespace_ids.len(),
            entity_count = self.entities.len(),
            "found enabled namespaces to dispatch entity indexing requests for"
        );

        let checkpoints = self.load_all_namespace_checkpoints().await?;

        let backfill_entities: HashSet<&str> = self
            .config
            .backfill_entities
            .iter()
            .map(|s| s.as_str())
            .collect();

        let watermark = Utc::now();
        let mut dispatched: u64 = 0;
        let mut skipped: u64 = 0;
        let mut backfill_dispatched: u64 = 0;
        let mut backfill_completed: u64 = 0;

        for (namespace_id, traversal_path) in namespace_ids.iter().zip(traversal_paths.iter()) {
            if !is_dispatchable_traversal_path(traversal_path) {
                warn!(
                    namespace_id = *namespace_id,
                    traversal_path = %traversal_path,
                    "skipping enabled namespace with invalid traversal_path"
                );
                continue;
            }

            for entity_info in &self.entities {
                let entity = &entity_info.name;
                let entity_key = format!("ns.{namespace_id}.{entity}");

                if checkpoints.contains_key(&entity_key) {
                    let (d, s) = self
                        .publish_direct(entity_info, *namespace_id, traversal_path, watermark)
                        .await?;
                    dispatched += d;
                    skipped += s;
                    continue;
                }

                if backfill_entities.contains(entity.as_str())
                    && let Some(source_table) = &entity_info.source_table
                {
                    let manifest_key = format!("{entity_key}.ranges");
                    if let Some(manifest) = checkpoints.get(&manifest_key) {
                        let (completed, advanced) = self
                            .check_backfill_completion(
                                &entity_key,
                                manifest,
                                &checkpoints,
                                watermark,
                            )
                            .await?;
                        if advanced {
                            backfill_completed += 1;
                            let (d, s) = self
                                .publish_direct(
                                    entity_info,
                                    *namespace_id,
                                    traversal_path,
                                    watermark,
                                )
                                .await?;
                            dispatched += d;
                            skipped += s;
                        } else {
                            debug!(
                                entity = %entity,
                                namespace_id = *namespace_id,
                                completed_ranges = completed,
                                "backfill in progress, skipping"
                            );
                        }
                        continue;
                    }

                    let range_count = self
                        .dispatch_backfill(
                            entity_info,
                            source_table,
                            *namespace_id,
                            traversal_path,
                            watermark,
                        )
                        .await?;
                    backfill_dispatched += range_count as u64;
                    continue;
                }

                let (d, s) = self
                    .publish_direct(entity_info, *namespace_id, traversal_path, watermark)
                    .await?;
                dispatched += d;
                skipped += s;
            }
        }

        self.metrics
            .record_requests_published(self.name(), dispatched + backfill_dispatched);
        self.metrics.record_requests_skipped(self.name(), skipped);

        info!(
            dispatched,
            backfill_dispatched,
            backfill_completed,
            skipped,
            "dispatched namespace entity indexing requests"
        );
        Ok(())
    }

    async fn load_all_namespace_checkpoints(
        &self,
    ) -> Result<HashMap<String, Checkpoint>, TaskError> {
        let query_start = Instant::now();
        let entries = self
            .checkpoint_store
            .load_by_prefix("ns.")
            .await
            .map_err(TaskError::new)?;
        self.metrics
            .record_query_duration("namespace_checkpoints", query_start.elapsed().as_secs_f64());
        debug!(
            checkpoint_count = entries.len(),
            "loaded namespace checkpoints"
        );
        Ok(entries.into_iter().collect())
    }

    async fn publish_direct(
        &self,
        entity_info: &EntityInfo,
        namespace_id: i64,
        traversal_path: &str,
        watermark: DateTime<Utc>,
    ) -> Result<(u64, u64), TaskError> {
        let request = EntityIndexingRequest {
            entity: entity_info.name.clone(),
            namespace: Some(namespace_id),
            traversal_path: Some(traversal_path.to_string()),
            range: None,
            watermark,
            range_index: None,
            range_count: None,
        };
        self.publish_request(&request).await
    }

    async fn dispatch_backfill(
        &self,
        entity_info: &EntityInfo,
        source_table: &str,
        namespace_id: i64,
        traversal_path: &str,
        watermark: DateTime<Utc>,
    ) -> Result<u32, TaskError> {
        let partitions = self.config.backfill_partitions;
        let boundaries = self
            .compute_range_boundaries(source_table, traversal_path, partitions)
            .await?;

        let range_count = boundaries.len() + 1;
        info!(
            entity = %entity_info.name,
            namespace_id,
            range_count,
            "dispatching backfill with partitioned ranges"
        );

        for range_index in 0..range_count {
            let start = if range_index > 0 {
                Some(boundaries[range_index - 1].clone())
            } else {
                None
            };
            let end = boundaries.get(range_index).cloned();

            let request = EntityIndexingRequest {
                entity: entity_info.name.clone(),
                namespace: Some(namespace_id),
                traversal_path: Some(traversal_path.to_string()),
                range: Some(CursorRange {
                    partition_column: "id".to_string(),
                    start,
                    end,
                }),
                watermark,
                range_index: Some(range_index as u32),
                range_count: Some(range_count as u32),
            };

            self.publish_request(&request).await?;
        }

        let entity_key = format!("ns.{namespace_id}.{}", entity_info.name);
        let manifest_key = format!("{entity_key}.ranges");
        self.checkpoint_store
            .save_completed(&manifest_key, &watermark)
            .await
            .map_err(TaskError::new)?;

        Ok(range_count as u32)
    }

    async fn compute_range_boundaries(
        &self,
        source_table: &str,
        traversal_path: &str,
        partitions: u32,
    ) -> Result<Vec<String>, TaskError> {
        if partitions <= 1 {
            return Ok(vec![]);
        }

        let quantiles: Vec<String> = (1..partitions)
            .map(|i| {
                let q = i as f64 / partitions as f64;
                format!("toString(quantile({q})(id))")
            })
            .collect();

        let quantile_select = quantiles.join(", ");
        let sql = format!(
            "SELECT {quantile_select} FROM {source_table} \
             WHERE startsWith(traversal_path, {{tp:String}})"
        );

        let batches = self
            .datalake
            .query(&sql)
            .param("tp", traversal_path)
            .fetch_arrow()
            .await
            .map_err(|error| {
                self.metrics.record_error(self.name(), "quantile_query");
                TaskError::new(error)
            })?;

        let batch = match batches.into_iter().next() {
            Some(b) if b.num_rows() > 0 => b,
            _ => return Ok(vec![]),
        };

        let mut boundaries = Vec::new();
        for col_idx in 0..batch.num_columns() {
            if let Some(value) =
                gkg_utils::arrow::ArrowUtils::array_value_to_string(batch.column(col_idx), 0)
                    .filter(|v| !v.is_empty())
            {
                boundaries.push(value);
            }
        }

        boundaries.dedup();
        Ok(boundaries)
    }

    async fn check_backfill_completion(
        &self,
        entity_key: &str,
        manifest: &Checkpoint,
        checkpoints: &HashMap<String, Checkpoint>,
        _watermark: DateTime<Utc>,
    ) -> Result<(u32, bool), TaskError> {
        let range_prefix = format!("{entity_key}.r.");
        let range_checkpoints: Vec<(&String, &Checkpoint)> = checkpoints
            .iter()
            .filter(|(k, _)| k.starts_with(&range_prefix))
            .collect();

        let completed_count = range_checkpoints
            .iter()
            .filter(|(_, cp)| cp.cursor_values.is_none())
            .count() as u32;

        let total_ranges: u32 = range_checkpoints.len() as u32;

        if total_ranges == 0 || completed_count < total_ranges {
            return Ok((completed_count, false));
        }

        info!(
            entity_key,
            completed_count, "all backfill ranges completed, advancing entity checkpoint"
        );

        self.checkpoint_store
            .save_completed(entity_key, &manifest.watermark)
            .await
            .map_err(TaskError::new)?;

        for (key, _) in &range_checkpoints {
            self.checkpoint_store
                .delete(key)
                .await
                .map_err(TaskError::new)?;
        }

        let manifest_key = format!("{entity_key}.ranges");
        self.checkpoint_store
            .delete(&manifest_key)
            .await
            .map_err(TaskError::new)?;

        Ok((completed_count, true))
    }

    async fn publish_request(
        &self,
        request: &EntityIndexingRequest,
    ) -> Result<(u64, u64), TaskError> {
        let subscription = request.publish_subscription();
        let envelope = Envelope::new(request).map_err(|error| {
            self.metrics.record_error(self.name(), "publish");
            TaskError::new(error)
        })?;

        match self.nats.publish(&subscription, &envelope).await {
            Ok(()) => {
                debug!(
                    entity = %request.entity,
                    namespace_id = ?request.namespace,
                    range_index = ?request.range_index,
                    "dispatched entity indexing request"
                );
                Ok((1, 0))
            }
            Err(crate::nats::NatsError::PublishDuplicate) => {
                debug!(
                    entity = %request.entity,
                    namespace_id = ?request.namespace,
                    range_index = ?request.range_index,
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
}

fn is_dispatchable_traversal_path(path: &str) -> bool {
    gkg_utils::traversal_path::is_valid(path)
}

#[cfg(test)]
mod tests {
    use super::{ENABLED_NAMESPACE_QUERY, is_dispatchable_traversal_path};

    #[test]
    fn enabled_namespace_query_excludes_empty_traversal_paths() {
        assert!(ENABLED_NAMESPACE_QUERY.contains("traversal_path != ''"));
    }

    #[test]
    fn dispatchable_traversal_paths_require_org_and_namespace_segments() {
        assert!(is_dispatchable_traversal_path("1/9/"));
        assert!(!is_dispatchable_traversal_path(""));
        assert!(!is_dispatchable_traversal_path("0/"));
        assert!(!is_dispatchable_traversal_path("1/"));
    }
}
