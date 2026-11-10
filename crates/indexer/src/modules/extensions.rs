//! The extensions scope dispatcher (E6).
//!
//! A provider owns a scopes table: one row per scope, with its `traversal_path`
//! and the ETL columns. Each tick this task reads that table, and for every live
//! scope compares the newest `_siphon_watermark` across the scope's declared
//! source tables against the scope's dispatch checkpoint. A scope whose source
//! data advanced gets one indexing request; a scope the provider marked deleted
//! gets the existing namespace-deletion request. The decision is pure
//! ([`plan_scope_dispatch`]); the task wraps it with ClickHouse reads and NATS.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use orbit_log_fields::info;
use orbit_server_config::ScheduleConfiguration;
use orbit_utils::traversal_path::TraversalPath;
use uuid::Uuid;

use crate::checkpoint::CheckpointStore;
use crate::durability::WriteDurability;
use crate::nats::NatsServices;
use crate::orchestrator::scheduled::{ScheduledTask, TaskError};
use crate::topic::{NamespaceDeletionRequest, NamespaceIndexingRequest};
use crate::types::Envelope;

/// The dispatch checkpoint of a scope lives under this prefix, one key per scope
/// id. Distinct from the `ns.` and `global` entity-pipeline checkpoints.
const SCOPE_CHECKPOINT_PREFIX: &str = "dispatch.extensions.scope";

/// A scope as the provider's scopes table presents it, already reduced to its
/// latest replicated row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScopeState {
    pub id: i64,
    pub traversal_path: String,
    pub deleted: bool,
    /// Newest `_siphon_watermark` across the scope's declared source tables.
    /// `None` when the scope has no source rows yet.
    pub source_watermark: Option<DateTime<Utc>>,
}

/// What the dispatcher should publish this tick.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct ScopeDispatch {
    pub to_index: Vec<(i64, String, DateTime<Utc>)>,
    pub to_delete: Vec<(i64, String)>,
}

/// Decides, from the scope states and the per-scope dispatch checkpoints, which
/// scopes to re-index and which to delete. A deleted scope is always deleted,
/// even if its source data also changed. A live scope is re-indexed only when
/// its newest source watermark is past the watermark last dispatched for it.
pub(crate) fn plan_scope_dispatch(
    scopes: &[ScopeState],
    checkpoints: &HashMap<i64, DateTime<Utc>>,
) -> ScopeDispatch {
    let mut dispatch = ScopeDispatch::default();
    for scope in scopes {
        if scope.deleted {
            dispatch
                .to_delete
                .push((scope.id, scope.traversal_path.clone()));
            continue;
        }
        let Some(watermark) = scope.source_watermark else {
            continue;
        };
        let advanced = checkpoints
            .get(&scope.id)
            .is_none_or(|prior| watermark > *prior);
        if advanced {
            dispatch
                .to_index
                .push((scope.id, scope.traversal_path.clone(), watermark));
        }
    }
    dispatch
}

pub(crate) struct ExtensionScopeDispatcher {
    datalake: ArrowClickHouseClient,
    nats: Arc<dyn NatsServices>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    scopes_table: String,
    source_tables: Vec<String>,
    schedule: ScheduleConfiguration,
}

impl ExtensionScopeDispatcher {
    pub(crate) fn new(
        datalake: ArrowClickHouseClient,
        nats: Arc<dyn NatsServices>,
        checkpoint_store: Arc<dyn CheckpointStore>,
        scopes_table: String,
        source_tables: Vec<String>,
        schedule: ScheduleConfiguration,
    ) -> Self {
        Self {
            datalake,
            nats,
            checkpoint_store,
            scopes_table,
            source_tables,
            schedule,
        }
    }

    async fn read_scopes(&self) -> Result<Vec<ScopeState>, TaskError> {
        let sql = format!(
            "SELECT id, traversal_path, _siphon_deleted FROM {} FINAL",
            self.scopes_table
        );
        let batches = self
            .datalake
            .query(&sql)
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;
        let ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        let paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;
        let deleted = bool::extract_column(&batches, 2).map_err(TaskError::new)?;

        let mut scopes = Vec::with_capacity(ids.len());
        for ((id, path), deleted) in ids.into_iter().zip(paths).zip(deleted) {
            let source_watermark = self.newest_source_watermark(&path).await?;
            scopes.push(ScopeState {
                id,
                traversal_path: path,
                deleted,
                source_watermark,
            });
        }
        Ok(scopes)
    }

    /// The newest `_siphon_watermark` of any declared source table row under the
    /// scope's path. The source tables carry the data contract's ETL columns.
    async fn newest_source_watermark(
        &self,
        traversal_path: &str,
    ) -> Result<Option<DateTime<Utc>>, TaskError> {
        let mut newest: Option<DateTime<Utc>> = None;
        for table in &self.source_tables {
            let sql = format!(
                "SELECT max(_siphon_watermark) FROM {table} WHERE startsWith(traversal_path, {{path:String}})"
            );
            let batches = self
                .datalake
                .query(&sql)
                .param("path", traversal_path)
                .fetch_arrow()
                .await
                .map_err(TaskError::new)?;
            let column =
                Option::<DateTime<Utc>>::extract_column(&batches, 0).map_err(TaskError::new)?;
            if let Some(Some(watermark)) = column.into_iter().next() {
                newest = Some(newest.map_or(watermark, |current| current.max(watermark)));
            }
        }
        Ok(newest)
    }

    async fn load_checkpoints(&self) -> Result<HashMap<i64, DateTime<Utc>>, TaskError> {
        let entries = self
            .checkpoint_store
            .load_by_prefix(SCOPE_CHECKPOINT_PREFIX)
            .await
            .map_err(TaskError::new)?;
        let mut map = HashMap::new();
        for (key, checkpoint) in entries {
            if let (Some(id), Some(watermark)) =
                (scope_id_from_key(&key), checkpoint.synced_until())
            {
                map.insert(id, watermark);
            }
        }
        Ok(map)
    }

    async fn publish_index(
        &self,
        id: i64,
        traversal_path: String,
        watermark: DateTime<Utc>,
    ) -> Result<(), TaskError> {
        let request = NamespaceIndexingRequest {
            namespace: id,
            traversal_path: TraversalPath::new_unchecked(traversal_path),
            watermark,
            dispatch_id: Uuid::new_v4(),
            campaign_id: None,
            targets: Vec::new(),
        };
        self.publish(&request.publish_subscription(), &request)
            .await?;

        let mut checkpoint = crate::checkpoint::Checkpoint::new(watermark);
        checkpoint.complete(watermark);
        self.checkpoint_store
            .save(
                &scope_checkpoint_key(id),
                &checkpoint,
                WriteDurability::Durable,
            )
            .await
            .map_err(TaskError::new)
    }

    async fn publish_delete(&self, id: i64, traversal_path: String) -> Result<(), TaskError> {
        let request = NamespaceDeletionRequest {
            namespace_id: id,
            traversal_path: TraversalPath::new_unchecked(traversal_path),
            dispatch_id: Uuid::new_v4(),
        };
        self.publish(&request.publish_subscription(), &request)
            .await
    }

    async fn publish<T: crate::types::Event>(
        &self,
        subscription: &crate::types::Subscription,
        request: &T,
    ) -> Result<(), TaskError> {
        let envelope = Envelope::new(request).map_err(TaskError::new)?;
        match self.nats.publish(subscription, &envelope).await {
            Ok(()) | Err(crate::nats::NatsError::PublishDuplicate) => Ok(()),
            Err(error) => Err(TaskError::new(error)),
        }
    }
}

#[async_trait::async_trait]
impl ScheduledTask for ExtensionScopeDispatcher {
    fn name(&self) -> &str {
        SCOPE_CHECKPOINT_PREFIX
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let scopes = self.read_scopes().await?;
        let checkpoints = self.load_checkpoints().await?;
        let dispatch = plan_scope_dispatch(&scopes, &checkpoints);

        for (id, path, watermark) in dispatch.to_index {
            self.publish_index(id, path, watermark).await?;
        }
        for (id, path) in dispatch.to_delete {
            self.publish_delete(id, path).await?;
        }
        info!("dispatched extension scopes");
        Ok(())
    }
}

fn scope_checkpoint_key(id: i64) -> String {
    format!("{SCOPE_CHECKPOINT_PREFIX}.{id}")
}

fn scope_id_from_key(key: &str) -> Option<i64> {
    key.strip_prefix(SCOPE_CHECKPOINT_PREFIX)
        .and_then(|rest| rest.strip_prefix('.'))
        .and_then(|id| id.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(secs, 0).expect("valid timestamp")
    }

    fn scope(id: i64, deleted: bool, watermark: Option<i64>) -> ScopeState {
        ScopeState {
            id,
            traversal_path: format!("1/{id}/"),
            deleted,
            source_watermark: watermark.map(at),
        }
    }

    #[test]
    fn indexes_a_new_scope_with_source_data() {
        let dispatch = plan_scope_dispatch(&[scope(42, false, Some(100))], &HashMap::new());
        assert_eq!(dispatch.to_index, vec![(42, "1/42/".to_string(), at(100))]);
        assert!(dispatch.to_delete.is_empty());
    }

    #[test]
    fn skips_a_scope_that_has_not_advanced() {
        let checkpoints = HashMap::from([(42, at(100))]);
        let dispatch = plan_scope_dispatch(&[scope(42, false, Some(100))], &checkpoints);
        assert!(
            dispatch.to_index.is_empty(),
            "equal watermark is not an advance"
        );
    }

    #[test]
    fn indexes_a_scope_whose_source_advanced() {
        let checkpoints = HashMap::from([(42, at(100))]);
        let dispatch = plan_scope_dispatch(&[scope(42, false, Some(150))], &checkpoints);
        assert_eq!(dispatch.to_index, vec![(42, "1/42/".to_string(), at(150))]);
    }

    #[test]
    fn deletes_a_scope_the_provider_removed() {
        let dispatch = plan_scope_dispatch(&[scope(42, true, Some(150))], &HashMap::new());
        assert_eq!(dispatch.to_delete, vec![(42, "1/42/".to_string())]);
        assert!(
            dispatch.to_index.is_empty(),
            "a deleted scope is not re-indexed"
        );
    }

    #[test]
    fn skips_a_scope_without_source_rows() {
        let dispatch = plan_scope_dispatch(&[scope(42, false, None)], &HashMap::new());
        assert!(dispatch.to_index.is_empty() && dispatch.to_delete.is_empty());
    }

    #[test]
    fn checkpoint_key_round_trips() {
        assert_eq!(
            scope_id_from_key(&scope_checkpoint_key(9_000_000_000_042)),
            Some(9_000_000_000_042)
        );
        assert_eq!(scope_id_from_key("ns.42"), None);
    }
}
