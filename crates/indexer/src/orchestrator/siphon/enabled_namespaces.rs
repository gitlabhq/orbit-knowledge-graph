//! CDC route: newly-enabled namespaces trigger SDLC indexing and code backfill.

use std::sync::Arc;

use async_trait::async_trait;
use clickhouse_client::FromArrowColumn;
use siphon_proto::LogicalReplicationEvents;
use siphon_proto::replication_event::Operation;
use tracing::warn;

use crate::clickhouse::ArrowClickHouseClient;
use crate::orchestrator::dispatch::{
    CodeBackfill, NamespaceDispatchRequest, NamespaceIndexingDispatch,
    enabled_namespaces::resolved_paths_for_namespace_ids_sql,
};
use crate::orchestrator::scheduled::TaskError;
use crate::orchestrator::siphon::decoder::ColumnExtractor;
use crate::orchestrator::siphon::route::{CdcContext, Route, RouteOutcome};
use crate::orchestrator::siphon::subjects;
use orbit_utils::traversal_path::TraversalPath;

pub struct EnabledNamespacesRoute {
    namespace_indexing: NamespaceIndexingDispatch,
    code_backfill: Arc<CodeBackfill>,
    datalake: ArrowClickHouseClient,
}

impl EnabledNamespacesRoute {
    pub fn new(
        namespace_indexing: NamespaceIndexingDispatch,
        code_backfill: Arc<CodeBackfill>,
        datalake: ArrowClickHouseClient,
    ) -> Self {
        Self {
            namespace_indexing,
            code_backfill,
            datalake,
        }
    }
}

#[async_trait]
impl Route for EnabledNamespacesRoute {
    fn source_table(&self) -> &str {
        subjects::KNOWLEDGE_GRAPH_ENABLED_NAMESPACES
    }

    async fn dispatch(
        &self,
        ctx: &CdcContext,
        events: &[LogicalReplicationEvents],
    ) -> Result<RouteOutcome, TaskError> {
        let ids = enabled_namespace_ids(events);
        let enabled = lookup_paths(&self.datalake, &ids).await?;

        let sdlc_requests: Vec<NamespaceDispatchRequest> = enabled
            .iter()
            .map(|(namespace_id, traversal_path)| NamespaceDispatchRequest {
                namespace_id: *namespace_id,
                traversal_path: traversal_path.clone(),
                targets: Vec::new(),
            })
            .collect();
        let sdlc = self
            .namespace_indexing
            .dispatch_for_namespaces(&sdlc_requests, chrono::Utc::now(), ctx.campaign_id.clone())
            .await?;
        let code = self
            .code_backfill
            .dispatch_for_namespaces(&enabled, ctx.dispatch_id)
            .await?;
        Ok(RouteOutcome {
            dispatched: sdlc.dispatched + code.dispatched,
            skipped: sdlc.skipped + code.skipped,
        })
    }
}

fn enabled_namespace_ids(events: &[LogicalReplicationEvents]) -> Vec<i64> {
    let mut ids: Vec<i64> = Vec::new();

    for batch in events {
        let extractor = ColumnExtractor::new(batch);
        for event in &batch.events {
            let dispatchable = event.operation == Operation::Insert as i32
                || event.operation == Operation::InitialSnapshot as i32
                || event.operation == Operation::Update as i32;
            if !dispatchable {
                continue;
            }
            match extractor.get_i64(event, "root_namespace_id") {
                Some(id) => ids.push(id),
                None => warn!("failed to extract root_namespace_id, skipping"),
            }
        }
    }

    ids.sort_unstable();
    ids.dedup();
    ids
}

async fn lookup_paths(
    datalake: &ArrowClickHouseClient,
    namespace_ids: &[i64],
) -> Result<Vec<(i64, TraversalPath)>, TaskError> {
    if namespace_ids.is_empty() {
        return Ok(Vec::new());
    }

    let batches = datalake
        .query(resolved_paths_for_namespace_ids_sql())
        .param("ids", namespace_ids.to_vec())
        .fetch_arrow()
        .await
        .map_err(TaskError::new)?;

    let ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
    let paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;
    let found: Vec<(i64, TraversalPath)> = ids
        .into_iter()
        .zip(paths.into_iter().map(TraversalPath::new_unchecked))
        .collect();

    if found.len() < namespace_ids.len() {
        warn!(
            requested = namespace_ids.len(),
            resolved = found.len(),
            "enabled namespaces without a live top-level traversal path; \
             the sweep picks up any that become resolvable"
        );
    }

    Ok(found)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::code::test_helpers::{EventBuilder, build_replication_events_for_table};
    use crate::orchestrator::siphon::decoder::decode_logical_replication_events;

    fn enabled_namespace_event(root_namespace_id: i64) -> EventBuilder {
        EventBuilder::new().with_i64("root_namespace_id", root_namespace_id)
    }

    fn decode(
        events: Vec<(Vec<String>, siphon_proto::ReplicationEvent)>,
    ) -> LogicalReplicationEvents {
        let payload =
            build_replication_events_for_table("knowledge_graph_enabled_namespaces", events);
        decode_logical_replication_events(&payload).unwrap()
    }

    #[test]
    fn extracts_and_dedups_ids_from_insert_events() {
        let decoded = decode(vec![
            enabled_namespace_event(200).build(),
            enabled_namespace_event(100).build(),
            enabled_namespace_event(100).build(),
        ]);

        let ids = enabled_namespace_ids(std::slice::from_ref(&decoded));

        assert_eq!(ids, vec![100, 200]);
    }

    #[test]
    fn skips_delete_events() {
        let decoded = decode(vec![
            enabled_namespace_event(100)
                .with_operation(Operation::Delete as i32)
                .build(),
        ]);

        assert!(enabled_namespace_ids(std::slice::from_ref(&decoded)).is_empty());
    }

    #[test]
    fn extracts_ids_from_snapshot_and_update_events() {
        let decoded = decode(vec![
            enabled_namespace_event(300)
                .with_operation(Operation::InitialSnapshot as i32)
                .build(),
            enabled_namespace_event(400)
                .with_operation(Operation::Update as i32)
                .build(),
        ]);

        let ids = enabled_namespace_ids(std::slice::from_ref(&decoded));

        assert_eq!(ids, vec![300, 400]);
    }

    #[test]
    fn no_events_produce_no_ids() {
        assert!(enabled_namespace_ids(&[]).is_empty());
    }
}
