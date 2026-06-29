//! CDC route: newly-enabled namespaces trigger SDLC indexing and code backfill.

use std::sync::Arc;

use async_trait::async_trait;
use siphon_proto::LogicalReplicationEvents;
use siphon_proto::replication_event::Operation;
use tracing::{debug, warn};

use crate::orchestrator::dispatch::{
    CodeBackfill, NamespaceDispatchRequest, NamespaceIndexingDispatch,
};
use crate::orchestrator::scheduled::TaskError;
use crate::orchestrator::siphon::decoder::ColumnExtractor;
use crate::orchestrator::siphon::route::{CdcContext, Route, RouteOutcome};
use crate::orchestrator::siphon::subjects;

pub struct EnabledNamespacesRoute {
    namespace_indexing: NamespaceIndexingDispatch,
    code_backfill: Arc<CodeBackfill>,
}

impl EnabledNamespacesRoute {
    pub fn new(
        namespace_indexing: NamespaceIndexingDispatch,
        code_backfill: Arc<CodeBackfill>,
    ) -> Self {
        Self {
            namespace_indexing,
            code_backfill,
        }
    }
}

/// Pulls (namespace_id, traversal_path) from inserted CDC events on the
/// enabled-namespaces table. The replicated row carries `traversal_path`
/// directly, so no follow-up lookup is needed.
fn extract_enabled_namespaces(events: &[LogicalReplicationEvents]) -> Vec<(i64, String)> {
    let mut rows: Vec<(i64, String)> = Vec::new();

    for replication_events in events {
        let extractor = ColumnExtractor::new(replication_events);

        for event in &replication_events.events {
            let is_insert = event.operation == Operation::Insert as i32;
            let is_snapshot = event.operation == Operation::InitialSnapshot as i32;

            if !is_insert && !is_snapshot {
                debug!(
                    operation = event.operation,
                    "skipping non-insert/snapshot event"
                );
                continue;
            }

            let Some(root_namespace_id) = extractor.get_i64(event, "root_namespace_id") else {
                warn!("failed to extract root_namespace_id, skipping");
                continue;
            };

            let Some(traversal_path) = extractor.get_string(event, "traversal_path") else {
                warn!(
                    root_namespace_id,
                    "CDC event missing traversal_path; skipping (re-tries next tick via active backfill)"
                );
                continue;
            };

            if traversal_path.is_empty() {
                warn!(
                    root_namespace_id,
                    "CDC event has empty traversal_path; skipping to avoid prefix-matching every project"
                );
                continue;
            }

            rows.push((root_namespace_id, traversal_path.to_string()));
        }
    }

    rows.sort();
    rows.dedup();
    rows
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
        let enabled = extract_enabled_namespaces(events);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::code::test_helpers::{EventBuilder, build_replication_events_for_table};
    use crate::orchestrator::siphon::decoder::decode_logical_replication_events;

    fn namespace_enabled_columns(root_namespace_id: i64) -> EventBuilder {
        let traversal_path = format!("1/{root_namespace_id}/");
        EventBuilder::new()
            .with_i64("root_namespace_id", root_namespace_id)
            .with_string("traversal_path", &traversal_path)
    }

    fn decode(
        events: Vec<(Vec<String>, siphon_proto::ReplicationEvent)>,
    ) -> LogicalReplicationEvents {
        let payload =
            build_replication_events_for_table("knowledge_graph_enabled_namespaces", events);
        decode_logical_replication_events(&payload).unwrap()
    }

    #[test]
    fn extracts_namespace_ids_from_insert_events() {
        let decoded = decode(vec![
            namespace_enabled_columns(100).build(),
            namespace_enabled_columns(200).build(),
        ]);
        let rows = extract_enabled_namespaces(std::slice::from_ref(&decoded));

        assert_eq!(
            rows,
            vec![(100, "1/100/".to_string()), (200, "1/200/".to_string())]
        );
    }

    #[test]
    fn skips_delete_events() {
        let decoded = decode(vec![
            namespace_enabled_columns(100)
                .with_operation(Operation::Delete as i32)
                .build(),
        ]);
        let rows = extract_enabled_namespaces(std::slice::from_ref(&decoded));

        assert!(rows.is_empty());
    }

    #[test]
    fn extracts_namespace_ids_from_snapshot_events() {
        let decoded = decode(vec![
            namespace_enabled_columns(300)
                .with_operation(Operation::InitialSnapshot as i32)
                .build(),
        ]);
        let rows = extract_enabled_namespaces(std::slice::from_ref(&decoded));

        assert_eq!(rows, vec![(300, "1/300/".to_string())]);
    }

    #[test]
    fn no_events_produces_no_dispatches() {
        assert!(extract_enabled_namespaces(&[]).is_empty());
    }
}
