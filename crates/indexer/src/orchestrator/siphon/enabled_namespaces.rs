//! CDC route: newly-enabled namespaces trigger SDLC indexing and code backfill.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use clickhouse_client::FromArrowColumn;
use siphon_proto::LogicalReplicationEvents;
use siphon_proto::replication_event::Operation;
use tracing::{debug, warn};

use crate::clickhouse::ArrowClickHouseClient;
use crate::orchestrator::dispatch::{
    CodeBackfill, NamespaceDispatchRequest, NamespaceIndexingDispatch,
};
use crate::orchestrator::scheduled::TaskError;
use crate::orchestrator::siphon::decoder::ColumnExtractor;
use crate::orchestrator::siphon::route::{CdcContext, Route, RouteOutcome};
use crate::orchestrator::siphon::subjects;

const NAMESPACE_PATHS_TABLE: &str = "namespace_traversal_paths";

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
        let extracted = extract_enabled_namespaces(events);
        let unresolved: Vec<i64> = extracted
            .iter()
            .filter_map(|(id, path)| path.is_none().then_some(*id))
            .collect();
        let looked_up = if unresolved.is_empty() {
            HashMap::new()
        } else {
            lookup_paths(&self.datalake, &unresolved).await?
        };
        let enabled = merge_paths(extracted, &looked_up);

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

/// Pulls (namespace_id, traversal_path) from insert/snapshot/update CDC events
/// on the enabled-namespaces table.
fn extract_enabled_namespaces(events: &[LogicalReplicationEvents]) -> Vec<(i64, Option<String>)> {
    let mut rows: Vec<(i64, Option<String>)> = Vec::new();

    for replication_events in events {
        let extractor = ColumnExtractor::new(replication_events);

        for event in &replication_events.events {
            let dispatchable = event.operation == Operation::Insert as i32
                || event.operation == Operation::InitialSnapshot as i32
                || event.operation == Operation::Update as i32;

            if !dispatchable {
                debug!(
                    operation = event.operation,
                    "skipping non-dispatchable event"
                );
                continue;
            }

            let Some(root_namespace_id) = extractor.get_i64(event, "root_namespace_id") else {
                warn!("failed to extract root_namespace_id, skipping");
                continue;
            };

            let traversal_path = extractor
                .get_string(event, "traversal_path")
                .filter(|path| !path.is_empty())
                .map(str::to_string);

            rows.push((root_namespace_id, traversal_path));
        }
    }

    rows.sort();
    rows.dedup();
    rows
}

fn merge_paths(
    extracted: Vec<(i64, Option<String>)>,
    looked_up: &HashMap<i64, String>,
) -> Vec<(i64, String)> {
    extracted
        .into_iter()
        .filter_map(|(id, path)| {
            let path = path.or_else(|| looked_up.get(&id).cloned());
            if path.is_none() {
                warn!(
                    root_namespace_id = id,
                    "enabled namespace has no resolvable traversal path yet; \
                     the namespace sweep will pick it up"
                );
            }
            Some((id, path?))
        })
        .collect()
}

async fn lookup_paths(
    datalake: &ArrowClickHouseClient,
    namespace_ids: &[i64],
) -> Result<HashMap<i64, String>, TaskError> {
    let sql = format!(
        "SELECT id, argMax(traversal_path, version) AS traversal_path \
         FROM {NAMESPACE_PATHS_TABLE} \
         WHERE id IN {{ids:Array(Int64)}} \
         GROUP BY id \
         HAVING argMax(deleted, version) = false"
    );

    let batches = datalake
        .query(&sql)
        .param("ids", namespace_ids.to_vec())
        .fetch_arrow()
        .await
        .map_err(TaskError::new)?;
    let ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
    let paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;
    Ok(ids.into_iter().zip(paths).collect())
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

    fn namespace_enabled_without_path(root_namespace_id: i64) -> EventBuilder {
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
    fn extracts_namespace_ids_from_insert_events() {
        let decoded = decode(vec![
            namespace_enabled_columns(100).build(),
            namespace_enabled_columns(200).build(),
        ]);
        let rows = extract_enabled_namespaces(std::slice::from_ref(&decoded));

        assert_eq!(
            rows,
            vec![
                (100, Some("1/100/".to_string())),
                (200, Some("1/200/".to_string()))
            ]
        );
    }

    #[test]
    fn extracts_pathless_events_for_lookup() {
        let decoded = decode(vec![namespace_enabled_without_path(100).build()]);
        let rows = extract_enabled_namespaces(std::slice::from_ref(&decoded));

        assert_eq!(rows, vec![(100, None)]);
    }

    #[test]
    fn empty_path_counts_as_missing() {
        let decoded = decode(vec![
            EventBuilder::new()
                .with_i64("root_namespace_id", 100)
                .with_string("traversal_path", "")
                .build(),
        ]);
        let rows = extract_enabled_namespaces(std::slice::from_ref(&decoded));

        assert_eq!(rows, vec![(100, None)]);
    }

    #[test]
    fn mixed_batches_for_one_namespace_keep_both_rows() {
        let pathless = decode(vec![namespace_enabled_without_path(100).build()]);
        let pathed = decode(vec![namespace_enabled_columns(100).build()]);

        let rows = extract_enabled_namespaces(&[pathless, pathed]);

        assert_eq!(rows, vec![(100, None), (100, Some("1/100/".to_string()))]);
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

        assert_eq!(rows, vec![(300, Some("1/300/".to_string()))]);
    }

    #[test]
    fn extracts_namespace_ids_from_update_events() {
        let decoded = decode(vec![
            namespace_enabled_columns(400)
                .with_operation(Operation::Update as i32)
                .build(),
        ]);
        let rows = extract_enabled_namespaces(std::slice::from_ref(&decoded));

        assert_eq!(rows, vec![(400, Some("1/400/".to_string()))]);
    }

    #[test]
    fn no_events_produces_no_dispatches() {
        assert!(extract_enabled_namespaces(&[]).is_empty());
    }

    #[test]
    fn pathless_rows_resolve_through_the_lookup() {
        let looked_up = HashMap::from([(100, "1/100/".to_string())]);

        let resolved = merge_paths(
            vec![(100, None), (200, Some("1/200/".to_string()))],
            &looked_up,
        );

        assert_eq!(
            resolved,
            vec![(100, "1/100/".to_string()), (200, "1/200/".to_string())]
        );
    }

    #[test]
    fn unresolvable_rows_are_dropped() {
        assert!(merge_paths(vec![(100, None)], &HashMap::new()).is_empty());
    }

    #[test]
    fn event_provided_path_wins_over_the_lookup() {
        let looked_up = HashMap::from([(100, "1/999/".to_string())]);

        let resolved = merge_paths(vec![(100, Some("1/100/".to_string()))], &looked_up);

        assert_eq!(resolved, vec![(100, "1/100/".to_string())]);
    }
}
