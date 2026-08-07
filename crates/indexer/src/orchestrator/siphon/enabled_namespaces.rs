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
    path_lookup: Arc<dyn NamespacePathLookup>,
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
            path_lookup: Arc::new(datalake),
        }
    }

    /// Fills in traversal paths the CDC events did not carry. Real Siphon
    /// events never carry one — `knowledge_graph_enabled_namespaces` has no
    /// such column in Postgres — so before this lookup existed every
    /// enrollment event was skipped and namespaces waited for the hourly
    /// sweep. Namespaces whose path is not resolvable yet (their namespaces
    /// row has not replicated) are skipped with a warning; the sweep remains
    /// the backstop for those.
    async fn resolve_paths(
        &self,
        extracted: Vec<(i64, Option<String>)>,
    ) -> Result<Vec<(i64, String)>, TaskError> {
        let unresolved: Vec<i64> = extracted
            .iter()
            .filter_map(|(id, path)| path.is_none().then_some(*id))
            .collect();

        let looked_up = if unresolved.is_empty() {
            HashMap::new()
        } else {
            self.path_lookup.paths_for(&unresolved).await?
        };

        Ok(extracted
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
            .collect())
    }
}

/// Pulls (namespace_id, traversal_path) from insert/snapshot/update CDC events
/// on the enabled-namespaces table. The path is `None` when the event does not
/// carry a usable `traversal_path` column.
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
    rows.dedup_by_key(|(id, _)| *id);
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
        let enabled = self
            .resolve_paths(extract_enabled_namespaces(events))
            .await?;
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

#[async_trait]
pub(crate) trait NamespacePathLookup: Send + Sync {
    async fn paths_for(&self, namespace_ids: &[i64]) -> Result<HashMap<i64, String>, TaskError>;
}

#[async_trait]
impl NamespacePathLookup for ArrowClickHouseClient {
    async fn paths_for(&self, namespace_ids: &[i64]) -> Result<HashMap<i64, String>, TaskError> {
        // Reads the dictionary's source table instead of the dictionary: during
        // the enrollment race a dictionary miss is negatively cached for up to
        // its LIFETIME, while the table sees the namespaces row the moment the
        // materialized view commits it.
        let ids = namespace_ids
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT id, argMax(traversal_path, version) AS traversal_path \
             FROM {NAMESPACE_PATHS_TABLE} \
             WHERE id IN ({ids}) \
             GROUP BY id \
             HAVING argMax(deleted, version) = false"
        );

        let batches = self
            .query(&sql)
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;
        let ids = i64::extract_column(&batches, 0).map_err(TaskError::new)?;
        let paths = String::extract_column(&batches, 1).map_err(TaskError::new)?;
        Ok(ids.into_iter().zip(paths).collect())
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

    struct StubLookup {
        paths: HashMap<i64, String>,
    }

    #[async_trait]
    impl NamespacePathLookup for StubLookup {
        async fn paths_for(
            &self,
            namespace_ids: &[i64],
        ) -> Result<HashMap<i64, String>, TaskError> {
            Ok(namespace_ids
                .iter()
                .filter_map(|id| self.paths.get(id).map(|path| (*id, path.clone())))
                .collect())
        }
    }

    fn route_with_lookup(paths: HashMap<i64, String>) -> EnabledNamespacesRoute {
        let empty = &std::collections::HashMap::new();
        let dead_client = ArrowClickHouseClient::new(
            "http://localhost:0",
            "default",
            "default",
            None,
            empty,
            empty,
        );
        EnabledNamespacesRoute {
            namespace_indexing: NamespaceIndexingDispatch::new(Arc::new(
                crate::testkit::MockNatsServices::new(),
            )),
            code_backfill: Arc::new(CodeBackfill::new(
                Arc::new(crate::testkit::MockNatsServices::new()),
                dead_client.clone(),
                dead_client,
                crate::orchestrator::scheduled::ScheduledTaskMetrics::with_meter(
                    &crate::testkit::test_meter(),
                ),
                Arc::new(crate::campaign::CampaignState::new()),
            )),
            path_lookup: Arc::new(StubLookup { paths }),
        }
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

    #[tokio::test]
    async fn pathless_rows_resolve_through_the_lookup() {
        let route = route_with_lookup(HashMap::from([(100, "1/100/".to_string())]));

        let resolved = route
            .resolve_paths(vec![(100, None), (200, Some("1/200/".to_string()))])
            .await
            .unwrap();

        assert_eq!(
            resolved,
            vec![(100, "1/100/".to_string()), (200, "1/200/".to_string())]
        );
    }

    #[tokio::test]
    async fn unresolvable_rows_are_dropped() {
        let route = route_with_lookup(HashMap::new());

        let resolved = route.resolve_paths(vec![(100, None)]).await.unwrap();

        assert!(resolved.is_empty());
    }

    #[tokio::test]
    async fn event_provided_path_wins_over_the_lookup() {
        let route = route_with_lookup(HashMap::from([(100, "1/999/".to_string())]));

        let resolved = route
            .resolve_paths(vec![(100, Some("1/100/".to_string()))])
            .await
            .unwrap();

        assert_eq!(resolved, vec![(100, "1/100/".to_string())]);
    }
}
