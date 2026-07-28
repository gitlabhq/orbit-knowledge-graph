use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use ontology::{EdgeMapping, NodeRef, NodeRefKind, Ontology};
use tracing::{info, warn};

use crate::checkpoint::CheckpointStore;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use gkg_server_config::{ScheduleConfiguration, StaleEdgeReconciliationConfig};

const CHECKPOINT_KEY: &str = "maintenance.stale_edge_reconciliation";

/// Keys look like `ns.<id>.<Entity>`, or `ns.<id>.<Entity>.p1of3` when partitioned.
const NAMESPACE_CHECKPOINT_PREFIX: &str = "ns.";

/// Every edge table shares this sort key, so it doubles as edge identity.
const EDGE_SORT_KEY: &str =
    "traversal_path, relationship_kind, source_id, target_id, source_kind, target_kind";

/// Kinds are batched rather than swept one at a time because scan cost tracks
/// the owner's traversal paths, not the number of kinds: two kinds in one
/// statement measured the same as one.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ReconciliationGroup {
    owner_entity: String,
    owner_node_table: String,
    edge_table: String,
    /// Edge endpoint holding the owner's id; also picks the `*_kind` column that
    /// pins the owner's node type. `gl_edge` is shared, so without that pin the
    /// join would match unrelated node types that share the integer id space.
    owner_id_column: &'static str,
    owner_kind_column: &'static str,
    relationship_kinds: Vec<String>,
}

/// Periodic, dispatcher-side sweep that tombstones edges their owner stopped
/// emitting. Stateless: each run scans a fixed lookback, so a failed run costs
/// nothing beyond the rows it skipped.
pub struct StaleEdgeReconciliation {
    graph: ArrowClickHouseClient,
    checkpoint_store: Arc<dyn CheckpointStore>,
    groups: Vec<ReconciliationGroup>,
    metrics: ScheduledTaskMetrics,
    config: StaleEdgeReconciliationConfig,
}

impl StaleEdgeReconciliation {
    pub fn new(
        graph: ArrowClickHouseClient,
        ontology: &Ontology,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: ScheduledTaskMetrics,
        config: StaleEdgeReconciliationConfig,
    ) -> Self {
        let groups = group_mutable_edges_by_owner(ontology);
        let kinds: Vec<&str> = groups
            .iter()
            .flat_map(|group| group.relationship_kinds.iter().map(String::as_str))
            .collect();
        info!(
            groups = groups.len(),
            ?kinds,
            "stale-edge reconciliation groups resolved",
        );
        Self {
            graph,
            checkpoint_store,
            groups,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl ScheduledTask for StaleEdgeReconciliation {
    fn name(&self) -> &str {
        CHECKPOINT_KEY
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let start = Instant::now();
        let result = self.reconcile_all().await;
        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics.record_run(self.name(), outcome, duration);
        result
    }
}

impl StaleEdgeReconciliation {
    async fn reconcile_all(&self) -> Result<(), TaskError> {
        let pending = self.find_namespaces_with_pending_writes().await?;
        // `_version` is a Siphon source watermark, so this bounds how far back
        // the owner scan reaches, not how long ago the sweep last ran.
        let cursor = (Utc::now() - self.config.lookback())
            .format(TIMESTAMP_FORMAT)
            .to_string();

        let mut failed = 0u64;
        for group in &self.groups {
            info!(
                owner = group.owner_entity,
                owner_table = group.owner_node_table,
                edge_table = group.edge_table,
                kinds = ?group.relationship_kinds,
                cursor = cursor,
                pending_namespaces = pending.len(),
                "reconcile started",
            );
            let statement_start = Instant::now();
            let result = self.reconcile_group(group, &cursor, &pending).await;
            let elapsed = statement_start.elapsed();
            match result {
                Ok(()) => {
                    self.metrics.record_query_duration(
                        &group.relationship_kinds.join(","),
                        elapsed.as_secs_f64(),
                    );
                    info!(
                        owner = group.owner_entity,
                        edge_table = group.edge_table,
                        duration_ms = elapsed.as_millis() as u64,
                        outcome = "success",
                        "reconcile completed",
                    );
                }
                Err(error) => {
                    failed += 1;
                    self.metrics.record_error(self.name(), "reconcile");
                    warn!(
                        owner = group.owner_entity,
                        edge_table = group.edge_table,
                        duration_ms = elapsed.as_millis() as u64,
                        outcome = "error",
                        %error,
                        "reconcile completed",
                    );
                }
            }
        }

        if failed > 0 {
            return Err(TaskError::new(format!(
                "{failed}/{} reconcile statements failed",
                self.groups.len()
            )));
        }
        Ok(())
    }

    async fn reconcile_group(
        &self,
        group: &ReconciliationGroup,
        cursor: &str,
        pending_namespaces: &[i64],
    ) -> Result<(), TaskError> {
        self.graph
            .query(&build_tombstone_statement(group, pending_namespaces))
            .param("cursor", cursor)
            .execute()
            .await
            .map_err(TaskError::new)
    }

    /// `save_completed` clears `cursor_values` only once every page of a run has
    /// drained, so a set cursor means this namespace's edges may not have landed
    /// yet and comparing versions would tombstone them.
    async fn find_namespaces_with_pending_writes(&self) -> Result<Vec<i64>, TaskError> {
        let checkpoints = self
            .checkpoint_store
            .load_by_prefix(NAMESPACE_CHECKPOINT_PREFIX)
            .await
            .map_err(TaskError::new)?;

        let mut ids: Vec<i64> = checkpoints
            .iter()
            .filter(|(_, checkpoint)| checkpoint.cursor_values.is_some())
            .filter_map(|(key, _)| parse_namespace_id_from_checkpoint_key(key))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }
}

fn parse_namespace_id_from_checkpoint_key(key: &str) -> Option<i64> {
    key.strip_prefix(NAMESPACE_CHECKPOINT_PREFIX)?
        .split('.')
        .next()?
        .parse()
        .ok()
}

fn group_mutable_edges_by_owner(ontology: &Ontology) -> Vec<ReconciliationGroup> {
    let mut kinds_by_group: BTreeMap<(String, String, &'static str), Vec<String>> = BTreeMap::new();
    let mut owner_tables_by_entity: BTreeMap<String, String> = BTreeMap::new();

    for node in ontology.nodes() {
        if node.global {
            continue;
        }
        for pipeline in &node.pipelines {
            for mapping in pipeline.transform.edges() {
                if !mapping.mutable {
                    continue;
                }
                let Some(owner_id_column) = find_owner_id_column(mapping, &node.name) else {
                    continue;
                };
                let edge_table = prefixed_table_name(
                    ontology.edge_table_for_relationship(&mapping.label),
                    *SCHEMA_VERSION,
                );
                owner_tables_by_entity.insert(
                    node.name.clone(),
                    prefixed_table_name(&node.destination_table, *SCHEMA_VERSION),
                );
                kinds_by_group
                    .entry((node.name.clone(), edge_table, owner_id_column))
                    .or_default()
                    .push(mapping.label.clone());
            }
        }
    }

    kinds_by_group
        .into_iter()
        .map(|((owner_entity, edge_table, owner_id_column), mut kinds)| {
            kinds.sort();
            kinds.dedup();
            ReconciliationGroup {
                owner_node_table: owner_tables_by_entity[&owner_entity].clone(),
                owner_entity,
                edge_table,
                owner_id_column,
                owner_kind_column: if owner_id_column == "source_id" {
                    "source_kind"
                } else {
                    "target_kind"
                },
                relationship_kinds: kinds,
            }
        })
        .collect()
}

/// `None` for polymorphic or derived endpoints: with no side bound to the
/// owner's `id` there is no owner row to compare versions against.
fn find_owner_id_column(mapping: &EdgeMapping, node_name: &str) -> Option<&'static str> {
    let binds_owner = |node_ref: &NodeRef| {
        matches!(&node_ref.kind, NodeRefKind::Literal(kind) if kind == node_name)
            && node_ref.field == "id"
    };
    if binds_owner(&mapping.source) {
        Some("source_id")
    } else if binds_owner(&mapping.target) {
        Some("target_id")
    } else {
        None
    }
}

/// Staleness is `edge._version < owner._version`: the ETL re-emits every edge of
/// a source row stamped with that row's `_version`, so an edge left behind was
/// not re-emitted.
///
/// Both sides dedup with `LIMIT 1 BY` rather than `FINAL`. `FINAL` defeats
/// granule pruning on `_version` (measured 26.5M rows vs 22K for one window),
/// and on the edge side an un-deduped scan makes superseded copies of live edges
/// look stale (measured 18,879 false positives against 0). Pushing the cursor
/// inside the owner dedup is safe — a key whose newest version predates the
/// cursor has no version at or after it — but `_deleted` must stay outside on
/// both sides, where it reproduces `FINAL` dropping tombstoned keys.
fn build_tombstone_statement(group: &ReconciliationGroup, excluded_namespaces: &[i64]) -> String {
    let ReconciliationGroup {
        owner_node_table,
        edge_table,
        owner_id_column,
        owner_kind_column,
        owner_entity,
        relationship_kinds,
    } = group;

    let kinds = relationship_kinds
        .iter()
        .map(|kind| format!("'{kind}'"))
        .collect::<Vec<_>>()
        .join(", ");

    // A namespace mid-write has edges that have not landed yet.
    let namespace_guard = if excluded_namespaces.is_empty() {
        String::new()
    } else {
        let ids = excluded_namespaces
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        format!(" AND toInt64OrZero(extract(traversal_path, '^[0-9]+/([0-9]+)/')) NOT IN ({ids})")
    };

    format!(
        "INSERT INTO {edge_table} \
           (traversal_path, relationship_kind, source_id, source_kind, target_id, target_kind, _version, _deleted) \
         WITH owner AS ( \
           SELECT id, traversal_path, _version FROM ( \
             SELECT id, traversal_path, _version, _deleted \
             FROM {owner_node_table} \
             WHERE _version >= {{cursor:String}}{namespace_guard} \
             ORDER BY traversal_path, id, _version DESC \
             LIMIT 1 BY traversal_path, id \
           ) WHERE _deleted = false \
         ), \
         edge AS ( \
           SELECT {EDGE_SORT_KEY}, _version FROM ( \
             SELECT {EDGE_SORT_KEY}, _version, _deleted \
             FROM {edge_table} \
             WHERE relationship_kind IN ({kinds}) \
               AND {owner_kind_column} = '{owner_entity}' \
               AND traversal_path IN (SELECT traversal_path FROM owner) \
               AND {owner_id_column} IN (SELECT id FROM owner) \
             ORDER BY {EDGE_SORT_KEY}, _version DESC \
             LIMIT 1 BY {EDGE_SORT_KEY} \
           ) WHERE _deleted = false \
         ) \
         SELECT edge.traversal_path, edge.relationship_kind, edge.source_id, edge.source_kind, \
                edge.target_id, edge.target_kind, owner._version, true \
         FROM edge \
         JOIN owner ON owner.id = edge.{owner_id_column} AND owner.traversal_path = edge.traversal_path \
         WHERE edge._version < owner._version"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn groups() -> Vec<ReconciliationGroup> {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        group_mutable_edges_by_owner(&ontology)
    }

    fn find<'a>(
        groups: &'a [ReconciliationGroup],
        owner: &str,
        edge_table_suffix: &str,
        endpoint: &str,
    ) -> &'a ReconciliationGroup {
        groups
            .iter()
            .find(|g| {
                g.owner_entity == owner
                    && g.edge_table.ends_with(edge_table_suffix)
                    && g.owner_id_column == endpoint
            })
            .unwrap_or_else(|| panic!("expected group {owner}/*{edge_table_suffix}/{endpoint}"))
    }

    fn fixture() -> ReconciliationGroup {
        ReconciliationGroup {
            owner_entity: "Pipeline".to_string(),
            owner_node_table: "v57_gl_pipeline".to_string(),
            edge_table: "v57_gl_ci_edge".to_string(),
            owner_id_column: "target_id",
            owner_kind_column: "target_kind",
            relationship_kinds: vec!["TRIGGERED".to_string(), "AUTO_CANCELED_BY".to_string()],
        }
    }

    #[test]
    fn outgoing_edge_owner_is_source() {
        let groups = groups();
        let diff = find(&groups, "MergeRequest", "gl_diff_edge", "source_id");
        assert_eq!(diff.owner_kind_column, "source_kind");
        assert!(
            diff.relationship_kinds
                .contains(&"HAS_LATEST_DIFF".to_string())
        );
    }

    #[test]
    fn incoming_edge_owner_is_target() {
        let groups = groups();
        let incoming = find(&groups, "MergeRequest", "gl_edge", "target_id");
        assert_eq!(incoming.owner_kind_column, "target_kind");
        assert!(
            incoming
                .relationship_kinds
                .contains(&"LAST_EDITED_BY".to_string())
        );
    }

    #[test]
    fn kinds_sharing_owner_and_table_collapse_into_one_group() {
        let groups = groups();
        let incoming = find(&groups, "MergeRequest", "gl_edge", "target_id");
        assert!(
            incoming.relationship_kinds.len() > 1,
            "UPDATED_BY and LAST_EDITED_BY share an owner, table and endpoint so they \
             must batch into one statement: {incoming:?}",
        );
    }

    #[test]
    fn only_mutable_mappings_are_swept() {
        let groups = groups();
        let swept: std::collections::BTreeSet<&str> = groups
            .iter()
            .flat_map(|g| g.relationship_kinds.iter().map(String::as_str))
            .collect();
        for immutable in [
            "IN_PROJECT",
            "AUTHORED",
            "HAS_JOB",
            "IN_PIPELINE",
            "MERGED_AT_COMMIT",
            "HAS_IDENTIFIER",
        ] {
            assert!(
                !swept.contains(immutable),
                "{immutable} is not marked mutable in the ontology and must not be swept",
            );
        }
    }

    #[test]
    fn polymorphic_endpoint_has_no_owner_row_to_compare() {
        let polymorphic = EdgeMapping {
            source: NodeRef {
                field: "noteable_id".to_string(),
                property_inputs: Default::default(),
                enrich: false,
                kind: NodeRefKind::Derived {
                    column: "noteable_type".to_string(),
                    mapping: Default::default(),
                },
            },
            target: NodeRef {
                field: "user_id".to_string(),
                kind: NodeRefKind::Literal("User".to_string()),
                property_inputs: Default::default(),
                enrich: false,
            },
            label: "HAS_NOTE".to_string(),
            array_field: None,
            mutable: true,
        };
        assert_eq!(find_owner_id_column(&polymorphic, "MergeRequest"), None);
    }

    #[test]
    fn every_group_targets_versioned_tables() {
        let prefix = format!("v{}_", *SCHEMA_VERSION);
        for group in groups() {
            assert!(group.edge_table.starts_with(&prefix), "{group:?}");
            assert!(group.owner_node_table.starts_with(&prefix), "{group:?}");
        }
    }

    #[test]
    fn sql_pins_owner_kind_and_batches_every_relationship_kind() {
        let sql = build_tombstone_statement(&fixture(), &[]);

        assert!(sql.contains("INSERT INTO v57_gl_ci_edge"), "{sql}");
        assert!(sql.contains("FROM v57_gl_pipeline "), "{sql}");
        assert!(sql.contains("_version >= {cursor:String}"), "{sql}");
        assert!(
            sql.contains("relationship_kind IN ('TRIGGERED', 'AUTO_CANCELED_BY')"),
            "{sql}"
        );
        assert!(sql.contains("target_kind = 'Pipeline'"), "{sql}");
        assert!(!sql.contains("source_kind = "), "{sql}");
    }

    #[test]
    fn staleness_compares_versions_and_stamps_from_the_owner() {
        let sql = build_tombstone_statement(&fixture(), &[]);

        assert!(sql.contains("edge._version < owner._version"), "{sql}");
        assert!(sql.contains("target_kind, _version, _deleted)"), "{sql}");
        assert!(sql.contains("owner._version, true"), "{sql}");
        assert!(!sql.contains("now64"), "{sql}");
    }

    #[test]
    fn both_sides_dedup_without_final_and_filter_deleted_after() {
        let sql = build_tombstone_statement(&fixture(), &[]);

        assert!(!sql.contains("FINAL"), "{sql}");
        assert_eq!(sql.matches("LIMIT 1 BY").count(), 2, "{sql}");

        // A `_deleted` filter inside either dedup would drop a tombstoned newest
        // row and let a superseded one win, which is how valid edges get retired.
        for dedup in [
            "LIMIT 1 BY traversal_path, id",
            "LIMIT 1 BY traversal_path, relationship_kind",
        ] {
            let at = sql
                .find(dedup)
                .unwrap_or_else(|| panic!("{dedup} missing: {sql}"));
            let filter = sql[at..].find("_deleted = false").map(|i| i + at);
            assert!(
                filter.is_some(),
                "{dedup} has no trailing _deleted filter: {sql}"
            );
        }
    }

    #[test]
    fn find_namespaces_with_pending_writes_are_excluded_from_the_owner_scan() {
        let sql = build_tombstone_statement(&fixture(), &[7, 42]);
        assert!(sql.contains("NOT IN (7, 42)"), "{sql}");

        let clean = build_tombstone_statement(&fixture(), &[]);
        assert!(!clean.contains("NOT IN ("), "{clean}");
    }

    #[test]
    fn parse_namespace_id_from_checkpoint_key_reads_the_id_segment() {
        assert_eq!(
            parse_namespace_id_from_checkpoint_key("ns.42.MergeRequest"),
            Some(42)
        );
        assert_eq!(
            parse_namespace_id_from_checkpoint_key("ns.7.Job.p1of3"),
            Some(7)
        );
        assert_eq!(
            parse_namespace_id_from_checkpoint_key("maintenance.something"),
            None
        );
    }
}
