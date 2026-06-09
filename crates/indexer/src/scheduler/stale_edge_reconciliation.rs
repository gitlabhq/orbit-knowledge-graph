use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ontology::{EdgeDirection, EdgeTarget, EtlScope, Ontology};
use tracing::{info, warn};

use crate::checkpoint::CheckpointStore;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::scheduler::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use gkg_server_config::{ScheduleConfiguration, StaleEdgeReconciliationConfig};

const CHECKPOINT_KEY: &str = "maintenance.stale_edge_reconciliation";

/// One FK-derived edge variant the sweep can reconcile: a scalar, single-value,
/// literal-target edge whose endpoint is the value of a queryable column on the
/// owning node table.
///
/// Array/multi-value edges (labels, assignees) and polymorphic edges are
/// excluded — their endpoint is legitimately many-valued, so a single-FK
/// comparison would wrongly tombstone live edges.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ReconciliationSpec {
    relationship_kind: String,
    edge_table: String,
    owner_node_table: String,
    /// Graph node-table column holding the current FK value. May differ from the
    /// edge-map key, which is the *extract* column name (e.g. the CLOSED edge is
    /// keyed by `metric_latest_closed_by_id` but stored as `closed_by_id`).
    owner_fk_column: String,
    source_kind: String,
    target_kind: String,
    /// Edge endpoint holding the owner's id (the join key); the `other` endpoint
    /// is what must equal the owner's current FK, else the edge is stale.
    owner_id_column: &'static str,
    other_id_column: &'static str,
}

/// Periodic, dispatcher-side sweep that tombstones stale FK-derived edges.
///
/// Runs one `INSERT … SELECT` per spec sequentially (bounded memory), keyed off a
/// single watermark cursor advanced only on full success. Idempotent:
/// re-tombstoning an already-stale edge is a no-op, so a failed run just widens
/// the next window.
pub struct StaleEdgeReconciliation {
    graph: ArrowClickHouseClient,
    checkpoint_store: Arc<dyn CheckpointStore>,
    specs: Vec<ReconciliationSpec>,
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
        let specs = reconciliation_specs(ontology, &config.relationship_kinds);
        info!(
            specs = specs.len(),
            kinds = ?config.relationship_kinds,
            "stale-edge reconciliation specs resolved",
        );
        Self {
            graph,
            checkpoint_store,
            specs,
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
        // Capture the new watermark before reading so rows written during the run
        // are caught next time (overlap is harmless given idempotency), never lost.
        let new_watermark = Utc::now();
        let last_watermark = self
            .checkpoint_store
            .load(CHECKPOINT_KEY)
            .await
            .map_err(TaskError::new)?
            .map(|checkpoint| checkpoint.watermark)
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH);
        let cursor = last_watermark.format(TIMESTAMP_FORMAT).to_string();

        let mut failed = 0u64;
        for spec in &self.specs {
            let statement_start = Instant::now();
            match self.reconcile_one(spec, &cursor).await {
                Ok(()) => {
                    info!(
                        relationship_kind = spec.relationship_kind,
                        owner = spec.owner_node_table,
                        duration_ms = statement_start.elapsed().as_millis() as u64,
                        "reconciled stale edges",
                    );
                }
                Err(error) => {
                    failed += 1;
                    self.metrics.record_error(self.name(), "reconcile");
                    warn!(
                        relationship_kind = spec.relationship_kind,
                        owner = spec.owner_node_table,
                        %error,
                        "failed to reconcile stale edges",
                    );
                }
            }
        }

        if failed > 0 {
            // Leave the cursor where it was so the next run re-scans this window.
            return Err(TaskError::new(format!(
                "{failed}/{} reconcile statements failed",
                self.specs.len()
            )));
        }

        self.checkpoint_store
            .save_completed(CHECKPOINT_KEY, &new_watermark)
            .await
            .map_err(TaskError::new)?;

        Ok(())
    }

    async fn reconcile_one(
        &self,
        spec: &ReconciliationSpec,
        cursor: &str,
    ) -> Result<(), TaskError> {
        self.graph
            .query(&build_reconcile_sql(spec))
            .param("cursor", cursor)
            .with_setting("max_threads", self.config.max_threads.to_string())
            .with_setting(
                "max_memory_usage",
                self.config.max_memory_usage_bytes.to_string(),
            )
            .execute()
            .await
            .map_err(TaskError::new)
    }
}

/// Enumerates the FK-derived edge variants to reconcile. Metadata (owner table,
/// graph column, direction, endpoint kinds) is ontology-derived for correctness;
/// the *set* is scoped to `kinds` so immutable FKs aren't swept needlessly.
fn reconciliation_specs(ontology: &Ontology, kinds: &[String]) -> Vec<ReconciliationSpec> {
    let mut specs = Vec::new();

    for node in ontology.nodes() {
        let Some(etl) = &node.etl else { continue };
        // Global owners have no traversal_path on the node table, so the dual-IN
        // PK prune that makes this cheap doesn't apply. The measured staleness is
        // all on namespaced entities (MR, WorkItem, Pipeline).
        if etl.scope() != EtlScope::Namespaced {
            continue;
        }

        for (fk_extract_column, mapping) in etl.edge_mappings() {
            if !kinds.iter().any(|k| k == &mapping.relationship_kind) {
                continue;
            }
            // Only scalar single-value edges: array/exploded endpoints are
            // legitimately many-valued.
            if mapping.delimiter.is_some() || mapping.array_field.is_some() || mapping.array {
                continue;
            }
            // Polymorphic targets resolve their kind from a column, not a fixed
            // node type; skip — the single-FK identity doesn't hold.
            let EdgeTarget::Literal(other_kind) = &mapping.target else {
                continue;
            };

            // The reconcile reads the graph node table, so we need the column
            // *name* there, which is the field whose source is the extract column.
            // No matching stored field means the FK isn't queryable on the node
            // table and we can't establish current truth — skip.
            let Some(owner_fk_column) = node
                .fields
                .iter()
                .find(|field| field.column_name() == Some(fk_extract_column.as_str()))
                .map(|field| field.name.clone())
            else {
                continue;
            };

            let edge_table = prefixed_table_name(
                ontology.edge_table_for_relationship(&mapping.relationship_kind),
                *SCHEMA_VERSION,
            );
            let owner_node_table = prefixed_table_name(&node.destination_table, *SCHEMA_VERSION);

            let (source_kind, target_kind, owner_id_column, other_id_column) =
                match mapping.direction {
                    EdgeDirection::Outgoing => (
                        node.name.clone(),
                        other_kind.clone(),
                        "source_id",
                        "target_id",
                    ),
                    EdgeDirection::Incoming => (
                        other_kind.clone(),
                        node.name.clone(),
                        "target_id",
                        "source_id",
                    ),
                };

            specs.push(ReconciliationSpec {
                relationship_kind: mapping.relationship_kind.clone(),
                edge_table,
                owner_node_table,
                owner_fk_column,
                source_kind,
                target_kind,
                owner_id_column,
                other_id_column,
            });
        }
    }

    specs
}

/// Builds the single tombstone statement for one spec.
///
/// `source_kind`/`target_kind` are pinned to concrete node types so a kind
/// emitted from several FK columns into the same table (e.g. `TRIGGERED` from
/// both `merge_request_id` and `user_id`) reconciles each variant in isolation.
fn build_reconcile_sql(spec: &ReconciliationSpec) -> String {
    let ReconciliationSpec {
        relationship_kind,
        edge_table,
        owner_node_table,
        owner_fk_column,
        source_kind,
        target_kind,
        owner_id_column,
        other_id_column,
    } = spec;

    format!(
        "INSERT INTO {edge_table} \
           (traversal_path, relationship_kind, source_id, source_kind, target_id, target_kind, _deleted) \
         WITH c AS ( \
           SELECT id, traversal_path, {owner_fk_column} AS fk \
           FROM {owner_node_table} FINAL \
           WHERE _version >= {{cursor:String}} \
         ) \
         SELECT e.traversal_path, e.relationship_kind, e.source_id, e.source_kind, \
                e.target_id, e.target_kind, true \
         FROM {edge_table} e \
         JOIN c ON c.id = e.{owner_id_column} AND c.traversal_path = e.traversal_path \
         WHERE e.relationship_kind = '{relationship_kind}' \
           AND e.source_kind = '{source_kind}' \
           AND e.target_kind = '{target_kind}' \
           AND e._deleted = false \
           AND e.traversal_path IN (SELECT traversal_path FROM c) \
           AND e.{owner_id_column} IN (SELECT id FROM c) \
           AND (c.fk IS NULL OR e.{other_id_column} != c.fk)"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn specs() -> Vec<ReconciliationSpec> {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let kinds = StaleEdgeReconciliationConfig::default().relationship_kinds;
        reconciliation_specs(&ontology, &kinds)
    }

    fn specs_for(kinds: &[&str]) -> Vec<ReconciliationSpec> {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let kinds: Vec<String> = kinds.iter().map(|k| k.to_string()).collect();
        reconciliation_specs(&ontology, &kinds)
    }

    fn find<'a>(
        specs: &'a [ReconciliationSpec],
        kind: &str,
        owner_table_suffix: &str,
    ) -> &'a ReconciliationSpec {
        specs
            .iter()
            .find(|s| {
                s.relationship_kind == kind && s.owner_node_table.ends_with(owner_table_suffix)
            })
            .unwrap_or_else(|| panic!("expected spec {kind} owned by *{owner_table_suffix}"))
    }

    #[test]
    fn outgoing_edge_owner_is_source() {
        let specs = specs();
        let diff = find(&specs, "HAS_LATEST_DIFF", "gl_merge_request");
        assert_eq!(diff.owner_id_column, "source_id");
        assert_eq!(diff.other_id_column, "target_id");
        assert_eq!(diff.source_kind, "MergeRequest");
        assert_eq!(diff.target_kind, "MergeRequestDiff");
        assert_eq!(diff.owner_fk_column, "latest_merge_request_diff_id");
    }

    #[test]
    fn incoming_edge_owner_is_target() {
        let specs = specs();
        let edited = find(&specs, "LAST_EDITED_BY", "gl_merge_request");
        assert_eq!(edited.owner_id_column, "target_id");
        assert_eq!(edited.other_id_column, "source_id");
        assert_eq!(edited.source_kind, "User");
        assert_eq!(edited.target_kind, "MergeRequest");
    }

    #[test]
    fn resolves_renamed_graph_column() {
        let specs = specs_for(&["CLOSED"]);
        let closed = find(&specs, "CLOSED", "gl_merge_request");
        // Edge-map key is `metric_latest_closed_by_id`; node table stores `closed_by_id`.
        assert_eq!(closed.owner_fk_column, "closed_by_id");
    }

    #[test]
    fn immutable_fk_kinds_are_not_swept_by_default() {
        let specs = specs();
        for immutable in ["IN_PROJECT", "AUTHORED", "HAS_JOB", "IN_PIPELINE"] {
            assert!(
                !specs.iter().any(|s| s.relationship_kind == immutable),
                "{immutable} is not in the default allowlist and must not be swept",
            );
        }
    }

    #[test]
    fn array_and_polymorphic_edges_are_excluded_even_when_allowlisted() {
        let ontology = Ontology::load_embedded().unwrap();
        let kinds = vec![
            "REVIEWER".to_string(),
            "APPROVED".to_string(),
            "HAS_NOTE".to_string(),
        ];
        let specs = reconciliation_specs(&ontology, &kinds);
        assert!(
            specs.is_empty(),
            "array_field (REVIEWER/APPROVED) and polymorphic (HAS_NOTE) edges must never be \
             swept even if explicitly listed, got {specs:?}",
        );
    }

    #[test]
    fn empty_allowlist_disables_the_sweep() {
        let ontology = Ontology::load_embedded().unwrap();
        assert!(reconciliation_specs(&ontology, &[]).is_empty());
    }

    #[test]
    fn default_allowlist_resolves_exactly_the_enabled_kinds() {
        let specs = specs();
        let kinds: std::collections::BTreeSet<&str> =
            specs.iter().map(|s| s.relationship_kind.as_str()).collect();
        assert_eq!(
            kinds,
            [
                "HAS_HEAD_PIPELINE",
                "HAS_LATEST_DIFF",
                "IN_MILESTONE",
                "LAST_EDITED_BY"
            ]
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>(),
        );
    }

    #[test]
    fn triggered_variants_are_disambiguated_by_kind() {
        let specs = specs_for(&["TRIGGERED"]);
        let triggered: Vec<&ReconciliationSpec> = specs
            .iter()
            .filter(|s| s.relationship_kind == "TRIGGERED")
            .collect();
        assert!(
            triggered.len() >= 2,
            "TRIGGERED should produce one spec per FK owner, got {triggered:?}",
        );
        for spec in &triggered {
            assert!(
                !spec.source_kind.is_empty() && !spec.target_kind.is_empty(),
                "each TRIGGERED spec must pin both endpoint kinds: {spec:?}",
            );
        }
        let kind_pairs: std::collections::BTreeSet<(String, String)> = triggered
            .iter()
            .map(|s| (s.source_kind.clone(), s.target_kind.clone()))
            .collect();
        assert_eq!(
            kind_pairs.len(),
            triggered.len(),
            "TRIGGERED variants must differ by endpoint kind so they don't tombstone each other",
        );
    }

    #[test]
    fn every_spec_targets_a_versioned_table() {
        let prefix = format!("v{}_", *SCHEMA_VERSION);
        for spec in specs() {
            assert!(spec.edge_table.starts_with(&prefix), "{spec:?}");
            assert!(spec.owner_node_table.starts_with(&prefix), "{spec:?}");
        }
    }

    #[test]
    fn sql_pins_kinds_and_compares_other_endpoint() {
        let spec = ReconciliationSpec {
            relationship_kind: "TRIGGERED".to_string(),
            edge_table: "v57_gl_ci_edge".to_string(),
            owner_node_table: "v57_gl_pipeline".to_string(),
            owner_fk_column: "merge_request_id".to_string(),
            source_kind: "MergeRequest".to_string(),
            target_kind: "Pipeline".to_string(),
            owner_id_column: "target_id",
            other_id_column: "source_id",
        };
        let sql = build_reconcile_sql(&spec);

        assert!(sql.contains("INSERT INTO v57_gl_ci_edge"), "{sql}");
        assert!(sql.contains("merge_request_id AS fk"), "{sql}");
        assert!(sql.contains("FROM v57_gl_pipeline FINAL"), "{sql}");
        assert!(sql.contains("_version >= {cursor:String}"), "{sql}");
        assert!(sql.contains("c.id = e.target_id"), "{sql}");
        assert!(sql.contains("e.relationship_kind = 'TRIGGERED'"), "{sql}");
        assert!(sql.contains("e.source_kind = 'MergeRequest'"), "{sql}");
        assert!(sql.contains("e.target_kind = 'Pipeline'"), "{sql}");
        assert!(sql.contains("e.target_id IN (SELECT id FROM c)"), "{sql}");
        assert!(
            sql.contains("e.traversal_path IN (SELECT traversal_path FROM c)"),
            "{sql}"
        );
        assert!(
            sql.contains("(c.fk IS NULL OR e.source_id != c.fk)"),
            "{sql}"
        );
    }
}
