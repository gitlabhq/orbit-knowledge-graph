use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use ontology::{EdgeDirection, EdgeMapping, EdgeTarget, EtlScope, NodeEntity, Ontology};
use tracing::{info, warn};

use crate::checkpoint::CheckpointStore;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::scheduler::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use gkg_server_config::{ScheduleConfiguration, StaleEdgeReconciliationConfig};

const CHECKPOINT_KEY: &str = "maintenance.stale_edge_reconciliation";

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReconciliationSpec {
    relationship_kind: String,
    edge_table: String,
    owner_node_table: String,
    /// Graph node-table column holding the current FK value; can differ from the
    /// edge-map (extract) key, e.g. `closed_by_id` vs `metric_latest_closed_by_id`.
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
/// The watermark cursor advances only on full success; re-tombstoning is a
/// no-op, so a failed run just widens the next window.
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
        let specs = reconciliation_specs(ontology);
        let kinds: Vec<&str> = specs
            .iter()
            .map(|spec| spec.relationship_kind.as_str())
            .collect();
        info!(
            specs = specs.len(),
            ?kinds,
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
        // Capture before reading so rows written mid-run are caught next time, not lost.
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
            .execute()
            .await
            .map_err(TaskError::new)
    }
}

fn reconciliation_specs(ontology: &Ontology) -> Vec<ReconciliationSpec> {
    let mut specs = Vec::new();
    for node in ontology.nodes() {
        let Some(etl) = &node.etl else { continue };
        // Global owners lack a traversal_path, so the dual-IN PK prune can't apply;
        // measured staleness is all on namespaced entities (MR, WorkItem, Pipeline).
        if etl.scope() != EtlScope::Namespaced {
            continue;
        }
        for (fk_extract_column, mapping) in etl.edge_mappings() {
            if let Some(spec) = reconciliation_spec(ontology, node, fk_extract_column, mapping) {
                specs.push(spec);
            }
        }
    }
    specs
}

fn is_scalar_edge(mapping: &EdgeMapping) -> bool {
    mapping.delimiter.is_none() && mapping.array_field.is_none() && !mapping.array
}

fn reconciliation_spec(
    ontology: &Ontology,
    node: &NodeEntity,
    fk_extract_column: &str,
    mapping: &EdgeMapping,
) -> Option<ReconciliationSpec> {
    if !mapping.mutable {
        return None;
    }
    if !is_scalar_edge(mapping) {
        return None;
    }
    let EdgeTarget::Literal(other_kind) = &mapping.target else {
        return None;
    };
    // No stored field for this extract column means the current FK isn't queryable — skip.
    let owner_fk_column = node
        .fields
        .iter()
        .find(|field| field.column_name() == Some(fk_extract_column))
        .map(|field| field.name.clone())?;

    let edge_table = prefixed_table_name(
        ontology.edge_table_for_relationship(&mapping.relationship_kind),
        *SCHEMA_VERSION,
    );
    let owner_node_table = prefixed_table_name(&node.destination_table, *SCHEMA_VERSION);

    let (source_kind, target_kind, owner_id_column, other_id_column) = match mapping.direction {
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

    Some(ReconciliationSpec {
        relationship_kind: mapping.relationship_kind.clone(),
        edge_table,
        owner_node_table,
        owner_fk_column,
        source_kind,
        target_kind,
        owner_id_column,
        other_id_column,
    })
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
         WITH owner AS ( \
           SELECT id, traversal_path, {owner_fk_column} AS current_fk \
           FROM {owner_node_table} FINAL \
           WHERE _version >= {{cursor:String}} \
         ) \
         SELECT edge.traversal_path, edge.relationship_kind, edge.source_id, edge.source_kind, \
                edge.target_id, edge.target_kind, true \
         FROM {edge_table} edge \
         JOIN owner ON owner.id = edge.{owner_id_column} AND owner.traversal_path = edge.traversal_path \
         WHERE edge.relationship_kind = '{relationship_kind}' \
           AND edge.source_kind = '{source_kind}' \
           AND edge.target_kind = '{target_kind}' \
           AND edge._deleted = false \
           AND edge.traversal_path IN (SELECT traversal_path FROM owner) \
           AND edge.{owner_id_column} IN (SELECT id FROM owner) \
           AND (owner.current_fk IS NULL OR edge.{other_id_column} != owner.current_fk)"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn specs() -> Vec<ReconciliationSpec> {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        reconciliation_specs(&ontology)
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

    fn mapping(relationship_kind: &str) -> EdgeMapping {
        EdgeMapping {
            target: EdgeTarget::Literal("User".to_string()),
            relationship_kind: relationship_kind.to_string(),
            direction: EdgeDirection::Incoming,
            delimiter: None,
            array_field: None,
            array: false,
            mutable: true,
        }
    }

    fn merge_request_node(ontology: &Ontology) -> &NodeEntity {
        ontology
            .get_node("MergeRequest")
            .expect("MergeRequest node")
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
    fn derives_exactly_the_mutable_kinds_from_the_ontology() {
        let specs = specs();
        let kinds: std::collections::BTreeSet<&str> =
            specs.iter().map(|s| s.relationship_kind.as_str()).collect();
        assert_eq!(
            kinds,
            [
                "CLOSED",
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
    fn resolves_renamed_graph_column() {
        let specs = specs();
        let closed = find(&specs, "CLOSED", "gl_merge_request");
        assert_eq!(closed.owner_fk_column, "closed_by_id");
    }

    #[test]
    fn immutable_fk_kinds_are_not_marked_mutable() {
        let specs = specs();
        for immutable in [
            "IN_PROJECT",
            "AUTHORED",
            "HAS_JOB",
            "IN_PIPELINE",
            "UPDATED_BY",
        ] {
            assert!(
                !specs.iter().any(|s| s.relationship_kind == immutable),
                "{immutable} is not marked mutable in the ontology and must not be swept",
            );
        }
    }

    #[test]
    fn non_mutable_edge_is_skipped() {
        let ontology = Ontology::load_embedded().unwrap();
        let immutable = EdgeMapping {
            mutable: false,
            ..mapping("AUTHORED")
        };
        assert!(
            reconciliation_spec(
                &ontology,
                merge_request_node(&ontology),
                "author_id",
                &immutable
            )
            .is_none()
        );
    }

    #[test]
    fn mutable_array_edge_is_skipped() {
        let ontology = Ontology::load_embedded().unwrap();
        let array_edge = EdgeMapping {
            array_field: Some("user_id".to_string()),
            ..mapping("REVIEWER")
        };
        assert!(
            reconciliation_spec(
                &ontology,
                merge_request_node(&ontology),
                "reviewers",
                &array_edge
            )
            .is_none()
        );
    }

    #[test]
    fn mutable_polymorphic_edge_is_skipped() {
        let ontology = Ontology::load_embedded().unwrap();
        let polymorphic = EdgeMapping {
            target: EdgeTarget::Column {
                column: "noteable_type".to_string(),
                type_mapping: Default::default(),
            },
            ..mapping("HAS_NOTE")
        };
        assert!(
            reconciliation_spec(
                &ontology,
                merge_request_node(&ontology),
                "noteable_id",
                &polymorphic
            )
            .is_none()
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
        assert!(sql.contains("v57_gl_pipeline FINAL"), "{sql}");
        assert!(sql.contains("_version >= {cursor:String}"), "{sql}");

        assert!(sql.contains("relationship_kind = 'TRIGGERED'"), "{sql}");
        assert!(sql.contains("source_kind = 'MergeRequest'"), "{sql}");
        assert!(sql.contains("target_kind = 'Pipeline'"), "{sql}");

        assert!(sql.contains("source_id != "), "{sql}");
        assert!(!sql.contains("target_id != "), "{sql}");
        assert!(sql.contains("IS NULL OR"), "{sql}");
    }
}
