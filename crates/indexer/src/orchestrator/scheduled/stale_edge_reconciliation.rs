use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::Utc;
use ontology::{EdgeMapping, NodeRef, NodeRefKind, Ontology};
use orbit_log_fields::{info, warn};

use crate::checkpoint::{CheckpointStore, NAMESPACE_KEY_PREFIX, namespace_id_from_key};
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};
use orbit_server_config::{ScheduleConfiguration, StaleEdgeReconciliationConfig};

const CHECKPOINT_KEY: &str = "maintenance.stale_edge_reconciliation";

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum StaleWhen {
    EmitterChanged,
    EmitterDeleted,
}

impl StaleWhen {
    fn of(mapping: &EdgeMapping) -> Self {
        if mapping.mutable {
            Self::EmitterChanged
        } else {
            Self::EmitterDeleted
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::EmitterChanged => "emitter_changed",
            Self::EmitterDeleted => "emitter_deleted",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ReconciliationGroup {
    stale_when: StaleWhen,
    emitting_node: String,
    emitting_node_table: String,
    edge_table: String,
    emitting_node_id_column: &'static str,
    emitting_node_kind_column: &'static str,
    relationship_kinds: Vec<String>,
    edge_sort_key: String,
    parent_keys: Option<Vec<(String, String)>>,
}

#[derive(Default)]
struct GroupMappings {
    kinds: Vec<String>,
    parent_keys: Vec<(String, String)>,
    has_unkeyed_parent: bool,
}

/// Tombstones FK edges a node's pipeline stopped emitting, including every FK edge of a deleted
/// node. Stateless: each run scans a fixed lookback rather than resuming from a cursor.
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
        let groups = group_fk_edges_by_emitting_node(ontology);
        let kinds: Vec<&str> = groups
            .iter()
            .flat_map(|group| group.relationship_kinds.iter().map(String::as_str))
            .collect();
        info!(
            safe(ENTITY_COUNT) = groups.len(),
            safe(RELATIONSHIP_KINDS) = kinds.join(","),
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
        let cursor = (Utc::now() - self.config.lookback())
            .format(TIMESTAMP_FORMAT)
            .to_string();

        let mut failed = 0u64;
        for group in &self.groups {
            info!(
                safe(ENTITY) = group.emitting_node.as_str(),
                safe(KIND) = group.stale_when.as_str(),
                safe(SOURCE_TABLE) = group.emitting_node_table.as_str(),
                safe(TABLE) = group.edge_table.as_str(),
                safe(RELATIONSHIP_KINDS) = group.relationship_kinds.join(","),
                safe(WATERMARK) = cursor.as_str(),
                safe(NAMESPACE_COUNT) = pending.len(),
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
                        safe(ENTITY) = group.emitting_node.as_str(),
                        safe(TABLE) = group.edge_table.as_str(),
                        safe(DURATION_S) = elapsed.as_secs_f64(),
                        safe(OUTCOME) = "success",
                        "reconcile completed",
                    );
                }
                Err(error) => {
                    failed += 1;
                    self.metrics.record_error(self.name(), "reconcile");
                    warn!(
                        safe(ENTITY) = group.emitting_node.as_str(),
                        safe(TABLE) = group.edge_table.as_str(),
                        safe(DURATION_S) = elapsed.as_secs_f64(),
                        safe(OUTCOME) = "error",
                        safe(ERROR_TYPE) = "reconcile",
                        expanded(ERROR_MESSAGE) = %error,
                        "reconcile failed",
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
            .with_setting("enable_materialized_cte", "1")
            .execute()
            .await
            .map_err(TaskError::new)
    }

    /// A set cursor means the run is mid-flight, so its edges may not have landed yet.
    async fn find_namespaces_with_pending_writes(&self) -> Result<Vec<i64>, TaskError> {
        let checkpoints = self
            .checkpoint_store
            .load_by_prefix(NAMESPACE_KEY_PREFIX)
            .await
            .map_err(TaskError::new)?;

        let mut ids: Vec<i64> = checkpoints
            .iter()
            .filter(|(_, checkpoint)| checkpoint.is_paging())
            .filter_map(|(key, _)| namespace_id_from_key(key))
            .collect();
        ids.sort_unstable();
        ids.dedup();
        Ok(ids)
    }
}

fn group_fk_edges_by_emitting_node(ontology: &Ontology) -> Vec<ReconciliationGroup> {
    type GroupKey = (StaleWhen, String, String, &'static str);
    let mut mappings_by_group: BTreeMap<GroupKey, GroupMappings> = BTreeMap::new();
    let mut tables_by_emitting_node: BTreeMap<String, String> = BTreeMap::new();

    for node in ontology.nodes() {
        if node.global {
            continue;
        }
        for pipeline in &node.pipelines {
            for mapping in pipeline.transform.edges() {
                let Some(emitting_node_id_column) =
                    find_emitting_node_id_column(mapping, &node.name)
                else {
                    continue;
                };
                tables_by_emitting_node.insert(
                    node.name.clone(),
                    prefixed_table_name(&node.destination_table, *SCHEMA_VERSION),
                );
                let parent_key_column = mapping
                    .array_field
                    .is_none()
                    .then(|| {
                        node.fields.iter().find(|field| {
                            field.column_name() == Some(mapping.source.field.as_str())
                        })
                    })
                    .flatten()
                    .map(|field| field.name.clone());
                for edge_table in ontology
                    .edge_routes_for_mapping(mapping)
                    .into_iter()
                    .map(|edge| edge.destination_table.as_str())
                {
                    let group = mappings_by_group
                        .entry((
                            StaleWhen::of(mapping),
                            node.name.clone(),
                            edge_table.to_string(),
                            emitting_node_id_column,
                        ))
                        .or_default();
                    group.kinds.push(mapping.label.clone());
                    match &parent_key_column {
                        Some(column) => group
                            .parent_keys
                            .push((mapping.label.clone(), column.clone())),
                        None => group.has_unkeyed_parent = true,
                    }
                }
            }
        }
    }

    mappings_by_group
        .into_iter()
        .map(
            |((stale_when, emitting_node, edge_table, emitting_node_id_column), mappings)| {
                let GroupMappings {
                    mut kinds,
                    mut parent_keys,
                    has_unkeyed_parent,
                } = mappings;
                kinds.sort();
                kinds.dedup();
                parent_keys.sort();
                parent_keys.dedup();
                let parent_keys = (stale_when == StaleWhen::EmitterDeleted
                    && emitting_node_id_column == "target_id"
                    && !has_unkeyed_parent)
                    .then_some(parent_keys);
                let edge_sort_key = ontology
                    .edge_table_config(&edge_table)
                    .map(|config| config.sort_key.join(", "))
                    .unwrap_or_default();
                ReconciliationGroup {
                    stale_when,
                    emitting_node_table: tables_by_emitting_node[&emitting_node].clone(),
                    emitting_node,
                    edge_table: prefixed_table_name(&edge_table, *SCHEMA_VERSION),
                    emitting_node_id_column,
                    emitting_node_kind_column: if emitting_node_id_column == "source_id" {
                        "source_kind"
                    } else {
                        "target_kind"
                    },
                    relationship_kinds: kinds,
                    edge_sort_key,
                    parent_keys,
                }
            },
        )
        .collect()
}

/// `None` when neither side binds the emitting node's `id`, so there is no node row to compare against.
fn find_emitting_node_id_column(mapping: &EdgeMapping, node_name: &str) -> Option<&'static str> {
    let binds_emitting_node = |node_ref: &NodeRef| {
        matches!(&node_ref.kind, NodeRefKind::Literal(kind) if kind == node_name)
            && node_ref.field == "id"
    };
    if binds_emitting_node(&mapping.source) {
        Some("source_id")
    } else if binds_emitting_node(&mapping.target) {
        Some("target_id")
    } else {
        None
    }
}

fn build_tombstone_statement(group: &ReconciliationGroup, excluded_namespaces: &[i64]) -> String {
    let ReconciliationGroup {
        stale_when,
        emitting_node_table,
        edge_table,
        emitting_node_id_column,
        emitting_node_kind_column,
        emitting_node,
        relationship_kinds,
        edge_sort_key,
        parent_keys,
    } = group;

    let emitter_state = match stale_when {
        StaleWhen::EmitterChanged => "",
        StaleWhen::EmitterDeleted => " WHERE _deleted = true",
    };

    let kinds = relationship_kinds
        .iter()
        .map(|kind| format!("'{kind}'"))
        .collect::<Vec<_>>()
        .join(", ");

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

    let edge_read = |endpoint_filter: &str| {
        format!(
            "SELECT {edge_sort_key}, _version, _deleted \
             FROM {edge_table} \
             WHERE relationship_kind IN ({kinds}) \
               AND {emitting_node_kind_column} = '{emitting_node}' \
               AND traversal_path IN (SELECT traversal_path FROM emitter) \
               AND {endpoint_filter}"
        )
    };

    let (prior, edge_rows) = match parent_keys {
        None => (
            String::new(),
            edge_read(&format!(
                "{emitting_node_id_column} IN (SELECT id FROM emitter)"
            )),
        ),
        Some(keys) => (
            format!(
                "prior AS MATERIALIZED ( \
                   SELECT traversal_path, id, link.1 AS relationship_kind, link.2 AS foreign_key \
                   FROM {emitting_node_table} ARRAY JOIN [{links}] AS link \
                   WHERE _deleted = false \
                     AND (traversal_path, id) IN (SELECT traversal_path, id FROM emitter) \
                 ), ",
                links = keys
                    .iter()
                    .map(|(kind, column)| format!("('{kind}', {column})"))
                    .collect::<Vec<_>>()
                    .join(", "),
            ),
            format!(
                "SELECT * FROM ({} UNION ALL {})",
                edge_read(
                    "(traversal_path, relationship_kind, source_id, target_id) IN \
                     (SELECT traversal_path, relationship_kind, assumeNotNull(foreign_key), id \
                     FROM prior WHERE foreign_key IS NOT NULL)"
                ),
                edge_read(
                    "target_id IN (SELECT id FROM emitter \
                     WHERE (traversal_path, id) NOT IN (SELECT traversal_path, id FROM prior))"
                ),
            ),
        ),
    };

    format!(
        "INSERT INTO {edge_table} \
           (traversal_path, relationship_kind, source_id, source_kind, target_id, target_kind, _version, _deleted) \
         WITH emitter AS MATERIALIZED ( \
           SELECT id, traversal_path, _version FROM ( \
             SELECT id, traversal_path, _version, _deleted \
             FROM {emitting_node_table} \
             WHERE _version >= {{cursor:String}}{namespace_guard} \
             ORDER BY traversal_path, id, _version DESC \
             LIMIT 1 BY traversal_path, id \
           ){emitter_state} \
         ), \
         {prior}\
         edge AS ( \
           SELECT {edge_sort_key}, _version FROM ( \
             {edge_rows} \
             ORDER BY {edge_sort_key}, _version DESC \
             LIMIT 1 BY {edge_sort_key} \
           ) WHERE _deleted = false \
         ) \
         SELECT edge.traversal_path, edge.relationship_kind, edge.source_id, edge.source_kind, \
                edge.target_id, edge.target_kind, emitter._version - toIntervalMicrosecond(1), true \
         FROM edge \
         JOIN emitter ON emitter.id = edge.{emitting_node_id_column} AND emitter.traversal_path = edge.traversal_path \
         WHERE edge._version < emitter._version"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const EDGE_SORT_KEY_FIXTURE: &str =
        "traversal_path, relationship_kind, source_id, target_id, source_kind, target_kind";

    fn groups() -> Vec<ReconciliationGroup> {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        group_fk_edges_by_emitting_node(&ontology)
    }

    fn find<'a>(
        groups: &'a [ReconciliationGroup],
        stale_when: StaleWhen,
        emitting_node: &str,
        edge_table_suffix: &str,
        endpoint: &str,
    ) -> &'a ReconciliationGroup {
        groups
            .iter()
            .find(|g| {
                g.stale_when == stale_when
                    && g.emitting_node == emitting_node
                    && g.edge_table.ends_with(edge_table_suffix)
                    && g.emitting_node_id_column == endpoint
            })
            .unwrap_or_else(|| {
                panic!(
                    "expected {stale_when:?} group {emitting_node}/*{edge_table_suffix}/{endpoint}"
                )
            })
    }

    fn fixture(stale_when: StaleWhen) -> ReconciliationGroup {
        ReconciliationGroup {
            stale_when,
            emitting_node: "Pipeline".to_string(),
            emitting_node_table: "v57_gl_pipeline".to_string(),
            edge_table: "v57_gl_ci_edge".to_string(),
            emitting_node_id_column: "target_id",
            emitting_node_kind_column: "target_kind",
            relationship_kinds: vec!["TRIGGERED".to_string(), "AUTO_CANCELED_BY".to_string()],
            edge_sort_key: EDGE_SORT_KEY_FIXTURE.to_string(),
            parent_keys: None,
        }
    }

    #[test]
    fn outgoing_edge_is_emitted_from_source_endpoint() {
        let groups = groups();
        let diff = find(
            &groups,
            StaleWhen::EmitterChanged,
            "MergeRequest",
            "gl_diff_edge",
            "source_id",
        );
        assert_eq!(diff.emitting_node_kind_column, "source_kind");
        assert!(
            diff.relationship_kinds
                .contains(&"HAS_LATEST_DIFF".to_string())
        );
    }

    #[test]
    fn incoming_edge_is_emitted_from_target_endpoint() {
        let groups = groups();
        let incoming = find(
            &groups,
            StaleWhen::EmitterChanged,
            "MergeRequest",
            "gl_edge",
            "target_id",
        );
        assert_eq!(incoming.emitting_node_kind_column, "target_kind");
        assert!(
            incoming
                .relationship_kinds
                .contains(&"LAST_EDITED_BY".to_string())
        );
    }

    #[test]
    fn kinds_sharing_emitting_node_and_table_collapse_into_one_group() {
        let groups = groups();
        let incoming = find(
            &groups,
            StaleWhen::EmitterChanged,
            "MergeRequest",
            "gl_edge",
            "target_id",
        );
        assert!(
            incoming.relationship_kinds.len() > 1,
            "UPDATED_BY and LAST_EDITED_BY share an emitting node, table and endpoint so they \
             must batch into one statement: {incoming:?}",
        );
    }

    #[test]
    fn only_mutable_mappings_are_swept_on_change() {
        let groups = groups();
        let swept_on_change: std::collections::BTreeSet<(&str, &str)> = groups
            .iter()
            .filter(|g| g.stale_when == StaleWhen::EmitterChanged)
            .flat_map(|g| {
                g.relationship_kinds
                    .iter()
                    .map(|kind| (g.emitting_node.as_str(), kind.as_str()))
            })
            .collect();
        for immutable in [
            ("MergeRequest", "IN_PROJECT"),
            ("Job", "IN_PROJECT"),
            ("Job", "HAS_JOB"),
            ("Job", "IN_PIPELINE"),
            ("MergeRequest", "MERGED_AT_COMMIT"),
            ("VulnerabilityOccurrence", "HAS_IDENTIFIER"),
        ] {
            assert!(
                !swept_on_change.contains(&immutable),
                "{immutable:?} is not marked mutable in the ontology and must not be swept on change",
            );
        }
    }

    #[test]
    fn every_fk_mapping_is_swept_when_its_emitter_is_deleted() {
        let ontology = Ontology::load_embedded().expect("ontology must load");
        let groups = group_fk_edges_by_emitting_node(&ontology);
        let mut checked = 0;
        for node in ontology.nodes().filter(|node| !node.global) {
            for mapping in node.pipelines.iter().flat_map(|p| p.transform.edges()) {
                let Some(endpoint) = find_emitting_node_id_column(mapping, &node.name) else {
                    continue;
                };
                for route in ontology.edge_routes_for_mapping(mapping) {
                    let swept = groups.iter().any(|g| {
                        g.emitting_node == node.name
                            && g.edge_table
                                == prefixed_table_name(&route.destination_table, *SCHEMA_VERSION)
                            && g.emitting_node_id_column == endpoint
                            && g.relationship_kinds.contains(&mapping.label)
                    });
                    assert!(swept, "{}/{} is never swept", node.name, mapping.label);
                    checked += 1;
                }
            }
        }
        assert!(checked > 50, "only {checked} FK routes were checked");
    }

    #[test]
    fn immutable_fk_edges_are_swept_for_deleted_emitters() {
        let groups = groups();
        let job_incoming = find(
            &groups,
            StaleWhen::EmitterDeleted,
            "Job",
            "gl_ci_edge",
            "target_id",
        );
        assert!(
            job_incoming
                .relationship_kinds
                .contains(&"HAS_JOB".to_string())
        );
        let mr_outgoing = find(
            &groups,
            StaleWhen::EmitterDeleted,
            "MergeRequest",
            "gl_edge",
            "source_id",
        );
        assert!(
            mr_outgoing
                .relationship_kinds
                .contains(&"IN_PROJECT".to_string())
        );
    }

    #[test]
    fn polymorphic_endpoint_has_no_emitting_node_row_to_compare() {
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
        assert_eq!(
            find_emitting_node_id_column(&polymorphic, "MergeRequest"),
            None
        );
    }

    #[test]
    fn every_group_targets_versioned_tables() {
        let prefix = format!("v{}_", *SCHEMA_VERSION);
        for group in groups() {
            assert!(group.edge_table.starts_with(&prefix), "{group:?}");
            assert!(group.emitting_node_table.starts_with(&prefix), "{group:?}");
        }
    }

    #[test]
    fn sql_pins_emitting_node_kind_and_batches_every_relationship_kind() {
        let sql = build_tombstone_statement(&fixture(StaleWhen::EmitterChanged), &[]);

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
    fn staleness_compares_versions_and_stamps_from_the_emitting_node() {
        for stale_when in [StaleWhen::EmitterChanged, StaleWhen::EmitterDeleted] {
            let sql = build_tombstone_statement(&fixture(stale_when), &[]);

            assert!(sql.contains("edge._version < emitter._version"), "{sql}");
            assert!(sql.contains("target_kind, _version, _deleted)"), "{sql}");
            assert!(
                sql.contains("emitter._version - toIntervalMicrosecond(1), true"),
                "{sql}"
            );
            assert!(!sql.contains("now64"), "{sql}");
        }
    }

    #[test]
    fn emitter_window_is_read_once_per_statement() {
        for stale_when in [StaleWhen::EmitterChanged, StaleWhen::EmitterDeleted] {
            let sql = build_tombstone_statement(&fixture(stale_when), &[]);

            assert!(sql.contains("WITH emitter AS MATERIALIZED ("), "{sql}");
            assert_eq!(sql.matches("FROM v57_gl_pipeline").count(), 1, "{sql}");
            assert!(
                sql.contains("SELECT id, traversal_path, _version FROM ("),
                "{sql}"
            );
        }
    }

    #[test]
    fn both_sides_dedup_without_final_and_filter_deleted_after() {
        for stale_when in [StaleWhen::EmitterChanged, StaleWhen::EmitterDeleted] {
            let sql = build_tombstone_statement(&fixture(stale_when), &[]);

            assert!(!sql.contains("FINAL"), "{sql}");
            assert_eq!(sql.matches("LIMIT 1 BY").count(), 2, "{sql}");

            // A `_deleted` filter inside either dedup would drop a tombstoned newest
            // row and let a superseded one win, which is how valid edges get retired.
            let emitter_scan = &sql[sql.find("FROM v57_gl_pipeline").unwrap()..];
            let emitter_dedup = emitter_scan.find("LIMIT 1 BY traversal_path, id").unwrap();
            assert!(
                !emitter_scan[..emitter_dedup].contains("_deleted ="),
                "{sql}"
            );
            let edge_dedup = sql
                .find("LIMIT 1 BY traversal_path, relationship_kind")
                .unwrap_or_else(|| panic!("edge dedup missing: {sql}"));
            assert!(sql[edge_dedup..].contains("_deleted = false"), "{sql}");
        }
    }

    #[test]
    fn changed_emitters_include_deleted_ones() {
        let sql = build_tombstone_statement(&fixture(StaleWhen::EmitterChanged), &[]);
        let emitter = &sql[..sql.find("edge AS").unwrap()];
        assert!(!emitter.contains("_deleted = false"), "{sql}");
        assert!(!emitter.contains("_deleted = true"), "{sql}");
    }

    #[test]
    fn deleted_emitters_keep_only_a_deleted_latest_row() {
        let sql = build_tombstone_statement(&fixture(StaleWhen::EmitterDeleted), &[]);
        let emitter = &sql[..sql.find("edge AS").unwrap()];
        let dedup = emitter.find("LIMIT 1 BY traversal_path, id").unwrap();
        assert!(
            emitter[dedup..].contains(") WHERE _deleted = true"),
            "{sql}"
        );
    }

    #[test]
    fn target_keyed_edges_are_found_through_the_emitters_live_foreign_keys() {
        let groups = groups();
        for (node, parent_keys) in [
            (
                "Job",
                "('HAS_JOB', pipeline_id), ('HAS_JOB', stage_id), ('TRIGGERED', user_id)",
            ),
            (
                "Pipeline",
                "('TRIGGERED', merge_request_id), ('TRIGGERED', user_id)",
            ),
            ("Stage", "('HAS_STAGE', pipeline_id)"),
        ] {
            let group = find(
                &groups,
                StaleWhen::EmitterDeleted,
                node,
                "gl_ci_edge",
                "target_id",
            );
            let sql = build_tombstone_statement(group, &[]);

            assert!(
                sql.contains(&format!(
                    "prior AS MATERIALIZED ( \
                     SELECT traversal_path, id, link.1 AS relationship_kind, link.2 AS foreign_key \
                     FROM v{}_gl_{} ARRAY JOIN [{parent_keys}] AS link",
                    *SCHEMA_VERSION,
                    node.to_lowercase(),
                )),
                "{sql}"
            );
            assert!(
                sql.contains(
                    "WHERE _deleted = false \
                     AND (traversal_path, id) IN (SELECT traversal_path, id FROM emitter)"
                ),
                "{sql}"
            );
            assert!(
                sql.contains(
                    "AND (traversal_path, relationship_kind, source_id, target_id) IN \
                     (SELECT traversal_path, relationship_kind, assumeNotNull(foreign_key), id \
                     FROM prior WHERE foreign_key IS NOT NULL)"
                ),
                "{sql}"
            );
            assert!(
                sql.contains(
                    "AND target_id IN (SELECT id FROM emitter \
                     WHERE (traversal_path, id) NOT IN (SELECT traversal_path, id FROM prior))"
                ),
                "{sql}"
            );
            assert!(
                sql.contains("JOIN emitter ON emitter.id = edge.target_id"),
                "{sql}"
            );
            assert!(
                !sql.contains("AND target_id IN (SELECT id FROM emitter) "),
                "{sql}"
            );
        }
    }

    #[test]
    fn changed_keys_keep_the_target_read_because_merges_drop_old_key_values() {
        let groups = groups();
        for (node, table) in [("Note", "gl_edge"), ("MergeRequest", "gl_edge")] {
            let group = find(&groups, StaleWhen::EmitterChanged, node, table, "target_id");
            let sql = build_tombstone_statement(group, &[]);
            assert!(!sql.contains("prior AS"), "{sql}");
            assert!(
                sql.contains("AND target_id IN (SELECT id FROM emitter) "),
                "{sql}"
            );
        }
    }

    #[test]
    fn array_valued_parent_keys_keep_the_target_read() {
        let groups = groups();
        let group = find(
            &groups,
            StaleWhen::EmitterDeleted,
            "MergeRequest",
            "gl_edge",
            "target_id",
        );
        assert!(group.relationship_kinds.contains(&"REVIEWER".to_string()));
        let sql = build_tombstone_statement(group, &[]);
        assert!(!sql.contains("prior AS"), "{sql}");
        assert!(
            sql.contains("AND target_id IN (SELECT id FROM emitter) "),
            "{sql}"
        );
    }

    #[test]
    fn source_keyed_edges_keep_the_emitter_id_read() {
        let groups = groups();
        let group = find(
            &groups,
            StaleWhen::EmitterDeleted,
            "Job",
            "gl_ci_edge",
            "source_id",
        );
        let sql = build_tombstone_statement(group, &[]);
        assert!(!sql.contains("prior AS"), "{sql}");
        assert!(
            sql.contains("AND source_id IN (SELECT id FROM emitter) "),
            "{sql}"
        );
    }

    #[test]
    fn namespaces_with_pending_writes_are_excluded_from_the_scan() {
        for stale_when in [StaleWhen::EmitterChanged, StaleWhen::EmitterDeleted] {
            let sql = build_tombstone_statement(&fixture(stale_when), &[7, 42]);
            assert!(sql.contains("NOT IN (7, 42)"), "{sql}");

            let clean = build_tombstone_statement(&fixture(stale_when), &[]);
            assert!(!clean.contains("NOT IN ("), "{clean}");
        }
    }
}
