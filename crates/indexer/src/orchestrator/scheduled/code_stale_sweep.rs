//! One-shot batch sweep of stale code rows after the backfill drains.
//!
//! Per-task stale cleanup is skipped on first-time indexing, so rows left
//! behind by runs that died before checkpointing survive a backfill. This
//! sweep tombstones them in one batch statement per code table, keyed on
//! each project's checkpoint watermark, the first time a backfill sweep
//! tick finds nothing left to dispatch. A maintenance checkpoint in the
//! version-prefixed checkpoint table makes it run once per schema version.

use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use tracing::info;

use crate::checkpoint::CheckpointStore;
use crate::clickhouse::ArrowClickHouseClient;
use crate::durability::WriteDurability;
use crate::modules::code::config::CodeTableNames;
use crate::orchestrator::dispatch::DispatchOutcome;
use crate::orchestrator::scheduled::TaskError;
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};

const CHECKPOINT_KEY: &str = "maintenance.code_stale_sweep";

const CODE_INDEXING_CHECKPOINT_TABLE: &str = "code_indexing_checkpoint";

pub struct CodeStaleSweep {
    graph: ArrowClickHouseClient,
    checkpoint_store: Arc<dyn CheckpointStore>,
    statements: Vec<(String, String)>,
}

impl CodeStaleSweep {
    pub fn new(
        graph: ArrowClickHouseClient,
        table_names: &CodeTableNames,
        checkpoint_store: Arc<dyn CheckpointStore>,
    ) -> Self {
        let checkpoint_table = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let node_tables = table_names.node_tables();

        let mut statements: Vec<(String, String)> = node_tables
            .iter()
            .map(|table| {
                (
                    table.to_string(),
                    build_node_sweep(table, &checkpoint_table),
                )
            })
            .collect();
        statements.extend(table_names.edge_table_names().iter().map(|table| {
            (
                table.to_string(),
                build_edge_sweep(table, &node_tables, &checkpoint_table),
            )
        }));

        Self {
            graph,
            checkpoint_store,
            statements,
        }
    }

    pub async fn run_after_drain(&self, outcome: &DispatchOutcome) -> Result<(), TaskError> {
        if outcome.dispatched != 0 || outcome.skipped != 0 {
            return Ok(());
        }
        if self
            .checkpoint_store
            .load(CHECKPOINT_KEY)
            .await
            .map_err(TaskError::new)?
            .is_some()
        {
            return Ok(());
        }

        let started = Utc::now();
        for (table, sql) in &self.statements {
            let statement_start = Instant::now();
            self.graph
                .query(sql)
                .execute()
                .await
                .map_err(|e| TaskError::new(format!("stale sweep on {table}: {e}")))?;
            info!(
                table,
                duration_ms = statement_start.elapsed().as_millis() as u64,
                "post-backfill stale sweep statement complete"
            );
        }

        self.checkpoint_store
            .save_completed(CHECKPOINT_KEY, &started, WriteDurability::Durable)
            .await
            .map_err(TaskError::new)?;
        info!("post-backfill stale sweep complete");
        Ok(())
    }
}

// The scans skip FINAL: a tombstone at `indexed_at - 1µs` is outranked by any
// surviving row versioned at or after the watermark, so reading raw parts can
// only produce no-op tombstones for ids that are still live.
fn build_node_sweep(table: &str, checkpoint_table: &str) -> String {
    format!(
        r#"
        INSERT INTO {table} (traversal_path, project_id, branch, id, _version, _deleted)
        SELECT
            s.traversal_path,
            s.project_id,
            s.branch,
            s.id,
            cp.indexed_at - toIntervalMicrosecond(1) AS _version,
            true AS _deleted
        FROM {table} AS s
        INNER JOIN {checkpoint_table} AS cp FINAL
            ON cp.traversal_path = s.traversal_path
           AND cp.project_id = s.project_id
           AND cp.branch = s.branch
        WHERE cp._deleted = false
          AND s._version < cp.indexed_at
        "#
    )
}

fn build_edge_sweep(edge_table: &str, node_tables: &[&str], checkpoint_table: &str) -> String {
    if edge_table.contains("code_edge") {
        return format!(
            r#"
            INSERT INTO {edge_table}
                (traversal_path, project_id, branch, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted)
            SELECT
                s.traversal_path,
                s.project_id,
                s.branch,
                s.source_id,
                s.source_kind,
                s.relationship_kind,
                s.target_id,
                s.target_kind,
                cp.indexed_at - toIntervalMicrosecond(1) AS _version,
                true AS _deleted
            FROM {edge_table} AS s
            INNER JOIN {checkpoint_table} AS cp FINAL
                ON cp.traversal_path = s.traversal_path
               AND cp.project_id = s.project_id
               AND cp.branch = s.branch
            WHERE cp._deleted = false
              AND s._version < cp.indexed_at
            "#
        );
    }

    let node_union = node_tables
        .iter()
        .map(|t| format!("SELECT traversal_path, project_id, branch, id FROM {t}"))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");

    format!(
        r#"
        INSERT INTO {edge_table}
            (traversal_path, source_id, source_kind, relationship_kind, target_id, target_kind, _version, _deleted)
        SELECT
            s.traversal_path,
            s.source_id,
            s.source_kind,
            s.relationship_kind,
            s.target_id,
            s.target_kind,
            w.watermark - toIntervalMicrosecond(1) AS _version,
            true AS _deleted
        FROM {edge_table} AS s
        INNER JOIN (
            SELECT DISTINCT
                n.traversal_path,
                n.id,
                cp.indexed_at AS watermark
            FROM ({node_union}) AS n
            INNER JOIN {checkpoint_table} AS cp FINAL
                ON cp.traversal_path = n.traversal_path
               AND cp.project_id = n.project_id
               AND cp.branch = n.branch
            WHERE cp._deleted = false
        ) AS w
            ON w.traversal_path = s.traversal_path
           AND w.id = s.source_id
        WHERE s._version < w.watermark
        "#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_names() -> CodeTableNames {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        CodeTableNames::from_ontology(&ontology).expect("code tables must resolve")
    }

    #[test]
    fn node_sweep_joins_checkpoint_watermark_without_final_scan() {
        let sql = build_node_sweep("v9_gl_file", "v9_code_indexing_checkpoint");
        assert!(sql.contains("FROM v9_gl_file AS s"), "{sql}");
        assert!(!sql.contains("v9_gl_file FINAL"), "{sql}");
        assert!(
            sql.contains("v9_code_indexing_checkpoint AS cp FINAL"),
            "{sql}"
        );
        assert!(sql.contains("s._version < cp.indexed_at"), "{sql}");
        assert!(sql.contains("cp._deleted = false"), "{sql}");
    }

    #[test]
    fn code_edge_sweep_joins_checkpoint_directly() {
        let sql = build_edge_sweep("v9_gl_code_edge", &["v9_gl_file"], "v9_cp");
        assert!(sql.contains("cp.project_id = s.project_id"), "{sql}");
        assert!(!sql.contains("UNION ALL"), "{sql}");
    }

    #[test]
    fn plain_edge_sweep_maps_source_ids_through_node_tables() {
        let sql = build_edge_sweep("v9_gl_edge", &["v9_gl_directory", "v9_gl_file"], "v9_cp");
        assert!(sql.contains("UNION ALL"), "{sql}");
        assert!(sql.contains("w.id = s.source_id"), "{sql}");
        assert!(sql.contains("s._version < w.watermark"), "{sql}");
    }

    #[test]
    fn statements_cover_every_code_table_nodes_first() {
        let names = table_names();
        let graph = ArrowClickHouseClient::new(
            "http://localhost:0",
            "default",
            "default",
            None,
            &Default::default(),
            &Default::default(),
        );
        let store = Arc::new(crate::checkpoint::ClickHouseCheckpointStore::new(Arc::new(
            graph.clone(),
        )));
        let sweep = CodeStaleSweep::new(graph, &names, store);
        let tables: Vec<&str> = sweep.statements.iter().map(|(t, _)| t.as_str()).collect();
        assert_eq!(
            tables.len(),
            names.node_tables().len() + names.edge_table_names().len()
        );
        assert!(tables[0].contains("gl_"), "{tables:?}");
        assert!(
            tables.last().unwrap().contains("edge"),
            "edge sweeps must come after node sweeps: {tables:?}"
        );
    }
}
