//! One-shot batch sweep of stale code rows after the backfill drains.
//!
//! Per-task stale cleanup is skipped on first-time indexing, so rows left
//! behind by runs that died before checkpointing survive a backfill. This
//! sweep tombstones them in one batch statement per code table, keyed on
//! each project's code-indexing checkpoint watermark, the first time a
//! backfill sweep tick finds nothing left to dispatch. A maintenance
//! checkpoint makes it run once per schema version.

use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use futures::future::try_join_all;
use tracing::{debug, info};

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

        let mut statements: Vec<(String, String)> = table_names
            .node_tables()
            .iter()
            .map(|table| (table.to_string(), node_sweep(table, &checkpoint_table)))
            .collect();
        statements.extend(
            table_names
                .edge_table_names()
                .iter()
                .map(|table| (table.to_string(), edge_sweep(table, &checkpoint_table))),
        );

        Self {
            graph,
            checkpoint_store,
            statements,
        }
    }

    pub async fn run_after_drain(&self, outcome: &DispatchOutcome) -> Result<(), TaskError> {
        if outcome.dispatched != 0 || outcome.skipped != 0 {
            debug!(
                dispatched = outcome.dispatched,
                skipped = outcome.skipped,
                "post-backfill stale sweep deferred: backfill not yet drained"
            );
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
        try_join_all(self.statements.iter().map(|(table, sql)| async move {
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
            Ok::<(), TaskError>(())
        }))
        .await?;

        self.checkpoint_store
            .save_completed(CHECKPOINT_KEY, &started, WriteDurability::Durable)
            .await
            .map_err(TaskError::new)?;
        info!("post-backfill stale sweep complete");
        Ok(())
    }
}

// Every sweep scans raw parts, never FINAL: a tombstone at `watermark - 1µs`
// is outranked by any surviving row versioned at or after the watermark, so
// superseded-but-live ids can only receive no-op tombstones that the next
// merge collapses.
fn node_sweep(table: &str, checkpoint_table: &str) -> String {
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

fn edge_sweep(edge_table: &str, checkpoint_table: &str) -> String {
    // gl_code_edge carries project_id + branch, so it joins the checkpoint
    // table exactly like a node table.
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

    let code_source_kinds = CodeTableNames::NODE_KINDS
        .map(|kind| format!("'{kind}'"))
        .join(", ");

    // The shared edge table has no project_id/branch columns. Code-written
    // rows are identified by their source kind, and the watermark comes from
    // a per-traversal-path aggregate: min() so a second checkpointed branch
    // under the same path can never out-version another branch's live rows.
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
            SELECT traversal_path, min(indexed_at) AS watermark
            FROM {checkpoint_table} FINAL
            WHERE _deleted = false
            GROUP BY traversal_path
        ) AS w ON w.traversal_path = s.traversal_path
        WHERE s.source_kind IN ({code_source_kinds})
          AND s._version < w.watermark
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
        let sql = node_sweep("v9_gl_file", "v9_code_indexing_checkpoint");
        assert!(sql.contains("FROM v9_gl_file AS s"), "{sql}");
        assert!(!sql.contains("v9_gl_file AS s FINAL"), "{sql}");
        assert!(
            sql.contains("v9_code_indexing_checkpoint AS cp FINAL"),
            "{sql}"
        );
        assert!(sql.contains("s._version < cp.indexed_at"), "{sql}");
        assert!(sql.contains("cp._deleted = false"), "{sql}");
    }

    #[test]
    fn code_edge_sweep_joins_checkpoint_directly() {
        let sql = edge_sweep("v9_gl_code_edge", "v9_cp");
        assert!(sql.contains("cp.project_id = s.project_id"), "{sql}");
        assert!(!sql.contains("source_kind IN"), "{sql}");
    }

    #[test]
    fn plain_edge_sweep_scopes_by_source_kind_and_min_watermark() {
        let sql = edge_sweep("v9_gl_edge", "v9_cp");
        assert!(
            sql.contains("s.source_kind IN ('Directory', 'File', 'Definition', 'ImportedSymbol')"),
            "{sql}"
        );
        assert!(
            sql.contains("min(indexed_at)"),
            "a shared traversal_path must take the oldest branch watermark: {sql}"
        );
        assert!(sql.contains("s._version < w.watermark"), "{sql}");
        assert!(
            !sql.contains("UNION ALL"),
            "the shared edge sweep must not scan node tables: {sql}"
        );
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
        assert!(
            tables[0].contains("gl_") && !tables[0].contains("edge"),
            "first sweep statement must target a node table, got: {tables:?}"
        );
        assert!(
            tables.last().unwrap().contains("edge"),
            "edge sweeps must come after node sweeps: {tables:?}"
        );
    }
}
