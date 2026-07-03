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
use std::collections::HashMap;

use query_engine::compiler::{
    Expr, JoinType, ParamValue, Query, SelectExpr, TableRef, emit_simple_query,
};
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

const NODE_COLUMNS: [&str; 4] = ["traversal_path", "project_id", "branch", "id"];
const EDGE_COLUMNS: [&str; 6] = [
    "traversal_path",
    "source_id",
    "source_kind",
    "relationship_kind",
    "target_id",
    "target_kind",
];

struct SweepStatement {
    table: String,
    sql: String,
    params: HashMap<String, ParamValue>,
}

pub struct CodeStaleSweep {
    graph: ArrowClickHouseClient,
    checkpoint_store: Arc<dyn CheckpointStore>,
    statements: Vec<SweepStatement>,
}

impl CodeStaleSweep {
    pub fn new(
        graph: ArrowClickHouseClient,
        table_names: &CodeTableNames,
        checkpoint_store: Arc<dyn CheckpointStore>,
    ) -> Self {
        let checkpoint_table = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, *SCHEMA_VERSION);

        let mut statements: Vec<SweepStatement> = table_names
            .node_tables()
            .iter()
            .map(|table| node_sweep(table, &checkpoint_table))
            .collect();
        statements.extend(
            table_names
                .edge_table_names()
                .iter()
                .map(|table| edge_sweep(table, &checkpoint_table)),
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
        try_join_all(self.statements.iter().map(|statement| async move {
            let statement_start = Instant::now();
            let mut query = self.graph.query(&statement.sql);
            for (key, param) in &statement.params {
                query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.ch_type);
            }
            query
                .execute()
                .await
                .map_err(|e| TaskError::new(format!("stale sweep on {}: {e}", statement.table)))?;
            info!(
                table = statement.table,
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
fn node_sweep(table: &str, checkpoint_table: &str) -> SweepStatement {
    let select = Query {
        select: passthrough_columns(&NODE_COLUMNS)
            .chain(tombstone_columns(Expr::col("cp", "indexed_at")))
            .collect(),
        from: TableRef::join(
            JoinType::Inner,
            TableRef::scan(table, "s"),
            TableRef::scan_final(checkpoint_table, "cp"),
            checkpoint_join_on(),
        ),
        where_clause: Some(Expr::and(
            Expr::eq(Expr::col("cp", "_deleted"), Expr::lit(false)),
            Expr::binary(
                query_engine::compiler::Op::Lt,
                Expr::col("s", "_version"),
                Expr::col("cp", "indexed_at"),
            ),
        )),
        ..Query::default()
    };
    insert_from(table, &NODE_COLUMNS, &select)
}

fn edge_sweep(edge_table: &str, checkpoint_table: &str) -> SweepStatement {
    // gl_code_edge carries project_id + branch, so it joins the checkpoint
    // table exactly like a node table.
    if edge_table.contains("code_edge") {
        let columns: Vec<&str> = ["traversal_path", "project_id", "branch"]
            .into_iter()
            .chain(EDGE_COLUMNS.into_iter().skip(1))
            .collect();
        let select = Query {
            select: passthrough_columns(&columns)
                .chain(tombstone_columns(Expr::col("cp", "indexed_at")))
                .collect(),
            from: TableRef::join(
                JoinType::Inner,
                TableRef::scan(edge_table, "s"),
                TableRef::scan_final(checkpoint_table, "cp"),
                checkpoint_join_on(),
            ),
            where_clause: Some(Expr::and(
                Expr::eq(Expr::col("cp", "_deleted"), Expr::lit(false)),
                Expr::binary(
                    query_engine::compiler::Op::Lt,
                    Expr::col("s", "_version"),
                    Expr::col("cp", "indexed_at"),
                ),
            )),
            ..Query::default()
        };
        return insert_from(edge_table, &columns, &select);
    }

    // The shared edge table has no project_id/branch columns. Code-written
    // rows are identified by their source kind, and the watermark comes from
    // a per-traversal-path aggregate: min() so a second checkpointed branch
    // under the same path can never out-version another branch's live rows.
    let watermarks = Query {
        select: vec![
            SelectExpr::col("cp", "traversal_path"),
            SelectExpr::new(
                Expr::func("min", vec![Expr::col("cp", "indexed_at")]),
                "watermark",
            ),
        ],
        from: TableRef::scan_final(checkpoint_table, "cp"),
        where_clause: Some(Expr::eq(Expr::col("cp", "_deleted"), Expr::lit(false))),
        group_by: vec![Expr::col("cp", "traversal_path")],
        ..Query::default()
    };

    let code_source_kinds = Expr::or_all(
        CodeTableNames::NODE_KINDS
            .iter()
            .map(|kind| Some(Expr::eq(Expr::col("s", "source_kind"), Expr::lit(*kind)))),
    )
    .expect("NODE_KINDS is never empty");

    let select = Query {
        select: passthrough_columns(&EDGE_COLUMNS)
            .chain(tombstone_columns(Expr::col("w", "watermark")))
            .collect(),
        from: TableRef::join(
            JoinType::Inner,
            TableRef::scan(edge_table, "s"),
            TableRef::subquery(watermarks, "w"),
            Expr::eq(
                Expr::col("w", "traversal_path"),
                Expr::col("s", "traversal_path"),
            ),
        ),
        where_clause: Some(Expr::and(
            code_source_kinds,
            Expr::binary(
                query_engine::compiler::Op::Lt,
                Expr::col("s", "_version"),
                Expr::col("w", "watermark"),
            ),
        )),
        ..Query::default()
    };
    insert_from(edge_table, &EDGE_COLUMNS, &select)
}

fn passthrough_columns<'a>(columns: &'a [&'a str]) -> impl Iterator<Item = SelectExpr> + 'a {
    columns.iter().map(|column| SelectExpr::col("s", *column))
}

fn tombstone_columns(watermark: Expr) -> impl Iterator<Item = SelectExpr> {
    [
        SelectExpr::new(
            Expr::func("addMicroseconds", vec![watermark, Expr::lit(-1)]),
            "_version",
        ),
        SelectExpr::new(Expr::lit(true), "_deleted"),
    ]
    .into_iter()
}

fn checkpoint_join_on() -> Expr {
    Expr::and(
        Expr::and(
            Expr::eq(
                Expr::col("cp", "traversal_path"),
                Expr::col("s", "traversal_path"),
            ),
            Expr::eq(Expr::col("cp", "project_id"), Expr::col("s", "project_id")),
        ),
        Expr::eq(Expr::col("cp", "branch"), Expr::col("s", "branch")),
    )
}

fn insert_from(table: &str, columns: &[&str], select: &Query) -> SweepStatement {
    let (sql, params) = emit_simple_query(&query_engine::compiler::Node::Query(Box::new(
        select.clone(),
    )))
    .expect("sweep queries contain no unsupported nodes");
    SweepStatement {
        table: table.to_string(),
        sql: format!(
            "INSERT INTO {table} ({columns}, _version, _deleted) {sql}",
            columns = columns.join(", ")
        ),
        params,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_names() -> CodeTableNames {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        CodeTableNames::from_ontology(&ontology).expect("code tables must resolve")
    }

    fn resolved(statement: &SweepStatement) -> String {
        let re = regex::Regex::new(r"\{(\w+):[^}]+\}").expect("valid regex");
        re.replace_all(&statement.sql, |caps: &regex::Captures| {
            statement
                .params
                .get(&caps[1])
                .map(|p| p.render_literal())
                .unwrap_or_else(|| caps[0].to_string())
        })
        .into_owned()
    }

    #[test]
    fn node_sweep_joins_checkpoint_watermark_without_final_scan() {
        let sql = resolved(&node_sweep("v9_gl_file", "v9_code_indexing_checkpoint"));
        assert!(sql.contains("v9_gl_file AS s"), "{sql}");
        assert!(!sql.contains("s FINAL"), "{sql}");
        assert!(
            sql.contains("v9_code_indexing_checkpoint AS cp FINAL"),
            "{sql}"
        );
        assert!(sql.contains("s._version < cp.indexed_at"), "{sql}");
        assert!(sql.contains("cp._deleted"), "{sql}");
        assert!(sql.contains("addMicroseconds(cp.indexed_at, -1)"), "{sql}");
    }

    #[test]
    fn code_edge_sweep_joins_checkpoint_directly() {
        let sql = resolved(&edge_sweep("v9_gl_code_edge", "v9_cp"));
        assert!(sql.contains("cp.project_id = s.project_id"), "{sql}");
        assert!(!sql.contains("source_kind ="), "{sql}");
    }

    #[test]
    fn plain_edge_sweep_scopes_by_source_kind_and_min_watermark() {
        let sql = resolved(&edge_sweep("v9_gl_edge", "v9_cp"));
        for kind in CodeTableNames::NODE_KINDS {
            assert!(sql.contains(&format!("s.source_kind = '{kind}'")), "{sql}");
        }
        assert!(
            sql.contains("min(cp.indexed_at)"),
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
        let tables: Vec<&str> = sweep.statements.iter().map(|s| s.table.as_str()).collect();
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
