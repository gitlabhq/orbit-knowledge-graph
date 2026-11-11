use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use chrono::Utc;
use orbit_log_fields::{debug, info, info_span, safe, warn};
use tracing::Instrument;

use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::clickhouse::ArrowClickHouseClient;
use crate::durability::WriteDurability;
use crate::modules::code::config::CodeTableNames;
use crate::orchestrator::scheduled::TaskError;
use orbit_utils::traversal_path::TraversalPath;

pub(crate) const CHECKPOINT_KEY_PREFIX: &str = "maintenance.code_stale_sweep";

fn namespace_checkpoint_key(traversal_path: &TraversalPath) -> String {
    format!("{CHECKPOINT_KEY_PREFIX}.{traversal_path}")
}

pub struct CodeStaleSweep {
    graph: ArrowClickHouseClient,
    checkpoint_store: Arc<dyn CheckpointStore>,
    statements: Vec<(String, String)>,
    sweeps_per_tick: usize,
}

impl CodeStaleSweep {
    pub fn new(
        graph: ArrowClickHouseClient,
        table_names: &CodeTableNames,
        checkpoint_store: Arc<dyn CheckpointStore>,
        sweeps_per_tick: usize,
    ) -> Self {
        let checkpoint_table = &table_names.checkpoints;

        let mut statements: Vec<(String, String)> = table_names
            .node_tables()
            .iter()
            .map(|table| {
                (
                    table.to_string(),
                    node_sweep(
                        table,
                        checkpoint_table,
                        table_names.immutable_columns(table),
                    ),
                )
            })
            .collect();
        statements.extend(table_names.edge_table_names().iter().map(|table| {
            (
                table.to_string(),
                edge_sweep(table, checkpoint_table, table_names),
            )
        }));

        Self {
            graph,
            checkpoint_store,
            statements,
            sweeps_per_tick,
        }
    }

    /// Capped per tick so a burst of drained namespaces cannot hold up the next dispatch ticks.
    pub async fn run_for_drained(&self, drained_paths: &[TraversalPath]) -> Result<(), TaskError> {
        if drained_paths.is_empty() || self.sweeps_per_tick == 0 {
            return Ok(());
        }
        let swept: HashSet<String> = self
            .checkpoint_store
            .load_by_prefix(CHECKPOINT_KEY_PREFIX)
            .await
            .map_err(TaskError::new)?
            .into_iter()
            .map(|(key, _)| key)
            .collect();

        let pending: Vec<&TraversalPath> = drained_paths
            .iter()
            .filter(|path| !swept.contains(&namespace_checkpoint_key(path)))
            .collect();
        let attempted = pending.len().min(self.sweeps_per_tick);
        let mut failed = 0usize;
        for path in pending.iter().take(attempted) {
            let span = info_span!(
                "code_stale_sweep_namespace",
                safe(GL_ROOT_NAMESPACE_ID) = path.top_level_namespace_id(),
                safe(TRAVERSAL_PATH) = tracing::field::Empty,
            );
            if path.is_valid() {
                span.record(safe::TRAVERSAL_PATH, path.as_str());
            }
            let sweep = async {
                if let Err(error) = self.sweep_namespace(path).await {
                    failed += 1;
                    warn!(
                        safe(ERROR_TYPE) = "stale_sweep",
                        expanded(ERROR_MESSAGE) = %error,
                        "post-backfill stale sweep failed, retrying on a later tick"
                    );
                }
            };
            sweep.instrument(span).await;
        }
        if pending.len() > self.sweeps_per_tick {
            info!(
                safe(COUNT) = pending.len(),
                safe(LIMIT) = self.sweeps_per_tick,
                "post-backfill stale sweeps deferred to later ticks"
            );
        }
        if failed > 0 {
            return Err(TaskError::new(format!(
                "{failed} of {attempted} post-backfill stale sweeps failed"
            )));
        }
        Ok(())
    }

    async fn sweep_namespace(&self, traversal_path: &TraversalPath) -> Result<(), TaskError> {
        let started = Utc::now();
        for (table, sql) in &self.statements {
            let statement_start = Instant::now();
            self.graph
                .query(sql)
                .param("path", traversal_path.as_str())
                .execute()
                .await
                .map_err(|e| {
                    TaskError::new(format!("stale sweep on {table} for {traversal_path}: {e}"))
                })?;
            debug!(
                safe(TABLE) = table.as_str(),
                safe(DURATION_S) = statement_start.elapsed().as_secs_f64(),
                "stale sweep statement complete"
            );
        }

        self.checkpoint_store
            .save(
                &namespace_checkpoint_key(traversal_path),
                &Checkpoint::new(started),
                WriteDurability::Durable,
            )
            .await
            .map_err(TaskError::new)?;
        info!("post-backfill stale sweep complete");
        Ok(())
    }
}

fn node_sweep(table: &str, checkpoint_table: &str, carried_columns: &[String]) -> String {
    let insert_carried = carried_columns
        .iter()
        .map(|column| format!(", {column}"))
        .collect::<String>();
    let select_carried = carried_columns
        .iter()
        .map(|column| format!("s.{column},\n            "))
        .collect::<String>();
    format!(
        r#"
        INSERT INTO {table} (traversal_path, project_id, branch, id{insert_carried}, _version, _deleted)
        SELECT
            s.traversal_path,
            s.project_id,
            s.branch,
            s.id,
            {select_carried}cp.indexed_at - toIntervalMicrosecond(1) AS _version,
            true AS _deleted
        FROM {table} AS s FINAL
        INNER JOIN {checkpoint_table} AS cp FINAL
            ON cp.traversal_path = s.traversal_path
           AND cp.project_id = s.project_id
           AND cp.branch = s.branch
        WHERE startsWith(s.traversal_path, {{path:String}})
          AND s._deleted = false
          AND cp._deleted = false
          AND s._version < cp.indexed_at
        "#
    )
}

fn edge_sweep(edge_table: &str, checkpoint_table: &str, table_names: &CodeTableNames) -> String {
    let written = table_names.written_variants_sql();
    if table_names.branch_scoped(edge_table) {
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
            FROM {edge_table} AS s FINAL
            INNER JOIN {checkpoint_table} AS cp FINAL
                ON cp.traversal_path = s.traversal_path
               AND cp.project_id = s.project_id
               AND cp.branch = s.branch
            WHERE startsWith(s.traversal_path, {{path:String}})
              AND s._deleted = false
              AND cp._deleted = false
              AND {written}
              AND s._version < cp.indexed_at
            "#
        );
    }

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
        FROM {edge_table} AS s FINAL
        INNER JOIN (
            SELECT traversal_path, min(indexed_at) AS watermark
            FROM {checkpoint_table} FINAL
            WHERE _deleted = false AND startsWith(traversal_path, {{path:String}})
            GROUP BY traversal_path
        ) AS w ON w.traversal_path = s.traversal_path
        WHERE startsWith(s.traversal_path, {{path:String}})
          AND s._deleted = false
          AND {written}
          AND s._version < w.watermark
        "#
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};

    fn table_names() -> CodeTableNames {
        CodeTableNames::from_catalog(
            &query_data_model::Catalog::<query_data_model::ClickHouse>::embedded().unwrap(),
        )
        .expect("code tables must resolve")
    }

    #[test]
    fn node_sweep_tombstones_only_final_survivors() {
        let sql = node_sweep("v9_gl_file", "v9_code_indexing_checkpoint", &[]);
        assert!(
            sql.contains("FROM v9_gl_file AS s FINAL"),
            "a raw-parts scan emits a no-op tombstone per superseded part row \
             instead of one per surviving stale key: {sql}"
        );
        assert!(sql.contains("s._deleted = false"), "{sql}");
        assert!(
            sql.contains("v9_code_indexing_checkpoint AS cp FINAL"),
            "{sql}"
        );
        assert!(sql.contains("s._version < cp.indexed_at"), "{sql}");
        assert!(sql.contains("cp._deleted = false"), "{sql}");
    }

    #[test]
    fn node_sweep_carries_the_unchanging_columns() {
        let names = table_names();
        let carried = names.immutable_columns(&names.file);
        assert!(carried.iter().any(|c| c == "path"), "{carried:?}");
        let sql = node_sweep(&names.file, "v9_cp", carried);
        for column in carried {
            assert!(sql.contains(&format!(", {column}")), "{sql}");
            assert!(sql.contains(&format!("s.{column},")), "{sql}");
        }
    }

    /// Code tables with CALLS File -> Definition and File -> ImportedSymbol routed to gl_edge.
    fn split_names() -> (CodeTableNames, String, String) {
        let names = CodeTableNames::from_catalog(&crate::testkit::variant_routes_catalog())
            .expect("code tables must resolve");
        let table = |name| prefixed_table_name(name, *SCHEMA_VERSION);
        (names, table("gl_code_edge"), table("gl_edge"))
    }

    #[test]
    fn sweeps_scope_to_the_namespace_path_for_pk_pruning() {
        let (names, code_edge, edge) = split_names();
        for sql in [
            node_sweep("v9_gl_file", "v9_cp", &[]),
            edge_sweep(&code_edge, "v9_cp", &names),
            edge_sweep(&edge, "v9_cp", &names),
        ] {
            assert!(
                sql.contains("startsWith(s.traversal_path, {path:String})"),
                "the checkpoint join alone does not prune the source scan: {sql}"
            );
        }
    }

    #[test]
    fn branch_scoped_edge_sweep_joins_checkpoint_directly() {
        let (names, code_edge, _) = split_names();
        let sql = edge_sweep(&code_edge, "v9_cp", &names);
        assert!(sql.contains("cp.project_id = s.project_id"), "{sql}");
        assert!(
            sql.contains("('CALLS', 'Definition', 'Definition')"),
            "{sql}"
        );
    }

    #[test]
    fn shared_edge_sweep_keeps_every_code_indexer_variant_and_min_watermark() {
        let (names, _, edge) = split_names();
        let sql = edge_sweep(&edge, "v9_cp", &names);
        for variant in [
            "('CALLS', 'File', 'Definition')",
            "('CONTAINS', 'Directory', 'File')",
            "('ON_BRANCH', 'File', 'Branch')",
        ] {
            assert!(sql.contains(variant), "{sql}");
        }
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
    fn namespace_checkpoint_keys_share_the_seed_drop_prefix() {
        let key = namespace_checkpoint_key(&TraversalPath::new_unchecked("1/9970/"));
        assert!(
            key.starts_with(CHECKPOINT_KEY_PREFIX),
            "SEED_CODE_CHECKPOINT_SQL drops sweep gates by this prefix; a key \
             outside it would survive a code migration and suppress the re-sweep: {key}"
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
            "graph",
        );
        let catalog =
            query_data_model::Catalog::<query_data_model::ClickHouse>::embedded().unwrap();
        let store = Arc::new(
            crate::checkpoint::ClickHouseCheckpointStore::new(
                Arc::new(graph.clone()),
                catalog.schema().unwrap(),
            )
            .unwrap(),
        );
        let sweep = CodeStaleSweep::new(graph, &names, store, 10);
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
