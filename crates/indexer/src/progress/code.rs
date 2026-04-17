//! Code indexing progress writes to NATS KV.
//!
//! Produces two KV values per code indexing run:
//! - `code.<project_id>`: per-project snapshot with per-branch node/edge counts,
//!   preserving snapshots for other branches on merge.
//! - `meta.<namespace_id>.code`: namespace-wide code rollup (projects_indexed,
//!   projects_total, last_indexed_at). The SDLC field and any other top-level
//!   meta fields are preserved.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, UInt64Array};
use clickhouse_client::ArrowClickHouseClient;
use gkg_server_config::indexing_progress::{
    BranchCodeSnapshot, CodeMeta, CodeProgressSnapshot, MetaSnapshot, code_key, meta_key,
};
use gkg_utils::arrow::ArrowUtils;
use ontology::Ontology;
use tracing::{debug, info};

use crate::handler::HandlerError;
use crate::nats::NatsServices;
use crate::progress::debounce::Debouncer;
use crate::progress::kv;
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};

/// Source kinds for edges produced by the code indexing pipeline. Mirrors
/// `stale_data_cleaner::CODE_EDGE_SOURCE_KINDS`; duplicated here to keep
/// `progress` free of `modules::code` imports.
const CODE_EDGE_SOURCE_KINDS: &[&str] = &[
    "Branch",
    "Directory",
    "File",
    "Definition",
    "ImportedSymbol",
];

/// Node entities produced by the code indexing pipeline.
///
/// `Branch` is keyed by `(traversal_path, project_id, name)` in `gl_branch`.
/// The other kinds carry a `branch` column, so they are filtered by
/// `(traversal_path, project_id, branch)`.
const CODE_NODE_KINDS_WITH_BRANCH: &[&str] = &["Directory", "File", "Definition", "ImportedSymbol"];

pub struct CodeProgressWriter {
    client: Arc<ArrowClickHouseClient>,
    ontology: Arc<Ontology>,
    debouncer: Debouncer,
}

impl CodeProgressWriter {
    pub fn new(
        client: Arc<ArrowClickHouseClient>,
        ontology: Arc<Ontology>,
        debounce_secs: u64,
    ) -> Self {
        Self {
            client,
            ontology,
            debouncer: Debouncer::new(debounce_secs),
        }
    }

    /// Writes `code.<project_id>` with the branch snapshot merged into any
    /// pre-existing entries for other branches.
    pub async fn write_project_progress(
        &self,
        nats: &dyn NatsServices,
        project_id: i64,
        traversal_path: &str,
        branch: &str,
        commit: &str,
        indexed_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), HandlerError> {
        let count_started = Instant::now();

        let (nodes, edges) = self
            .run_project_branch_counts(traversal_path, project_id, branch)
            .await
            .map_err(|e| HandlerError::Processing(format!("code count query failed: {e}")))?;

        let branch_snapshot = BranchCodeSnapshot {
            commit: commit.to_string(),
            indexed_at: indexed_at.to_rfc3339(),
            nodes,
            edges,
        };

        let prev = kv::read_json::<CodeProgressSnapshot>(nats, &code_key(project_id)).await;
        let snapshot = merge_snapshot(
            prev,
            traversal_path,
            branch,
            branch_snapshot,
            indexed_at.to_rfc3339(),
        );

        kv::write_json(nats, &code_key(project_id), &snapshot).await?;

        info!(
            project_id,
            branch,
            count_ms = count_started.elapsed().as_millis() as u64,
            "code progress written to KV"
        );

        Ok(())
    }

    /// Refreshes the `code` block of the `meta.<namespace_id>` entry while
    /// preserving every other field (state, initial_backfill_done, sdlc, ...).
    pub async fn update_namespace_code_meta(
        &self,
        nats: &dyn NatsServices,
        namespace_id: i64,
        namespace_traversal_path: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), HandlerError> {
        if self.debouncer.is_debounced(namespace_id) {
            debug!(
                namespace_id,
                "skipping namespace code meta write (debounced)"
            );
            return Ok(());
        }

        let projects_indexed = self
            .scalar_count_by_prefix(
                &prefixed_table_name("code_indexing_checkpoint", *SCHEMA_VERSION),
                "uniq(project_id)",
                namespace_traversal_path,
                false,
            )
            .await
            .map_err(|e| HandlerError::Processing(format!("query projects_indexed: {e}")))?;
        let projects_total = self
            .scalar_count_by_prefix(
                &prefixed_table_name("gl_project", *SCHEMA_VERSION),
                "count()",
                namespace_traversal_path,
                true,
            )
            .await
            .map_err(|e| HandlerError::Processing(format!("query projects_total: {e}")))?;

        let code = CodeMeta {
            projects_indexed,
            projects_total,
            last_indexed_at: now.to_rfc3339(),
        };
        kv::update_json::<MetaSnapshot, _>(nats, &meta_key(namespace_id), |meta| {
            apply_code_block(meta, code, &now.to_rfc3339());
        })
        .await?;
        self.debouncer.record(namespace_id);
        Ok(())
    }

    async fn run_project_branch_counts(
        &self,
        traversal_path: &str,
        project_id: i64,
        branch: &str,
    ) -> Result<(HashMap<String, i64>, HashMap<String, i64>), String> {
        let mut nodes: HashMap<String, i64> = HashMap::new();

        // gl_branch is keyed by `name`; the other node tables carry a
        // `branch` column. The query shape only differs in that one filter.
        for kind in std::iter::once("Branch").chain(CODE_NODE_KINDS_WITH_BRANCH.iter().copied()) {
            let table = self.resolve_table(kind)?;
            let branch_filter = if kind == "Branch" {
                "name = {branch:String}"
            } else {
                "branch = {branch:String}"
            };
            let sql = format!(
                r#"
                SELECT uniq(id) AS cnt
                FROM {table}
                WHERE traversal_path = {{traversal_path:String}}
                  AND project_id = {{project_id:Int64}}
                  AND {branch_filter}
                  AND NOT _deleted
                "#
            );
            let batches = self
                .client
                .query(&sql)
                .param("traversal_path", traversal_path)
                .param("project_id", project_id)
                .param("branch", branch)
                .fetch_arrow()
                .await
                .map_err(|e| format!("query {kind}: {e}"))?;
            let count = scalar_u64(&batches, "cnt") as i64;
            if count > 0 {
                nodes.insert(kind.to_string(), count);
            }
        }

        // Edges: gl_edge has no project_id or branch column. Scope by
        // (traversal_path, source_kind IN code kinds). The code pipeline
        // indexes one branch per project today, so this is a correct
        // per-project, per-branch approximation. Multi-branch indexing would
        // need a join with the source node tables to attribute edges.
        let edge_table = prefixed_table_name(self.ontology.edge_table(), *SCHEMA_VERSION);
        let source_kinds_sql = CODE_EDGE_SOURCE_KINDS
            .iter()
            .map(|k| format!("'{k}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            r#"
            SELECT
                traversal_path,
                relationship_kind,
                uniq(source_id, target_id) AS cnt
            FROM {edge_table}
            WHERE traversal_path = {{traversal_path:String}}
              AND source_kind IN ({source_kinds_sql})
              AND NOT _deleted
            GROUP BY traversal_path, relationship_kind
            "#
        );
        let batches = self
            .client
            .query(&sql)
            .param("traversal_path", traversal_path)
            .fetch_arrow()
            .await
            .map_err(|e| format!("query edges: {e}"))?;

        let mut rows = Vec::new();
        for batch in &batches {
            crate::progress::extract_count_rows(batch, "relationship_kind", &mut rows);
        }
        let edges = rows.into_iter().map(|r| (r.key, r.count)).collect();

        Ok((nodes, edges))
    }

    /// Runs `SELECT <agg> AS cnt FROM <table> [FINAL] WHERE
    /// startsWith(traversal_path, <prefix>) [AND NOT _deleted]` and returns
    /// the scalar `cnt` value. `filter_deleted` adds the `FINAL` modifier and
    /// the `NOT _deleted` predicate — needed for soft-deleted tables only.
    async fn scalar_count_by_prefix(
        &self,
        table: &str,
        agg: &str,
        prefix: &str,
        filter_deleted: bool,
    ) -> Result<i64, String> {
        let (final_kw, deleted_clause) = if filter_deleted {
            ("FINAL", "AND NOT _deleted")
        } else {
            ("", "")
        };
        let sql = format!(
            r#"
            SELECT {agg} AS cnt
            FROM {table} {final_kw}
            WHERE startsWith(traversal_path, {{traversal_path:String}})
              {deleted_clause}
            "#
        );
        let batches = self
            .client
            .query(&sql)
            .param("traversal_path", prefix)
            .fetch_arrow()
            .await
            .map_err(|e| e.to_string())?;
        Ok(scalar_u64(&batches, "cnt") as i64)
    }

    fn resolve_table(&self, node_name: &str) -> Result<String, String> {
        let raw = self
            .ontology
            .table_name(node_name)
            .map_err(|e| format!("ontology table_name({node_name}): {e}"))?;
        Ok(prefixed_table_name(raw, *SCHEMA_VERSION))
    }
}

fn scalar_u64(batches: &[arrow::record_batch::RecordBatch], column: &str) -> u64 {
    for batch in batches {
        let Some(col) = ArrowUtils::get_column_by_name::<UInt64Array>(batch, column) else {
            continue;
        };
        if col.is_empty() || col.is_null(0) {
            continue;
        }
        return col.value(0);
    }
    0
}

/// Apply a fresh `code` block to a meta snapshot while preserving every other
/// field. A fresh snapshot (`state == ""`) is seeded to `"pending"` so readers
/// see a meaningful state until SDLC writes `"idle"`.
pub(crate) fn apply_code_block(meta: &mut MetaSnapshot, code: CodeMeta, updated_at: &str) {
    if meta.state.is_empty() {
        meta.state = "pending".to_string();
    }
    meta.code = code;
    meta.updated_at = updated_at.to_string();
}

/// Merge a freshly computed branch snapshot into the previous per-project
/// snapshot, preserving branches that were not reindexed in this cycle.
pub(crate) fn merge_snapshot(
    prev: Option<CodeProgressSnapshot>,
    traversal_path: &str,
    branch: &str,
    branch_snapshot: BranchCodeSnapshot,
    updated_at: String,
) -> CodeProgressSnapshot {
    let mut snapshot = prev.unwrap_or_default();
    snapshot.traversal_path = traversal_path.to_string();
    snapshot.updated_at = updated_at;
    snapshot
        .branches
        .insert(branch.to_string(), branch_snapshot);
    snapshot
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::ClickHouseConfigurationExt;
    use crate::testkit::mocks::MockNatsServices;
    use gkg_server_config::indexing_progress::{INDEXING_PROGRESS_BUCKET, SdlcMeta};

    fn test_writer() -> CodeProgressWriter {
        let graph_client =
            Arc::new(gkg_server_config::ClickHouseConfiguration::default().build_client());
        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        CodeProgressWriter::new(graph_client, ontology, 9999)
    }

    #[test]
    fn merge_snapshot_adds_branch_when_prev_is_none() {
        let snapshot = merge_snapshot(
            None,
            "1/9970/proj/",
            "main",
            BranchCodeSnapshot {
                commit: "abc".to_string(),
                indexed_at: "2026-01-01T00:00:00Z".to_string(),
                nodes: HashMap::from([("File".to_string(), 10)]),
                edges: HashMap::from([("DEFINES".to_string(), 5)]),
            },
            "2026-01-01T00:00:00Z".to_string(),
        );

        assert_eq!(snapshot.traversal_path, "1/9970/proj/");
        assert_eq!(snapshot.branches.len(), 1);
        let main = snapshot.branches.get("main").unwrap();
        assert_eq!(main.nodes.get("File"), Some(&10));
        assert_eq!(main.edges.get("DEFINES"), Some(&5));
    }

    #[test]
    fn merge_snapshot_preserves_other_branches() {
        let mut prev_branches = HashMap::new();
        prev_branches.insert(
            "feature".to_string(),
            BranchCodeSnapshot {
                commit: "feat-sha".to_string(),
                indexed_at: "2026-01-01T00:00:00Z".to_string(),
                nodes: HashMap::from([("File".to_string(), 100)]),
                edges: HashMap::from([("DEFINES".to_string(), 50)]),
            },
        );
        let prev = CodeProgressSnapshot {
            traversal_path: "1/9970/proj/".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            branches: prev_branches,
        };

        let snapshot = merge_snapshot(
            Some(prev),
            "1/9970/proj/",
            "main",
            BranchCodeSnapshot {
                commit: "main-sha".to_string(),
                indexed_at: "2026-02-01T00:00:00Z".to_string(),
                nodes: HashMap::from([("File".to_string(), 200)]),
                edges: HashMap::from([("DEFINES".to_string(), 150)]),
            },
            "2026-02-01T00:00:00Z".to_string(),
        );

        assert_eq!(snapshot.branches.len(), 2, "both branches retained");
        let feature = snapshot.branches.get("feature").unwrap();
        assert_eq!(
            feature.nodes.get("File"),
            Some(&100),
            "feature branch untouched"
        );
        let main = snapshot.branches.get("main").unwrap();
        assert_eq!(main.nodes.get("File"), Some(&200));
        assert_eq!(snapshot.updated_at, "2026-02-01T00:00:00Z");
    }

    #[test]
    fn merge_snapshot_replaces_reindexed_branch() {
        let mut prev_branches = HashMap::new();
        prev_branches.insert(
            "main".to_string(),
            BranchCodeSnapshot {
                commit: "old".to_string(),
                indexed_at: "2026-01-01T00:00:00Z".to_string(),
                nodes: HashMap::from([("File".to_string(), 1)]),
                edges: HashMap::new(),
            },
        );
        let prev = CodeProgressSnapshot {
            traversal_path: "1/p/".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            branches: prev_branches,
        };

        let snapshot = merge_snapshot(
            Some(prev),
            "1/p/",
            "main",
            BranchCodeSnapshot {
                commit: "new".to_string(),
                indexed_at: "2026-02-01T00:00:00Z".to_string(),
                nodes: HashMap::from([("File".to_string(), 999)]),
                edges: HashMap::new(),
            },
            "2026-02-01T00:00:00Z".to_string(),
        );

        let main = snapshot.branches.get("main").unwrap();
        assert_eq!(main.commit, "new");
        assert_eq!(main.nodes.get("File"), Some(&999));
    }

    fn code_meta(indexed: i64, total: i64) -> CodeMeta {
        CodeMeta {
            projects_indexed: indexed,
            projects_total: total,
            last_indexed_at: "2026-02-01T00:00:00Z".to_string(),
        }
    }

    #[test]
    fn apply_code_block_preserves_sdlc_and_flags() {
        let mut meta = MetaSnapshot {
            state: "idle".to_string(),
            initial_backfill_done: true,
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            sdlc: SdlcMeta {
                cycle_count: 42,
                last_error: "prev error".to_string(),
                last_duration_ms: 1234,
                ..Default::default()
            },
            code: code_meta(1, 2),
        };

        apply_code_block(&mut meta, code_meta(5, 10), "2026-02-01T00:00:00Z");

        assert_eq!(meta.state, "idle", "state preserved");
        assert!(meta.initial_backfill_done);
        assert_eq!(meta.sdlc.cycle_count, 42);
        assert_eq!(meta.sdlc.last_error, "prev error");
        assert_eq!(meta.code.projects_indexed, 5);
        assert_eq!(meta.updated_at, "2026-02-01T00:00:00Z");
    }

    #[test]
    fn apply_code_block_on_default_meta_seeds_pending() {
        let mut meta = MetaSnapshot::default();
        apply_code_block(&mut meta, code_meta(1, 3), "2026-02-01T00:00:00Z");

        assert_eq!(meta.state, "pending");
        assert!(!meta.initial_backfill_done);
        assert_eq!(meta.code.projects_total, 3);
    }

    #[tokio::test]
    async fn update_namespace_code_meta_is_debounced() {
        let writer = test_writer();
        let mock = MockNatsServices::new();

        // Pre-record the debouncer so the call short-circuits before touching
        // ClickHouse (which isn't available in tests).
        writer.debouncer.record(77);
        writer
            .update_namespace_code_meta(&mock, 77, "1/77/", chrono::Utc::now())
            .await
            .expect("debounced call must short-circuit");

        assert!(
            mock.get_kv(INDEXING_PROGRESS_BUCKET, &meta_key(77))
                .is_none()
        );
    }
}
