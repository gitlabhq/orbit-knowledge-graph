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

use arrow::array::{Array, StringArray, UInt64Array};
use bytes::Bytes;
use clickhouse_client::ArrowClickHouseClient;
use gkg_server_config::indexing_progress::{
    BranchCodeSnapshot, CodeMeta, CodeProgressSnapshot, INDEXING_PROGRESS_BUCKET, MetaSnapshot,
    code_key, meta_key,
};
use gkg_utils::arrow::ArrowUtils;
use ontology::Ontology;
use parking_lot::Mutex;
use tracing::{debug, info};

use crate::handler::HandlerError;
use crate::nats::{KvPutOptions, NatsServices};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};

const CODE_INDEXING_CHECKPOINT_TABLE: &str = "code_indexing_checkpoint";
const GL_PROJECT_TABLE: &str = "gl_project";

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
    last_update: Mutex<HashMap<i64, Instant>>,
    debounce_secs: u64,
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
            last_update: Mutex::new(HashMap::new()),
            debounce_secs,
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

        let prev = self.read_previous_project(nats, project_id).await;
        let snapshot = merge_snapshot(
            prev,
            traversal_path,
            branch,
            branch_snapshot,
            indexed_at.to_rfc3339(),
        );

        let value = serde_json::to_vec(&snapshot)
            .map_err(|e| HandlerError::Processing(format!("serialize code snapshot: {e}")))?;

        nats.kv_put(
            INDEXING_PROGRESS_BUCKET,
            &code_key(project_id),
            Bytes::from(value),
            KvPutOptions::default(),
        )
        .await
        .map_err(|e| HandlerError::Processing(format!("KV put code: {e}")))?;

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
        if self.is_debounced(namespace_id) {
            debug!(
                namespace_id,
                "skipping namespace code meta write (debounced)"
            );
            return Ok(());
        }

        let projects_indexed = self
            .query_projects_indexed(namespace_traversal_path)
            .await
            .map_err(|e| HandlerError::Processing(format!("query projects_indexed: {e}")))?;
        let projects_total = self
            .query_projects_total(namespace_traversal_path)
            .await
            .map_err(|e| HandlerError::Processing(format!("query projects_total: {e}")))?;

        let code = CodeMeta {
            projects_indexed,
            projects_total,
            last_indexed_at: now.to_rfc3339(),
        };

        let prev_meta = self.read_previous_meta(nats, namespace_id).await;
        let merged = merge_meta_code(prev_meta, code, now.to_rfc3339());

        let value = serde_json::to_vec(&merged)
            .map_err(|e| HandlerError::Processing(format!("serialize meta: {e}")))?;
        nats.kv_put(
            INDEXING_PROGRESS_BUCKET,
            &meta_key(namespace_id),
            Bytes::from(value),
            KvPutOptions::default(),
        )
        .await
        .map_err(|e| HandlerError::Processing(format!("KV put meta: {e}")))?;

        self.record_update(namespace_id);
        Ok(())
    }

    async fn run_project_branch_counts(
        &self,
        traversal_path: &str,
        project_id: i64,
        branch: &str,
    ) -> Result<(HashMap<String, i64>, HashMap<String, i64>), String> {
        let mut nodes: HashMap<String, i64> = HashMap::new();

        // gl_branch: no branch column; filter by (traversal_path, project_id, name=branch).
        let branch_table = self.resolve_table("Branch")?;
        let sql = format!(
            r#"
            SELECT uniq(id) AS cnt
            FROM {branch_table}
            WHERE traversal_path = {{traversal_path:String}}
              AND project_id = {{project_id:Int64}}
              AND name = {{branch:String}}
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
            .map_err(|e| format!("query Branch: {e}"))?;
        let branch_count = scalar_u64(&batches, "cnt");
        if branch_count > 0 {
            nodes.insert("Branch".to_string(), branch_count as i64);
        }

        for kind in CODE_NODE_KINDS_WITH_BRANCH {
            let table = self.resolve_table(kind)?;
            let sql = format!(
                r#"
                SELECT uniq(id) AS cnt
                FROM {table}
                WHERE traversal_path = {{traversal_path:String}}
                  AND project_id = {{project_id:Int64}}
                  AND branch = {{branch:String}}
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
            let count = scalar_u64(&batches, "cnt");
            if count > 0 {
                nodes.insert((*kind).to_string(), count as i64);
            }
        }

        // Edges: gl_edge has no project_id or branch column. Scope by
        // (traversal_path, source_kind IN code kinds). At present the code
        // pipeline indexes a single branch per project, so this is a correct
        // per-project, per-branch approximation. Multi-branch indexing (a
        // future enhancement) will need to attribute edges per branch via
        // join with the source node tables.
        let edge_table = prefixed_table_name(self.ontology.edge_table(), *SCHEMA_VERSION);
        let source_kinds_sql = CODE_EDGE_SOURCE_KINDS
            .iter()
            .map(|k| format!("'{k}'"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            r#"
            SELECT relationship_kind, uniq(source_id, target_id) AS cnt
            FROM {edge_table}
            WHERE traversal_path = {{traversal_path:String}}
              AND source_kind IN ({source_kinds_sql})
              AND NOT _deleted
            GROUP BY relationship_kind
            "#
        );
        let batches = self
            .client
            .query(&sql)
            .param("traversal_path", traversal_path)
            .fetch_arrow()
            .await
            .map_err(|e| format!("query edges: {e}"))?;

        let mut edges = HashMap::new();
        for batch in &batches {
            let Some(rels) =
                ArrowUtils::get_column_by_name::<StringArray>(batch, "relationship_kind")
            else {
                continue;
            };
            let Some(counts) = ArrowUtils::get_column_by_name::<UInt64Array>(batch, "cnt") else {
                continue;
            };
            for row in 0..batch.num_rows() {
                if rels.is_null(row) || counts.is_null(row) {
                    continue;
                }
                edges.insert(rels.value(row).to_string(), counts.value(row) as i64);
            }
        }

        Ok((nodes, edges))
    }

    async fn query_projects_indexed(&self, namespace_traversal_path: &str) -> Result<i64, String> {
        let table = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let sql = format!(
            r#"
            SELECT uniq(project_id) AS cnt
            FROM {table}
            WHERE startsWith(traversal_path, {{traversal_path:String}})
            "#
        );
        let batches = self
            .client
            .query(&sql)
            .param("traversal_path", namespace_traversal_path)
            .fetch_arrow()
            .await
            .map_err(|e| format!("projects_indexed: {e}"))?;
        Ok(scalar_u64(&batches, "cnt") as i64)
    }

    async fn query_projects_total(&self, namespace_traversal_path: &str) -> Result<i64, String> {
        let table = prefixed_table_name(GL_PROJECT_TABLE, *SCHEMA_VERSION);
        let sql = format!(
            r#"
            SELECT count() AS cnt
            FROM {table} FINAL
            WHERE startsWith(traversal_path, {{traversal_path:String}})
              AND NOT _deleted
            "#
        );
        let batches = self
            .client
            .query(&sql)
            .param("traversal_path", namespace_traversal_path)
            .fetch_arrow()
            .await
            .map_err(|e| format!("projects_total: {e}"))?;
        Ok(scalar_u64(&batches, "cnt") as i64)
    }

    fn resolve_table(&self, node_name: &str) -> Result<String, String> {
        let raw = self
            .ontology
            .table_name(node_name)
            .map_err(|e| format!("ontology table_name({node_name}): {e}"))?;
        Ok(prefixed_table_name(raw, *SCHEMA_VERSION))
    }

    async fn read_previous_project(
        &self,
        nats: &dyn NatsServices,
        project_id: i64,
    ) -> Option<CodeProgressSnapshot> {
        let entry = nats
            .kv_get(INDEXING_PROGRESS_BUCKET, &code_key(project_id))
            .await
            .ok()
            .flatten()?;
        serde_json::from_slice(&entry.value).ok()
    }

    async fn read_previous_meta(
        &self,
        nats: &dyn NatsServices,
        namespace_id: i64,
    ) -> Option<MetaSnapshot> {
        let entry = nats
            .kv_get(INDEXING_PROGRESS_BUCKET, &meta_key(namespace_id))
            .await
            .ok()
            .flatten()?;
        serde_json::from_slice(&entry.value).ok()
    }

    fn is_debounced(&self, namespace_id: i64) -> bool {
        let map = self.last_update.lock();
        match map.get(&namespace_id) {
            Some(last) => last.elapsed().as_secs() < self.debounce_secs,
            None => false,
        }
    }

    fn record_update(&self, namespace_id: i64) {
        self.last_update.lock().insert(namespace_id, Instant::now());
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

/// Merge a fresh `code` block into a pre-existing meta snapshot. Other
/// top-level meta fields (state, initial_backfill_done, sdlc, ...) are
/// preserved. If no previous meta exists, a minimal one is created.
pub(crate) fn merge_meta_code(
    prev: Option<MetaSnapshot>,
    code: CodeMeta,
    updated_at: String,
) -> MetaSnapshot {
    match prev {
        Some(mut meta) => {
            meta.code = code;
            meta.updated_at = updated_at;
            meta
        }
        None => MetaSnapshot {
            state: "pending".to_string(),
            initial_backfill_done: false,
            updated_at,
            sdlc: Default::default(),
            code,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::ClickHouseConfigurationExt;
    use crate::testkit::mocks::MockNatsServices;
    use gkg_server_config::indexing_progress::SdlcMeta;

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

    #[test]
    fn merge_meta_code_preserves_sdlc_and_flags() {
        let prev = MetaSnapshot {
            state: "idle".to_string(),
            initial_backfill_done: true,
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            sdlc: SdlcMeta {
                last_completed_at: "2026-01-01T00:00:00Z".to_string(),
                last_started_at: "2026-01-01T00:00:00Z".to_string(),
                last_duration_ms: 1234,
                cycle_count: 42,
                last_error: "prev error".to_string(),
            },
            code: CodeMeta {
                projects_indexed: 1,
                projects_total: 2,
                last_indexed_at: "2026-01-01T00:00:00Z".to_string(),
            },
        };

        let updated = merge_meta_code(
            Some(prev),
            CodeMeta {
                projects_indexed: 5,
                projects_total: 10,
                last_indexed_at: "2026-02-01T00:00:00Z".to_string(),
            },
            "2026-02-01T00:00:00Z".to_string(),
        );

        assert_eq!(updated.state, "idle", "state preserved");
        assert!(updated.initial_backfill_done);
        assert_eq!(updated.sdlc.cycle_count, 42);
        assert_eq!(updated.sdlc.last_duration_ms, 1234);
        assert_eq!(updated.sdlc.last_error, "prev error");
        assert_eq!(updated.code.projects_indexed, 5);
        assert_eq!(updated.code.projects_total, 10);
        assert_eq!(updated.updated_at, "2026-02-01T00:00:00Z");
    }

    #[test]
    fn merge_meta_code_without_prev_sets_pending_state() {
        let updated = merge_meta_code(
            None,
            CodeMeta {
                projects_indexed: 1,
                projects_total: 3,
                last_indexed_at: "2026-02-01T00:00:00Z".to_string(),
            },
            "2026-02-01T00:00:00Z".to_string(),
        );

        assert_eq!(updated.state, "pending");
        assert!(!updated.initial_backfill_done);
        assert_eq!(updated.code.projects_total, 3);
    }

    #[tokio::test]
    async fn update_namespace_code_meta_is_debounced() {
        let writer = test_writer();
        let mock = MockNatsServices::new();

        // Seed a prior meta and a prior debounce timestamp. Since we forbid the
        // ClickHouse call in this test, mark debounced state by writing first
        // (which fails ClickHouse) - instead use a direct `record_update`.
        writer.record_update(77);
        let result = writer
            .update_namespace_code_meta(&mock, 77, "1/77/", chrono::Utc::now())
            .await;
        assert!(result.is_ok(), "debounced call must short-circuit");

        // No KV write occurred: key must not exist.
        assert!(
            mock.get_kv(INDEXING_PROGRESS_BUCKET, &meta_key(77))
                .is_none()
        );
    }
}
