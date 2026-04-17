pub mod code;
pub(crate) mod debounce;
pub(crate) mod kv;
pub mod queries;

pub use code::CodeProgressWriter;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use arrow::array::{Array, StringArray, UInt64Array};
use clickhouse_client::ArrowClickHouseClient;
use gkg_server_config::QueryConfig;
use gkg_utils::arrow::ArrowUtils;
use ontology::Ontology;
use query_engine::compiler::{ResultContext, codegen};
use tracing::{debug, info};

use crate::handler::HandlerError;
use crate::nats::NatsServices;

use gkg_server_config::indexing_progress::{
    CountsSnapshot, MetaSnapshot, SdlcMeta, counts_key, meta_key,
};

use self::debounce::Debouncer;
use self::queries::{
    build_cross_namespace_edge_query, build_edge_count_query, build_node_count_query,
    cross_namespace_edge_targets, node_count_targets,
};

/// Inputs for one call to [`ProgressWriter::write_progress`]. Bundled into a
/// struct so the signature doesn't need to thread seven arguments through
/// handlers.
pub struct ProgressRun<'a> {
    pub namespace_id: i64,
    pub traversal_path: &'a str,
    pub started_at: chrono::DateTime<chrono::Utc>,
    pub elapsed: std::time::Duration,
    pub total_rows: u64,
    pub error: Option<&'a str>,
}

pub struct ProgressWriter {
    client: Arc<ArrowClickHouseClient>,
    ontology: Arc<Ontology>,
    debouncer: Debouncer,
}

impl ProgressWriter {
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

    /// Pre-ETL write of `state="indexing"` so readers observe the active phase.
    /// Preserves all other fields from the previous meta (sdlc, code, cycle_count,
    /// initial_backfill_done). If no previous meta exists, writes a minimal
    /// indexing snapshot with zeros.
    pub async fn mark_indexing_started(
        &self,
        nats: &dyn NatsServices,
        namespace_id: i64,
        started_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), HandlerError> {
        let mut meta = kv::read_json::<MetaSnapshot>(nats, &meta_key(namespace_id))
            .await
            .unwrap_or_default();
        meta.state = "indexing".to_string();
        meta.updated_at = started_at.to_rfc3339();
        meta.sdlc.last_started_at = started_at.to_rfc3339();

        kv::write_json(nats, &meta_key(namespace_id), &meta).await
    }

    pub async fn write_progress(
        &self,
        nats: &dyn NatsServices,
        run: ProgressRun<'_>,
    ) -> Result<(), HandlerError> {
        let ProgressRun {
            namespace_id,
            traversal_path,
            started_at,
            elapsed,
            total_rows,
            error,
        } = run;

        if self.debouncer.is_debounced(namespace_id) {
            debug!(namespace_id, "skipping progress write (debounced)");
            return Ok(());
        }

        let count_started = Instant::now();
        let prev_meta = kv::read_json::<MetaSnapshot>(nats, &meta_key(namespace_id)).await;

        // Zero-row skip: if the pipeline processed no rows AND a prior counts
        // snapshot already exists, the existing counts are still authoritative.
        // We check the counts KV directly (not prev_meta) because
        // `mark_indexing_started` always writes meta first.
        let has_prev_counts = kv::read_json::<CountsSnapshot>(nats, &counts_key(traversal_path))
            .await
            .is_some();
        let skip_counts = total_rows == 0 && has_prev_counts;
        let rollup_count = if skip_counts {
            self.touch_counts_updated_at(nats, traversal_path).await;
            0
        } else {
            self.write_count_rollups(nats, traversal_path).await?
        };

        let completed_at = chrono::Utc::now();
        let prev_cycle = prev_meta.as_ref().map(|m| m.sdlc.cycle_count).unwrap_or(0);
        let prev_backfill_done = prev_meta.as_ref().is_some_and(|m| m.initial_backfill_done);
        // Preserve the code block: the code indexing handler owns `code`, and
        // the SDLC handler must not clobber it.
        let prev_code = prev_meta.map(|m| m.code).unwrap_or_default();

        let meta = MetaSnapshot {
            state: "idle".to_string(),
            initial_backfill_done: prev_backfill_done || error.is_none(),
            updated_at: completed_at.to_rfc3339(),
            sdlc: SdlcMeta {
                last_completed_at: completed_at.to_rfc3339(),
                last_started_at: started_at.to_rfc3339(),
                last_duration_ms: i64::try_from(elapsed.as_millis()).unwrap_or(i64::MAX),
                cycle_count: prev_cycle + 1,
                last_error: error.unwrap_or("").to_string(),
            },
            code: prev_code,
        };
        kv::write_json(nats, &meta_key(namespace_id), &meta).await?;

        self.debouncer.record(namespace_id);

        info!(
            namespace_id,
            kv_keys = rollup_count,
            count_ms = count_started.elapsed().as_millis() as u64,
            skip_counts,
            total_rows,
            "indexing progress written to KV"
        );

        Ok(())
    }

    /// Runs node + edge count queries, rolls the results up to every
    /// ancestor traversal path, and writes one `counts.<tp>` snapshot per
    /// rollup. Returns the number of KV keys written.
    async fn write_count_rollups(
        &self,
        nats: &dyn NatsServices,
        traversal_path: &str,
    ) -> Result<usize, HandlerError> {
        let (node_counts, edge_counts) = self
            .run_count_queries(traversal_path)
            .await
            .map_err(|e| HandlerError::Processing(format!("count query failed: {e}")))?;

        let rollups = rollup_counts(&node_counts, &edge_counts);
        let now = chrono::Utc::now().to_rfc3339();

        for (tp, (nodes, edges)) in &rollups {
            let snapshot = CountsSnapshot {
                updated_at: now.clone(),
                nodes: nodes.clone(),
                edges: edges.clone(),
            };
            kv::write_json(nats, &counts_key(tp), &snapshot).await?;
        }
        Ok(rollups.len())
    }

    async fn run_count_queries(
        &self,
        traversal_path: &str,
    ) -> Result<(Vec<NodeCountRow>, Vec<EdgeCountRow>), String> {
        let targets = node_count_targets(&self.ontology);
        if targets.is_empty() {
            return Ok((vec![], vec![]));
        }

        let node_batches = self
            .execute(build_node_count_query(&targets, traversal_path), "node")
            .await?;
        let mut node_rows = Vec::new();
        for batch in &node_batches {
            extract_node_rows(batch, &mut node_rows);
        }

        let edge_batches = self
            .execute(build_edge_count_query(traversal_path), "edge")
            .await?;
        let mut edge_rows = Vec::new();
        for batch in &edge_batches {
            extract_edge_rows(batch, &mut edge_rows);
        }

        for target in cross_namespace_edge_targets() {
            let cross_batches = self
                .execute(
                    build_cross_namespace_edge_query(&target, traversal_path),
                    "cross-namespace",
                )
                .await?;
            for batch in &cross_batches {
                extract_edge_rows(batch, &mut edge_rows);
            }
        }

        Ok((node_rows, edge_rows))
    }

    /// Compile a count-query AST and fetch its Arrow batches. `label` is used
    /// only in error messages / trace logs.
    async fn execute(
        &self,
        ast: query_engine::compiler::Node,
        label: &'static str,
    ) -> Result<Vec<arrow::record_batch::RecordBatch>, String> {
        let config = QueryConfig {
            max_execution_time: Some(30),
            ..QueryConfig::default()
        };
        let pq = codegen(&ast, ResultContext::new(), config)
            .map_err(|e| format!("{label} codegen: {e}"))?;

        debug!(sql = %pq.sql, label, "executing count query");

        let mut q = self.client.query(&pq.sql);
        for (key, param) in &pq.params {
            q = ArrowClickHouseClient::bind_param(q, key, &param.value, &param.ch_type);
        }
        q.fetch_arrow()
            .await
            .map_err(|e| format!("{label} query: {e}"))
    }

    /// Refresh `updated_at` on the existing counts key for a namespace without
    /// re-running ClickHouse count queries. Used on zero-row skip so readers
    /// don't see a stale flag despite the indexer running.
    async fn touch_counts_updated_at(&self, nats: &dyn NatsServices, traversal_path: &str) {
        let key = counts_key(traversal_path);
        let Some(mut snapshot) = kv::read_json::<CountsSnapshot>(nats, &key).await else {
            return;
        };
        snapshot.updated_at = chrono::Utc::now().to_rfc3339();
        let _ = kv::write_json(nats, &key, &snapshot).await;
    }
}

#[derive(Debug)]
struct NodeCountRow {
    entity: String,
    count: i64,
    traversal_path: String,
}

#[derive(Debug)]
struct EdgeCountRow {
    traversal_path: String,
    relationship_kind: String,
    count: i64,
}

type RollupMap = HashMap<String, (HashMap<String, i64>, HashMap<String, i64>)>;

fn rollup_counts(node_rows: &[NodeCountRow], edge_rows: &[EdgeCountRow]) -> RollupMap {
    let mut result: RollupMap = HashMap::new();

    for row in node_rows {
        for prefix in traversal_path_prefixes(&row.traversal_path) {
            let entry = result.entry(prefix).or_default();
            *entry.0.entry(row.entity.clone()).or_insert(0) += row.count;
        }
    }

    for row in edge_rows {
        for prefix in traversal_path_prefixes(&row.traversal_path) {
            let entry = result.entry(prefix).or_default();
            *entry.1.entry(row.relationship_kind.clone()).or_insert(0) += row.count;
        }
    }

    result
}

fn traversal_path_prefixes(tp: &str) -> Vec<String> {
    let parts: Vec<&str> = tp.trim_end_matches('/').split('/').collect();
    let mut prefixes = Vec::with_capacity(parts.len());
    for i in 1..=parts.len() {
        prefixes.push(format!("{}/", parts[..i].join("/")));
    }
    prefixes
}

fn extract_node_rows(batch: &arrow::record_batch::RecordBatch, out: &mut Vec<NodeCountRow>) {
    let (Some(entities), Some(counts), Some(tps)) = (
        ArrowUtils::get_column_by_name::<StringArray>(batch, "entity"),
        ArrowUtils::get_column_by_name::<UInt64Array>(batch, "cnt"),
        ArrowUtils::get_column_by_name::<StringArray>(batch, "traversal_path"),
    ) else {
        return;
    };
    for row in 0..batch.num_rows() {
        if entities.is_null(row) || counts.is_null(row) || tps.is_null(row) {
            continue;
        }
        out.push(NodeCountRow {
            entity: entities.value(row).to_string(),
            count: counts.value(row) as i64,
            traversal_path: tps.value(row).to_string(),
        });
    }
}

fn extract_edge_rows(batch: &arrow::record_batch::RecordBatch, out: &mut Vec<EdgeCountRow>) {
    let (Some(tps), Some(rels), Some(counts)) = (
        ArrowUtils::get_column_by_name::<StringArray>(batch, "traversal_path"),
        ArrowUtils::get_column_by_name::<StringArray>(batch, "relationship_kind"),
        ArrowUtils::get_column_by_name::<UInt64Array>(batch, "cnt"),
    ) else {
        return;
    };
    for row in 0..batch.num_rows() {
        if tps.is_null(row) || rels.is_null(row) || counts.is_null(row) {
            continue;
        }
        out.push(EdgeCountRow {
            traversal_path: tps.value(row).to_string(),
            relationship_kind: rels.value(row).to_string(),
            count: counts.value(row) as i64,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::ClickHouseConfigurationExt;
    use crate::testkit::mocks::MockNatsServices;
    use bytes::Bytes;
    use gkg_server_config::indexing_progress::{CodeMeta, INDEXING_PROGRESS_BUCKET};

    fn test_writer() -> ProgressWriter {
        let graph_client =
            Arc::new(gkg_server_config::ClickHouseConfiguration::default().build_client());
        let ontology = Arc::new(ontology::Ontology::load_embedded().unwrap());
        // Large debounce so any second write in a test is skipped.
        ProgressWriter::new(graph_client, ontology, 9999)
    }

    fn run_for(namespace_id: i64, tp: &str, total_rows: u64) -> ProgressRun<'_> {
        ProgressRun {
            namespace_id,
            traversal_path: tp,
            started_at: chrono::Utc::now(),
            elapsed: std::time::Duration::from_millis(1),
            total_rows,
            error: None,
        }
    }

    fn seed<T: serde::Serialize>(mock: &MockNatsServices, key: &str, value: &T) {
        mock.set_kv(
            INDEXING_PROGRESS_BUCKET,
            key,
            Bytes::from(serde_json::to_vec(value).unwrap()),
        );
    }

    fn read<T: serde::de::DeserializeOwned>(mock: &MockNatsServices, key: &str) -> T {
        let bytes = mock.get_kv(INDEXING_PROGRESS_BUCKET, key).unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    #[test]
    fn traversal_path_prefixes_correct() {
        let prefixes = traversal_path_prefixes("1/9970/55154808/");
        assert_eq!(prefixes, vec!["1/", "1/9970/", "1/9970/55154808/"]);
    }

    #[test]
    fn rollup_aggregates_to_ancestors() {
        let node_rows = vec![
            NodeCountRow {
                entity: "Project".to_string(),
                count: 10,
                traversal_path: "1/2/3/".to_string(),
            },
            NodeCountRow {
                entity: "Project".to_string(),
                count: 5,
                traversal_path: "1/2/4/".to_string(),
            },
        ];
        let edge_rows = vec![EdgeCountRow {
            traversal_path: "1/2/3/".to_string(),
            relationship_kind: "IN_PROJECT".to_string(),
            count: 20,
        }];

        let result = rollup_counts(&node_rows, &edge_rows);

        let root = result.get("1/2/").unwrap();
        assert_eq!(root.0.get("Project"), Some(&15));
        assert_eq!(root.1.get("IN_PROJECT"), Some(&20));

        let child = result.get("1/2/3/").unwrap();
        assert_eq!(child.0.get("Project"), Some(&10));
    }

    #[tokio::test]
    async fn mark_indexing_started_writes_indexing_state_fresh() {
        let writer = test_writer();
        let mock = MockNatsServices::new();
        let started = chrono::Utc::now();

        writer
            .mark_indexing_started(&mock, 42, started)
            .await
            .unwrap();

        let meta: MetaSnapshot = read(&mock, &meta_key(42));
        assert_eq!(meta.state, "indexing");
        assert_eq!(meta.updated_at, started.to_rfc3339());
        assert_eq!(meta.sdlc.last_started_at, started.to_rfc3339());
        // Fresh (no prev meta): monotonic flag starts false.
        assert!(!meta.initial_backfill_done);
        assert_eq!(meta.sdlc.cycle_count, 0);
    }

    #[tokio::test]
    async fn mark_indexing_started_preserves_prev_fields() {
        let writer = test_writer();
        let mock = MockNatsServices::new();
        let prev = MetaSnapshot {
            state: "idle".to_string(),
            initial_backfill_done: true,
            updated_at: "2020-01-01T00:00:00Z".to_string(),
            sdlc: SdlcMeta {
                last_completed_at: "2020-01-01T00:00:00Z".to_string(),
                last_started_at: "2019-12-31T23:59:00Z".to_string(),
                last_duration_ms: 1000,
                cycle_count: 7,
                last_error: String::new(),
            },
            code: CodeMeta {
                projects_indexed: 3,
                projects_total: 5,
                last_indexed_at: "2020-01-01T00:00:00Z".to_string(),
            },
        };
        seed(&mock, &meta_key(42), &prev);

        writer
            .mark_indexing_started(&mock, 42, chrono::Utc::now())
            .await
            .unwrap();

        let meta: MetaSnapshot = read(&mock, &meta_key(42));
        assert_eq!(meta.state, "indexing");
        assert!(meta.initial_backfill_done, "monotonic flag preserved");
        assert_eq!(meta.sdlc.cycle_count, 7, "cycle_count preserved");
        assert_eq!(meta.code.projects_indexed, 3);
        assert_eq!(meta.code.projects_total, 5);
    }

    #[tokio::test]
    async fn zero_row_skip_refreshes_counts_updated_at() {
        let writer = test_writer();
        let mock = MockNatsServices::new();

        let stale_ts = "2020-01-01T00:00:00Z".to_string();
        let stale_counts = CountsSnapshot {
            updated_at: stale_ts.clone(),
            nodes: HashMap::from([("Project".to_string(), 42)]),
            edges: HashMap::new(),
        };
        seed(&mock, &counts_key("1/99/"), &stale_counts);
        seed(
            &mock,
            &meta_key(99),
            &MetaSnapshot {
                state: "idle".to_string(),
                initial_backfill_done: true,
                updated_at: stale_ts.clone(),
                sdlc: SdlcMeta::default(),
                code: CodeMeta::default(),
            },
        );

        writer
            .write_progress(&mock, run_for(99, "1/99/", 0))
            .await
            .unwrap();

        let refreshed: CountsSnapshot = read(&mock, &counts_key("1/99/"));
        assert_ne!(refreshed.updated_at, stale_ts);
        assert_eq!(refreshed.nodes.get("Project"), Some(&42));
    }

    #[tokio::test]
    async fn write_progress_skips_counts_when_zero_rows_with_prev() {
        let writer = test_writer();
        let mock = MockNatsServices::new();

        seed(&mock, &counts_key("1/99/"), &CountsSnapshot::default());
        seed(
            &mock,
            &meta_key(99),
            &MetaSnapshot {
                state: "idle".to_string(),
                initial_backfill_done: true,
                ..Default::default()
            },
        );

        // total_rows=0 + prev_counts=Some => skip counts. No ClickHouse call.
        writer
            .write_progress(&mock, run_for(99, "1/99/", 0))
            .await
            .expect("should succeed without ClickHouse");

        let meta: MetaSnapshot = read(&mock, &meta_key(99));
        assert_eq!(meta.state, "idle");
        assert!(meta.initial_backfill_done);
        assert_eq!(meta.sdlc.cycle_count, 1);
    }

    #[tokio::test]
    async fn write_progress_runs_counts_when_counts_key_missing() {
        let writer = test_writer();
        let mock = MockNatsServices::new();

        // Prior meta exists but NO counts key. total_rows=0 + no counts =>
        // must NOT skip; must attempt count queries, which fail against the
        // default ClickHouse config in tests.
        seed(
            &mock,
            &meta_key(99),
            &MetaSnapshot {
                state: "idle".to_string(),
                initial_backfill_done: true,
                ..Default::default()
            },
        );

        let result = writer.write_progress(&mock, run_for(99, "1/99/", 0)).await;
        assert!(
            result.is_err(),
            "expected count query attempt (and fail), got: {result:?}"
        );
    }

    #[tokio::test]
    async fn write_progress_preserves_monotonic_initial_backfill_done_on_error() {
        let writer = test_writer();
        let mock = MockNatsServices::new();

        seed(&mock, &counts_key("1/7/"), &CountsSnapshot::default());
        seed(
            &mock,
            &meta_key(7),
            &MetaSnapshot {
                state: "idle".to_string(),
                initial_backfill_done: true,
                sdlc: SdlcMeta {
                    cycle_count: 5,
                    ..Default::default()
                },
                ..Default::default()
            },
        );

        let run = ProgressRun {
            error: Some("boom"),
            ..run_for(7, "1/7/", 0)
        };
        writer.write_progress(&mock, run).await.unwrap();

        let meta: MetaSnapshot = read(&mock, &meta_key(7));
        assert!(meta.initial_backfill_done, "monotonic flag preserved");
        assert_eq!(meta.sdlc.last_error, "boom");
        assert_eq!(meta.sdlc.cycle_count, 6);
    }
}
