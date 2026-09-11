//! Post-backfill code stale sweep.
//!
//! The backfill tick only requests a sweep for each namespace it finds drained;
//! this task runs the requested sweeps on its own cadence. Running them inline
//! held the backfill tick for as long as the `FINAL` scans took, so a burst of
//! drained namespaces starved every project still waiting to be dispatched.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tracing::{debug, error, info, warn};

use crate::checkpoint::{Checkpoint, CheckpointError, CheckpointStore};
use crate::clickhouse::ArrowClickHouseClient;
use crate::durability::WriteDurability;
use crate::modules::code::config::CodeTableNames;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};
use orbit_server_config::{CodeStaleSweepConfig, ScheduleConfiguration};
use orbit_utils::traversal_path::TraversalPath;

pub(crate) const CHECKPOINT_KEY_PREFIX: &str = "maintenance.code_stale_sweep";

const CODE_INDEXING_CHECKPOINT_TABLE: &str = "code_indexing_checkpoint";

const STATEMENT_DURATION_LABEL: &str = "code_stale_sweep_statement";

/// A namespace whose sweep keeps failing is abandoned after this many attempts so its failing
/// `FINAL` scans stop recurring every run; its pre-backfill rows stay until the next schema version.
const MAX_SWEEP_ATTEMPTS: usize = 5;

fn namespace_checkpoint_key(traversal_path: &TraversalPath) -> String {
    format!("{CHECKPOINT_KEY_PREFIX}.{traversal_path}")
}

fn namespace_path_from_key(key: &str) -> Option<TraversalPath> {
    let path = key
        .strip_prefix(CHECKPOINT_KEY_PREFIX)?
        .strip_prefix('.')
        .filter(|path| !path.is_empty())?;
    Some(TraversalPath::new_unchecked(path))
}

/// A requested gate carries its attempt count in `cursor_values`; a completed gate has none.
fn requested_attempts(gate: &Checkpoint) -> Option<usize> {
    let cursor = gate.cursor_values.as_ref()?;
    let attempts = cursor
        .first()
        .and_then(|attempts| attempts.parse().ok())
        .unwrap_or(0);
    Some(attempts)
}

async fn save_request(
    checkpoint_store: &dyn CheckpointStore,
    path: &TraversalPath,
    attempts: usize,
) -> Result<(), CheckpointError> {
    let request = Checkpoint {
        watermark: Utc::now(),
        cursor_values: Some(vec![attempts.to_string()]),
        resume_floor: None,
    };
    checkpoint_store
        .save_progress(&namespace_checkpoint_key(path), &request)
        .await
}

/// The request is the gate key saved in progress; the sweep task turns it into a completed
/// gate. A gate in either state is never requested again for the schema version, so a
/// namespace is requested once no matter how many ticks see it drained.
pub async fn request_sweeps(
    checkpoint_store: &dyn CheckpointStore,
    drained_paths: &[TraversalPath],
) -> Result<usize, TaskError> {
    if drained_paths.is_empty() {
        return Ok(0);
    }
    let gated: HashSet<String> = checkpoint_store
        .load_by_prefix(CHECKPOINT_KEY_PREFIX)
        .await
        .map_err(TaskError::new)?
        .into_iter()
        .map(|(key, _)| key)
        .collect();

    let mut requested = 0usize;
    for path in drained_paths {
        if gated.contains(&namespace_checkpoint_key(path)) {
            continue;
        }
        save_request(checkpoint_store, path, 0)
            .await
            .map_err(TaskError::new)?;
        requested += 1;
    }
    if requested > 0 {
        info!(requested, "requested post-backfill stale sweeps");
    }
    Ok(requested)
}

struct SweepRequest {
    path: TraversalPath,
    attempts: usize,
}

/// Oldest requests first, so a burst of drained namespaces is served in drain order.
fn requested_sweeps(gates: Vec<(String, Checkpoint)>) -> Vec<SweepRequest> {
    let mut requested: Vec<(DateTime<Utc>, SweepRequest)> = gates
        .into_iter()
        .filter_map(|(key, gate)| {
            let attempts = requested_attempts(&gate)?;
            let path = namespace_path_from_key(&key)?;
            Some((gate.watermark, SweepRequest { path, attempts }))
        })
        .collect();
    requested.sort_by_key(|(requested_at, _)| *requested_at);
    requested.into_iter().map(|(_, request)| request).collect()
}

pub struct CodeStaleSweep {
    graph: ArrowClickHouseClient,
    checkpoint_store: Arc<dyn CheckpointStore>,
    statements: Vec<(String, String)>,
    metrics: ScheduledTaskMetrics,
    config: CodeStaleSweepConfig,
}

impl CodeStaleSweep {
    pub fn new(
        graph: ArrowClickHouseClient,
        table_names: &CodeTableNames,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: ScheduledTaskMetrics,
        config: CodeStaleSweepConfig,
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
            metrics,
            config,
        }
    }

    async fn sweep_requested(&self) -> Result<(), TaskError> {
        let gates = self
            .checkpoint_store
            .load_by_prefix(CHECKPOINT_KEY_PREFIX)
            .await
            .map_err(|error| {
                self.metrics
                    .record_error(CHECKPOINT_KEY_PREFIX, "checkpoint");
                TaskError::new(error)
            })?;
        let mut requests = requested_sweeps(gates);
        let pending = requests.len();
        if pending == 0 {
            return Ok(());
        }
        let cap = self.config.max_namespaces_per_run;
        if cap == 0 {
            warn!(
                pending,
                "code stale sweep paused by max_namespaces_per_run = 0"
            );
            return Ok(());
        }
        requests.truncate(cap);

        let mut failed = 0usize;
        for request in &requests {
            if let Err(error) = self.sweep_namespace(&request.path).await {
                failed += 1;
                warn!(
                    path = %request.path,
                    %error,
                    attempts = request.attempts + 1,
                    "post-backfill stale sweep failed"
                );
                self.retry_or_abandon(request).await;
            }
        }
        info!(
            pending,
            swept = requests.len() - failed,
            failed,
            "code stale sweep run complete"
        );
        if failed > 0 {
            return Err(TaskError::new(format!(
                "{failed} of {} requested stale sweeps failed",
                requests.len()
            )));
        }
        Ok(())
    }

    /// A fresh request timestamp moves a failing namespace behind every other request, so one
    /// namespace that fails every run cannot hold the oldest-first queue. Abandoning closes the
    /// gate like a completed sweep would.
    async fn retry_or_abandon(&self, request: &SweepRequest) {
        let attempts = request.attempts + 1;
        if attempts < MAX_SWEEP_ATTEMPTS {
            if let Err(error) =
                save_request(self.checkpoint_store.as_ref(), &request.path, attempts).await
            {
                self.metrics
                    .record_error(CHECKPOINT_KEY_PREFIX, "checkpoint");
                warn!(path = %request.path, %error, "failed to re-queue stale sweep request");
            }
            return;
        }
        let closed = self
            .checkpoint_store
            .save_completed(
                &namespace_checkpoint_key(&request.path),
                &Utc::now(),
                WriteDurability::Durable,
            )
            .await;
        match closed {
            Ok(()) => {
                self.metrics
                    .record_error(CHECKPOINT_KEY_PREFIX, "abandoned");
                error!(
                    path = %request.path,
                    attempts,
                    "abandoned post-backfill stale sweep; rows older than the backfill stay until the next schema version"
                );
            }
            Err(error) => {
                self.metrics
                    .record_error(CHECKPOINT_KEY_PREFIX, "checkpoint");
                warn!(path = %request.path, %error, "failed to abandon stale sweep request");
            }
        }
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
                    self.metrics.record_error(CHECKPOINT_KEY_PREFIX, "sweep");
                    TaskError::new(format!("stale sweep on {table} for {traversal_path}: {e}"))
                })?;
            let elapsed = statement_start.elapsed();
            self.metrics
                .record_query_duration(STATEMENT_DURATION_LABEL, elapsed.as_secs_f64());
            debug!(
                table,
                %traversal_path,
                duration_ms = elapsed.as_millis() as u64,
                "stale sweep statement complete"
            );
        }

        self.checkpoint_store
            .save_completed(
                &namespace_checkpoint_key(traversal_path),
                &started,
                WriteDurability::Durable,
            )
            .await
            .map_err(|error| {
                self.metrics
                    .record_error(CHECKPOINT_KEY_PREFIX, "checkpoint");
                TaskError::new(error)
            })?;
        info!(%traversal_path, "post-backfill stale sweep complete");
        Ok(())
    }
}

#[async_trait]
impl ScheduledTask for CodeStaleSweep {
    fn name(&self) -> &str {
        CHECKPOINT_KEY_PREFIX
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let start = Instant::now();
        let result = self.sweep_requested().await;
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics
            .record_run(self.name(), outcome, start.elapsed().as_secs_f64());
        result
    }
}

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

fn edge_sweep(edge_table: &str, checkpoint_table: &str) -> String {
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
            FROM {edge_table} AS s FINAL
            INNER JOIN {checkpoint_table} AS cp FINAL
                ON cp.traversal_path = s.traversal_path
               AND cp.project_id = s.project_id
               AND cp.branch = s.branch
            WHERE startsWith(s.traversal_path, {{path:String}})
              AND s._deleted = false
              AND cp._deleted = false
              AND s._version < cp.indexed_at
            "#
        );
    }

    let code_source_kinds = CodeTableNames::NODE_KINDS
        .map(|kind| format!("'{kind}'"))
        .join(", ");

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
          AND s.source_kind IN ({code_source_kinds})
          AND s._version < w.watermark
        "#
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use chrono::TimeDelta;

    use super::*;
    use crate::checkpoint::CheckpointError;

    fn table_names() -> CodeTableNames {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        CodeTableNames::from_ontology(&ontology).expect("code tables must resolve")
    }

    fn sweep_config(max_namespaces_per_run: usize) -> CodeStaleSweepConfig {
        orbit_utils::yaml::from_str(&format!(
            "cron: \"0 */1 * * * *\"\nmax_namespaces_per_run: {max_namespaces_per_run}"
        ))
        .expect("sweep config must parse")
    }

    fn requested(path: &str, requested_at: DateTime<Utc>) -> (String, Checkpoint) {
        (
            namespace_checkpoint_key(&TraversalPath::new_unchecked(path)),
            Checkpoint {
                watermark: requested_at,
                cursor_values: Some(vec!["0".to_string()]),
                resume_floor: None,
            },
        )
    }

    fn completed(path: &str, swept_at: DateTime<Utc>) -> (String, Checkpoint) {
        (
            namespace_checkpoint_key(&TraversalPath::new_unchecked(path)),
            Checkpoint {
                watermark: swept_at,
                cursor_values: None,
                resume_floor: None,
            },
        )
    }

    #[derive(Default)]
    struct RecordingCheckpointStore {
        gates: Mutex<Vec<(String, Checkpoint)>>,
    }

    impl RecordingCheckpointStore {
        fn with_gates(gates: Vec<(String, Checkpoint)>) -> Self {
            Self {
                gates: Mutex::new(gates),
            }
        }

        fn keys(&self) -> Vec<String> {
            self.gates
                .lock()
                .unwrap()
                .iter()
                .map(|(key, _)| key.clone())
                .collect()
        }
    }

    #[async_trait]
    impl CheckpointStore for RecordingCheckpointStore {
        async fn load(&self, key: &str) -> Result<Option<Checkpoint>, CheckpointError> {
            Ok(self
                .gates
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find(|(k, _)| k == key)
                .map(|(_, gate)| gate.clone()))
        }

        async fn save_progress(
            &self,
            key: &str,
            checkpoint: &Checkpoint,
        ) -> Result<(), CheckpointError> {
            self.gates
                .lock()
                .unwrap()
                .push((key.to_string(), checkpoint.clone()));
            Ok(())
        }

        async fn save_completed(
            &self,
            key: &str,
            watermark: &DateTime<Utc>,
            _durability: WriteDurability,
        ) -> Result<(), CheckpointError> {
            self.gates.lock().unwrap().push((
                key.to_string(),
                Checkpoint {
                    watermark: *watermark,
                    cursor_values: None,
                    resume_floor: None,
                },
            ));
            Ok(())
        }

        async fn load_by_prefix(
            &self,
            prefix: &str,
        ) -> Result<Vec<(String, Checkpoint)>, CheckpointError> {
            let latest_per_key: std::collections::BTreeMap<String, Checkpoint> = self
                .gates
                .lock()
                .unwrap()
                .iter()
                .filter(|(key, _)| key.starts_with(prefix))
                .cloned()
                .collect();
            Ok(latest_per_key.into_iter().collect())
        }

        async fn consolidate(
            &self,
            _parent_key: &str,
            _watermark: &DateTime<Utc>,
        ) -> Result<(), CheckpointError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn request_sweeps_skips_namespaces_with_a_gate_in_either_state() {
        let now = Utc::now();
        let store = RecordingCheckpointStore::with_gates(vec![
            requested("1/10/", now),
            completed("1/20/", now),
        ]);
        let drained = [
            TraversalPath::new_unchecked("1/10/"),
            TraversalPath::new_unchecked("1/20/"),
            TraversalPath::new_unchecked("1/30/"),
        ];

        let requested_count = request_sweeps(&store, &drained).await.unwrap();

        assert_eq!(requested_count, 1);
        assert_eq!(
            store.keys(),
            vec![
                "maintenance.code_stale_sweep.1/10/",
                "maintenance.code_stale_sweep.1/20/",
                "maintenance.code_stale_sweep.1/30/",
            ]
        );
        let request = store
            .load("maintenance.code_stale_sweep.1/30/")
            .await
            .unwrap()
            .unwrap();
        assert!(
            request.cursor_values.is_some(),
            "a request must be an in-progress gate so the sweep task can tell it from a completed one"
        );
    }

    #[tokio::test]
    async fn request_sweeps_without_drained_namespaces_writes_nothing() {
        let store = RecordingCheckpointStore::default();
        assert_eq!(request_sweeps(&store, &[]).await.unwrap(), 0);
        assert!(store.keys().is_empty());
    }

    #[test]
    fn requested_sweeps_orders_oldest_first_and_skips_completed_and_malformed_gates() {
        let now = Utc::now();
        let gates = vec![
            requested("1/30/", now),
            completed("1/40/", now - TimeDelta::hours(2)),
            requested("1/10/", now - TimeDelta::hours(1)),
            requested("1/20/", now - TimeDelta::minutes(30)),
            (
                CHECKPOINT_KEY_PREFIX.to_string(),
                Checkpoint {
                    watermark: now - TimeDelta::days(1),
                    cursor_values: Some(Vec::new()),
                    resume_floor: None,
                },
            ),
        ];

        let paths: Vec<String> = requested_sweeps(gates)
            .into_iter()
            .map(|request| request.path.as_str().to_string())
            .collect();

        assert_eq!(paths, vec!["1/10/", "1/20/", "1/30/"]);
    }

    #[tokio::test]
    async fn a_retried_request_moves_behind_the_other_requests_and_counts_its_attempts() {
        let now = Utc::now();
        let store = RecordingCheckpointStore::with_gates(vec![
            requested("1/10/", now - TimeDelta::hours(2)),
            requested("1/20/", now - TimeDelta::hours(1)),
        ]);

        save_request(&store, &TraversalPath::new_unchecked("1/10/"), 1)
            .await
            .unwrap();

        let gates = store.load_by_prefix(CHECKPOINT_KEY_PREFIX).await.unwrap();
        let requests: Vec<(String, usize)> = requested_sweeps(gates)
            .into_iter()
            .map(|request| (request.path.as_str().to_string(), request.attempts))
            .collect();
        assert_eq!(
            requests,
            vec![("1/20/".to_string(), 0), ("1/10/".to_string(), 1)]
        );
    }

    #[test]
    fn a_request_without_an_attempt_count_reads_as_a_first_attempt() {
        let gate = Checkpoint {
            watermark: Utc::now(),
            cursor_values: Some(Vec::new()),
            resume_floor: None,
        };
        assert_eq!(requested_attempts(&gate), Some(0));
        assert_eq!(requested_attempts(&completed("1/10/", Utc::now()).1), None);
    }

    #[test]
    fn node_sweep_tombstones_only_final_survivors() {
        let sql = node_sweep("v9_gl_file", "v9_code_indexing_checkpoint");
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
    fn sweeps_scope_to_the_namespace_path_for_pk_pruning() {
        for sql in [
            node_sweep("v9_gl_file", "v9_cp"),
            edge_sweep("v9_gl_code_edge", "v9_cp"),
            edge_sweep("v9_gl_edge", "v9_cp"),
        ] {
            assert!(
                sql.contains("startsWith(s.traversal_path, {path:String})"),
                "the checkpoint join alone does not prune the source scan: {sql}"
            );
        }
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
    fn namespace_checkpoint_keys_share_the_seed_drop_prefix() {
        let key = namespace_checkpoint_key(&TraversalPath::new_unchecked("1/9970/"));
        assert!(
            key.starts_with(CHECKPOINT_KEY_PREFIX),
            "SEED_CHECKPOINT_SQL drops sweep gates by this prefix; a key \
             outside it would survive a code migration and suppress the re-sweep: {key}"
        );
    }

    fn unreachable_graph() -> ArrowClickHouseClient {
        ArrowClickHouseClient::new(
            "http://localhost:0",
            "default",
            "default",
            None,
            &Default::default(),
            &Default::default(),
        )
    }

    fn sweep_with_store(
        store: Arc<RecordingCheckpointStore>,
        max_namespaces_per_run: usize,
    ) -> CodeStaleSweep {
        CodeStaleSweep::new(
            unreachable_graph(),
            &table_names(),
            store,
            ScheduledTaskMetrics::with_meter(&crate::testkit::test_meter()),
            sweep_config(max_namespaces_per_run),
        )
    }

    async fn gate(store: &RecordingCheckpointStore, path: &str) -> Checkpoint {
        store
            .load(&namespace_checkpoint_key(&TraversalPath::new_unchecked(
                path,
            )))
            .await
            .unwrap()
            .expect("gate must exist")
    }

    #[tokio::test]
    async fn a_failing_namespace_is_retried_behind_the_others_and_abandoned_after_the_cap() {
        let now = Utc::now();
        let store = Arc::new(RecordingCheckpointStore::with_gates(vec![requested(
            "1/10/",
            now - TimeDelta::hours(1),
        )]));
        let sweep = sweep_with_store(store.clone(), 10);

        for expected_attempts in 1..MAX_SWEEP_ATTEMPTS {
            assert!(
                sweep.run().await.is_err(),
                "a failed sweep must fail the run"
            );
            let gate = gate(&store, "1/10/").await;
            assert_eq!(requested_attempts(&gate), Some(expected_attempts));
            assert!(
                gate.watermark > now,
                "a retried request must move behind requests made before it"
            );
        }

        assert!(sweep.run().await.is_err());
        assert_eq!(
            requested_attempts(&gate(&store, "1/10/").await),
            None,
            "the last allowed attempt must close the gate so the namespace is not retried"
        );
        assert!(
            sweep.run().await.is_ok(),
            "an abandoned namespace must not be swept again"
        );
    }

    #[tokio::test]
    async fn one_failing_namespace_does_not_stop_the_others_in_the_same_run() {
        let now = Utc::now();
        let store = Arc::new(RecordingCheckpointStore::with_gates(vec![
            requested("1/10/", now - TimeDelta::hours(2)),
            requested("1/20/", now - TimeDelta::hours(1)),
        ]));
        let sweep = sweep_with_store(store.clone(), 10);

        assert!(sweep.run().await.is_err());

        assert_eq!(requested_attempts(&gate(&store, "1/10/").await), Some(1));
        assert_eq!(requested_attempts(&gate(&store, "1/20/").await), Some(1));
    }

    #[test]
    fn statements_cover_every_code_table_nodes_first() {
        let names = table_names();
        let graph = unreachable_graph();
        let store = Arc::new(crate::checkpoint::ClickHouseCheckpointStore::new(Arc::new(
            graph.clone(),
        )));
        let sweep = CodeStaleSweep::new(
            graph,
            &names,
            store,
            ScheduledTaskMetrics::with_meter(&crate::testkit::test_meter()),
            sweep_config(10),
        );
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
