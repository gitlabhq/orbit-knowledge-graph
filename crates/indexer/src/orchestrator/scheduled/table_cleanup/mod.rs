mod sql;

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

use arrow::array::{Array, StringArray};
use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use futures::StreamExt;
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::clickhouse::{ArrowClickHouseClient, ArrowQuery};
use crate::modules::code::checkpoint::CODE_INDEXING_CHECKPOINT_TABLE;
use crate::modules::code::config::CodeTableNames;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name, read_active_version};
use orbit_server_config::{ScheduleConfiguration, TableCleanupConfig};

const TASK_NAME: &str = "maintenance.table_cleanup";
const CURSOR_KEY_PREFIX: &str = "maintenance.table_cleanup.";
/// A refusal must outlive the condition that caused it: attached parts stop looking foreign once the table's own numbering passes them.
const IDENTITY_KEY_PREFIX: &str = "maintenance.table_cleanup.identity.";
const REFUSED: &str = "refused";
const ADMITTED: &str = "admitted";
/// Scope tuples are inlined three times per code statement; 500 keep a statement well under ClickHouse's default 256 KiB `max_query_size`.
const SCOPES_PER_STATEMENT: usize = 500;
/// The path list is inlined six times per statement, so it gets an eighth of the session's `max_query_size`:
/// 32 KiB under ClickHouse's default 256 KiB, capped so a statement never grows past a few MiB.
const MIN_PATH_LIST_BYTES: usize = 32 * 1024;
const MAX_PATH_LIST_BYTES: usize = 1024 * 1024;
const PATH_LIST_SHARE: usize = 8;
/// Cron passes land a few seconds after the minute, so an exact interval would skip a pass.
const PURGE_SLACK: TimeDelta = TimeDelta::seconds(60);
const NO_HOLD: i64 = -1;

#[derive(Clone, Copy, PartialEq, Eq)]
enum CodeRole {
    None,
    Project,
    SharedEdge,
}

struct CleanupTable {
    name: String,
    key: String,
    code: CodeRole,
}

impl CleanupTable {
    fn has_path(&self) -> bool {
        self.key.starts_with(sql::PATH_COLUMN)
    }
}

struct CandidateSet {
    sql: String,
    prune: Option<String>,
}

enum PathGroup {
    Paths(Vec<String>),
    Chunked { path: String, chunks: usize },
}

/// Parts a statement must leave alone this pass: a patch written against a merging or mutating part
/// is applied to the result by `(_block_number, _block_offset)` join, the slow path on multi-billion-row parts.
enum Busy {
    Mutating,
    Parts(Vec<String>),
}

impl Busy {
    fn defers(&self) -> bool {
        match self {
            Busy::Mutating => true,
            Busy::Parts(parts) => !parts.is_empty(),
        }
    }
}

#[derive(Default, Clone, Copy, PartialEq, Eq, Debug)]
struct BlockCursor {
    last_pass: Option<DateTime<Utc>>,
    block: u64,
    previous_block: u64,
    last_purge: Option<DateTime<Utc>>,
    /// Start of the oldest window a pass could not finish because parts were busy; re-run once nothing is.
    hold_block: Option<u64>,
}

impl BlockCursor {
    /// Busy passes keep their normal window and remember where the backlog starts;
    /// the first quiet pass re-reads from there so rows skipped in merging parts are caught up once.
    fn window_start(&self, busy: bool) -> u64 {
        match self.hold_block {
            Some(hold) if !busy => hold,
            _ => self.previous_block,
        }
    }

    fn next_hold(&self, busy: bool) -> Option<u64> {
        busy.then_some(self.hold_block.unwrap_or(self.previous_block))
    }
}

struct Topology {
    database: String,
    cluster: Option<String>,
}

pub struct TableCleanup {
    graph: ArrowClickHouseClient,
    checkpoints: Arc<dyn CheckpointStore>,
    tables: Vec<CleanupTable>,
    code_checkpoint_table: String,
    code_branch_table: String,
    code_file_table: String,
    metrics: ScheduledTaskMetrics,
    config: TableCleanupConfig,
    prepared: AtomicBool,
    supported: AtomicBool,
    path_list_bytes: AtomicUsize,
    topology: OnceLock<Topology>,
    unsafe_tables: Mutex<BTreeSet<String>>,
    /// Seeded at start so a restart never triggers `APPLY PATCHES` on every table at once.
    last_patch_apply: Mutex<Instant>,
}

impl TableCleanup {
    pub fn new(
        graph: ArrowClickHouseClient,
        ontology: &ontology::Ontology,
        code_tables: &CodeTableNames,
        checkpoints: Arc<dyn CheckpointStore>,
        metrics: ScheduledTaskMetrics,
        config: TableCleanupConfig,
    ) -> Self {
        Self {
            graph,
            checkpoints,
            tables: cleanup_tables(ontology, code_tables),
            code_checkpoint_table: prefixed_table_name(
                CODE_INDEXING_CHECKPOINT_TABLE,
                *SCHEMA_VERSION,
            ),
            code_branch_table: code_tables.branch.clone(),
            code_file_table: code_tables.file.clone(),
            metrics,
            config,
            prepared: AtomicBool::new(false),
            supported: AtomicBool::new(false),
            path_list_bytes: AtomicUsize::new(MIN_PATH_LIST_BYTES),
            topology: OnceLock::new(),
            unsafe_tables: Mutex::new(BTreeSet::new()),
            last_patch_apply: Mutex::new(Instant::now()),
        }
    }

    async fn rows(&self, query: ArrowQuery) -> Result<Vec<Vec<String>>, TaskError> {
        let batches = query.fetch_arrow().await.map_err(TaskError::new)?;
        let mut rows = Vec::new();
        for batch in batches {
            let columns: Vec<&StringArray> = batch
                .columns()
                .iter()
                .map(|column| {
                    column
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| TaskError::new("expected String columns"))
                })
                .collect::<Result<_, _>>()?;
            for row in 0..batch.num_rows() {
                rows.push(
                    columns
                        .iter()
                        .map(|column| column.value(row).to_string())
                        .collect(),
                );
            }
        }
        Ok(rows)
    }

    async fn column(&self, sql: &str) -> Result<Vec<String>, TaskError> {
        let rows = self.rows(self.graph.query(sql)).await?;
        Ok(rows.into_iter().map(|row| row[0].clone()).collect())
    }

    async fn scalar(&self, sql: &str) -> Result<u64, TaskError> {
        let rows = self.rows(self.graph.query(sql)).await?;
        Ok(rows
            .first()
            .and_then(|row| row.first())
            .and_then(|value| value.parse().ok())
            .unwrap_or(0))
    }

    async fn count(&self, sql: &str) -> Result<u64, TaskError> {
        self.scalar(&format!("SELECT toString(count()) FROM ({sql})"))
            .await
    }

    async fn execute(&self, table: &str, sql: &str) -> Result<(), TaskError> {
        let started = Instant::now();
        self.graph
            .query(sql)
            .execute()
            .await
            .map_err(TaskError::new)?;
        let elapsed = started.elapsed().as_secs_f64();
        self.metrics.record_query_duration(table, elapsed);
        debug!(
            table,
            statement_bytes = sql.len(),
            duration_ms = (elapsed * 1000.0) as u64,
            "table cleanup statement finished"
        );
        Ok(())
    }

    /// Tables under construction are left to the migration; the first pass after activation sweeps them whole.
    async fn version_active(&self) -> Result<bool, TaskError> {
        let active = read_active_version(&self.graph)
            .await
            .map_err(TaskError::new)?;
        Ok(active == Some(*SCHEMA_VERSION))
    }

    async fn prepare(&self) -> Result<bool, TaskError> {
        if self.prepared.load(Ordering::Acquire) {
            return Ok(self.supported.load(Ordering::Acquire));
        }
        let version = self
            .column("SELECT version()")
            .await?
            .pop()
            .unwrap_or_default();
        let has_setting = self
            .count("SELECT name FROM system.settings WHERE name = 'lightweight_delete_mode'")
            .await?
            > 0;
        if !has_setting || !sql::supports_patch_deletes(&version) {
            warn!(
                version,
                "ClickHouse is older than the first release with working patch-part deletes; table cleanup stays idle"
            );
            self.prepared.store(true, Ordering::Release);
            return Ok(false);
        }
        let max_query_size = self
            .scalar("SELECT value FROM system.settings WHERE name = 'max_query_size'")
            .await?;
        self.path_list_bytes
            .store(path_list_bytes(max_query_size as usize), Ordering::Release);
        let cluster = self
            .count(&sql::cluster_exists_sql(&self.config.merges_cluster))
            .await?
            > 0;
        if !cluster {
            warn!(
                cluster = self.config.merges_cluster,
                "cluster is not defined; only merges on this replica are excluded from deletes"
            );
        }
        let _ = self.topology.set(Topology {
            database: self.graph.database().to_string(),
            cluster: cluster.then(|| self.config.merges_cluster.clone()),
        });
        let mut unsafe_tables = self.unsafe_tables.lock().await;
        for table in &self.tables {
            if self.load_or_record_refusal(&table.name).await? {
                unsafe_tables.insert(table.name.clone());
            }
        }
        self.supported.store(true, Ordering::Release);
        self.prepared.store(true, Ordering::Release);
        Ok(true)
    }

    /// The verdict is stored per schema version; delete the identity row to re-check a rebuilt table.
    async fn load_or_record_refusal(&self, table: &str) -> Result<bool, TaskError> {
        let key = format!("{IDENTITY_KEY_PREFIX}{table}");
        let stored = self.checkpoints.load(&key).await.map_err(TaskError::new)?;
        if let Some(verdict) = stored.and_then(|checkpoint| checkpoint.cursor_values) {
            return Ok(verdict.first().is_some_and(|value| value == REFUSED));
        }
        let reason = self.unsafe_reason(table).await?;
        if let Some(reason) = reason {
            warn!(table, reason, "refusing table cleanup for this table");
        }
        self.record_verdict(table, reason.is_some()).await?;
        Ok(reason.is_some())
    }

    async fn record_verdict(&self, table: &str, refused: bool) -> Result<(), TaskError> {
        let verdict = if refused { REFUSED } else { ADMITTED };
        self.checkpoints
            .save_progress(
                &format!("{IDENTITY_KEY_PREFIX}{table}"),
                &Checkpoint {
                    watermark: Utc::now(),
                    cursor_values: Some(vec![verdict.to_string()]),
                    resume_floor: None,
                },
            )
            .await
            .map_err(TaskError::new)
    }

    /// A patch applied after a merge matches rows by `(_block_number, _block_offset)`, so that pair must be unique.
    async fn unsafe_reason(&self, table: &str) -> Result<Option<&'static str>, TaskError> {
        let checks = [
            (
                sql::block_settings_missing_sql(table),
                "table does not declare both block columns",
            ),
            (
                sql::offset_only_parts_sql(table),
                "parts persist _block_offset without _block_number",
            ),
            (
                sql::foreign_block_numbers_sql(table),
                "parts carry block numbers from another table",
            ),
        ];
        for (check, reason) in checks {
            if self.count(&check).await? > 0 {
                return Ok(Some(reason));
            }
        }
        Ok(None)
    }

    /// Merges on ClickHouse Cloud persist `_block_offset` alone unless the table DDL says otherwise; a table that starts doing so is refused for good.
    async fn ratchet_identity(&self) -> Result<(), TaskError> {
        let admitted: Vec<String> = self
            .safe_tables(|_| true)
            .await
            .into_iter()
            .map(|table| table.name.clone())
            .collect();
        for table in admitted {
            if self.count(&sql::offset_only_parts_sql(&table)).await? > 0 {
                warn!(
                    table,
                    "merged parts persist _block_offset without _block_number; refusing table cleanup for this table"
                );
                self.record_verdict(&table, true).await?;
                self.unsafe_tables.lock().await.insert(table);
            }
        }
        Ok(())
    }

    async fn safe_tables(&self, role: impl Fn(CodeRole) -> bool) -> Vec<&CleanupTable> {
        let unsafe_tables = self.unsafe_tables.lock().await;
        self.tables
            .iter()
            .filter(|table| role(table.code) && !unsafe_tables.contains(&table.name))
            .collect()
    }

    async fn busy(&self, table: &str) -> Result<Busy, TaskError> {
        let topology = self
            .topology
            .get()
            .ok_or_else(|| TaskError::new("table cleanup topology is not prepared"))?;
        if self
            .count(&sql::pending_mutations_sql(&topology.database, table))
            .await?
            > 0
        {
            return Ok(Busy::Mutating);
        }
        let parts = self
            .column(&sql::busy_parts_sql(
                &topology.database,
                table,
                topology.cluster.as_deref(),
            ))
            .await?;
        Ok(Busy::Parts(parts))
    }

    fn cursor_key(table: &str) -> String {
        format!("{CURSOR_KEY_PREFIX}{table}")
    }

    async fn block_cursor(&self, key: &str) -> Result<BlockCursor, TaskError> {
        let checkpoint = self.checkpoints.load(key).await.map_err(TaskError::new)?;
        Ok(checkpoint
            .map(|checkpoint| {
                let values: Vec<i64> = checkpoint
                    .cursor_values
                    .unwrap_or_default()
                    .iter()
                    .filter_map(|value| value.parse().ok())
                    .collect();
                BlockCursor {
                    last_pass: Some(checkpoint.watermark),
                    block: values.first().copied().unwrap_or(0).max(0) as u64,
                    previous_block: values.get(1).copied().unwrap_or(0).max(0) as u64,
                    last_purge: values
                        .get(2)
                        .filter(|secs| **secs > 0)
                        .and_then(|secs| DateTime::<Utc>::from_timestamp(*secs, 0)),
                    hold_block: values
                        .get(3)
                        .filter(|block| **block >= 0)
                        .map(|block| *block as u64),
                }
            })
            .unwrap_or_default())
    }

    async fn save_block_cursor(
        &self,
        key: &str,
        pass_at: DateTime<Utc>,
        cursor: &BlockCursor,
        high_block: u64,
        last_purge: Option<DateTime<Utc>>,
        hold_block: Option<u64>,
    ) -> Result<(), TaskError> {
        self.checkpoints
            .save_progress(
                key,
                &Checkpoint {
                    watermark: pass_at,
                    cursor_values: Some(vec![
                        high_block.to_string(),
                        cursor.block.to_string(),
                        last_purge.map(|at| at.timestamp()).unwrap_or(0).to_string(),
                        hold_block
                            .map(|block| block as i64)
                            .unwrap_or(NO_HOLD)
                            .to_string(),
                    ]),
                    resume_floor: None,
                },
            )
            .await
            .map_err(TaskError::new)
    }

    async fn high_block(&self, table: &str) -> Result<u64, TaskError> {
        self.scalar(&sql::high_block_sql(table)).await
    }

    async fn run_collapse(
        &self,
        table: &CleanupTable,
        candidate_sets: &[CandidateSet],
        keep: sql::Keep,
        exclude: &str,
    ) -> Result<(), TaskError> {
        for candidates in candidate_sets {
            let statement = sql::collapse_statement(
                &table.name,
                &table.key,
                &candidates.sql,
                candidates.prune.as_deref(),
                keep,
                exclude,
                self.config.statement_timeout_secs,
            );
            self.execute(&table.name, &statement).await?;
        }
        Ok(())
    }

    /// Path groups keep every statement primary-key-pruned; tables without a path column fall back to key hashes.
    async fn candidate_sets(
        &self,
        table: &CleanupTable,
        filter: &str,
    ) -> Result<(u64, Vec<CandidateSet>), TaskError> {
        let limit = self.config.max_candidates_per_statement.max(1);
        if !table.has_path() {
            let total = self
                .count(&sql::tombstone_rows_sql(&table.name, filter))
                .await?;
            let chunks = total.div_ceil(limit) as usize;
            let sets = (0..chunks)
                .map(|chunk| CandidateSet {
                    sql: sql::candidates_sql(
                        &table.name,
                        &table.key,
                        &format!("_deleted{filter}"),
                        (chunks > 1).then_some((chunks, chunk)),
                    ),
                    prune: None,
                })
                .collect();
            return Ok((total, sets));
        }
        let counts = self
            .rows(
                self.graph
                    .query(&sql::tombstones_per_path_sql(&table.name, filter)),
            )
            .await?
            .into_iter()
            .map(|row| (row[0].clone(), row[1].parse().unwrap_or(0)))
            .collect();
        let (total, groups) =
            group_paths(counts, limit, self.path_list_bytes.load(Ordering::Acquire));
        let mut sets = Vec::new();
        for group in groups {
            match group {
                PathGroup::Paths(paths) => {
                    sets.push(candidate_set(table, &paths, filter, None));
                }
                PathGroup::Chunked { path, chunks } => {
                    for chunk in 0..chunks {
                        sets.push(candidate_set(
                            table,
                            std::slice::from_ref(&path),
                            filter,
                            Some((chunks, chunk)),
                        ));
                    }
                }
            }
        }
        Ok((total, sets))
    }

    /// Younger tombstones stay so a late row with an older `_version` cannot resurface behind them.
    async fn purge_tombstones(
        &self,
        table: &CleanupTable,
        cutoff: DateTime<Utc>,
        exclude: &str,
    ) -> Result<u64, TaskError> {
        let (total, sets) = self
            .candidate_sets(table, &sql::version_filter("<", cutoff))
            .await?;
        self.run_collapse(
            table,
            &sets,
            sql::Keep::NewestUnlessExpiredTombstone(cutoff),
            exclude,
        )
        .await?;
        info!(
            table = table.name,
            tombstones = total,
            statements = sets.len(),
            "purged expired tombstones"
        );
        Ok(total)
    }

    /// Rows written before the block columns existed report their part's first block, invisible to the incremental window.
    async fn sweep_history(
        &self,
        table: &CleanupTable,
        cutoff: DateTime<Utc>,
        exclude: &str,
    ) -> Result<u64, TaskError> {
        let purged = self.purge_tombstones(table, cutoff, exclude).await?;
        let (total, sets) = self
            .candidate_sets(table, &sql::version_filter(">=", cutoff))
            .await?;
        self.run_collapse(table, &sets, sql::Keep::Newest, exclude)
            .await?;
        info!(
            table = table.name,
            tombstones = total,
            statements = sets.len(),
            "collapsed historical tombstoned keys"
        );
        Ok(purged + total)
    }

    /// Project-scoped code tables are left to `cleanup_code_snapshots`, which removes whole previous snapshots.
    async fn collapse_tombstones(
        &self,
        table: &CleanupTable,
        pass_at: DateTime<Utc>,
    ) -> Result<u64, TaskError> {
        let key = Self::cursor_key(&table.name);
        let cursor = self.block_cursor(&key).await?;
        let high_block = self.high_block(&table.name).await?.max(cursor.block);
        let cutoff = pass_at - TimeDelta::seconds(self.config.tombstone_retention_secs as i64);
        let busy = self.busy(&table.name).await?;
        let (exclude, deferred) = match &busy {
            Busy::Mutating => {
                info!(
                    table = table.name,
                    "a mutation is pending; deferring tombstone collapse"
                );
                (String::new(), true)
            }
            Busy::Parts(parts) => (sql::exclude_parts_sql(parts), !parts.is_empty()),
        };
        if cursor.last_pass.is_none() {
            let (total, purged_at) = match (self.config.sweep_history, &busy) {
                (false, _) | (_, Busy::Mutating) => (0, None),
                (true, Busy::Parts(_)) => (
                    self.sweep_history(table, cutoff, &exclude).await?,
                    Some(pass_at),
                ),
            };
            let swept = BlockCursor {
                block: high_block,
                ..cursor
            };
            let hold = (self.config.sweep_history && deferred).then_some(0);
            self.save_block_cursor(&key, pass_at, &swept, high_block, purged_at, hold)
                .await?;
            return Ok(total);
        }
        let mut total = 0;
        if !matches!(busy, Busy::Mutating) {
            let after = cursor.window_start(deferred);
            let (found, sets) = self
                .candidate_sets(table, &sql::new_rows_filter(after))
                .await?;
            total = found;
            if total > 0 {
                self.run_collapse(table, &sets, sql::Keep::Newest, &exclude)
                    .await?;
                info!(
                    table = table.name,
                    candidates = total,
                    statements = sets.len(),
                    catching_up = after < cursor.previous_block,
                    excluded_parts = !exclude.is_empty(),
                    "collapsed tombstoned keys"
                );
            }
        }
        let hold = cursor.next_hold(deferred);
        // Saved before the purge so a failing purge cannot stall the incremental window.
        self.save_block_cursor(&key, pass_at, &cursor, high_block, cursor.last_purge, hold)
            .await?;
        let purge_due = cursor.last_purge.is_none_or(|at| {
            pass_at - at >= TimeDelta::seconds(self.config.purge_interval_secs as i64) - PURGE_SLACK
        });
        if !purge_due || matches!(busy, Busy::Mutating) {
            return Ok(total);
        }
        let purged = self.purge_tombstones(table, cutoff, &exclude).await?;
        self.save_block_cursor(&key, pass_at, &cursor, high_block, Some(pass_at), hold)
            .await?;
        Ok(total + purged)
    }

    /// Scopes come from checkpoint rows by `_block_number`: a checkpoint lands at job end while `indexed_at` is the job start.
    async fn cleanup_code_snapshots(&self, pass_at: DateTime<Utc>) -> Result<u64, TaskError> {
        let key = Self::cursor_key(&self.code_checkpoint_table);
        let cursor = self.block_cursor(&key).await?;
        let high_block = self
            .high_block(&self.code_checkpoint_table)
            .await?
            .max(cursor.block);
        let history = cursor.last_pass.is_none();
        if history && !self.config.sweep_history {
            let swept = BlockCursor {
                block: high_block,
                ..cursor
            };
            self.save_block_cursor(&key, pass_at, &swept, high_block, None, None)
                .await?;
            return Ok(0);
        }
        let tables = self.safe_tables(|role| role != CodeRole::None).await;
        let mut excludes = Vec::with_capacity(tables.len());
        let mut deferred = false;
        for table in &tables {
            let busy = self.busy(&table.name).await?;
            deferred |= busy.defers();
            excludes.push(match busy {
                Busy::Mutating => {
                    info!(
                        table = table.name,
                        "a mutation is pending; deferring code snapshot cleanup"
                    );
                    None
                }
                Busy::Parts(parts) => Some(sql::exclude_parts_sql(&parts)),
            });
        }
        let changed = if history {
            sql::multi_snapshot_scopes_sql(&self.code_file_table)
        } else {
            sql::changed_scopes_sql(&self.code_checkpoint_table, cursor.window_start(deferred))
        };
        let scopes: Vec<sql::Scope> = self
            .rows(self.graph.query(&sql::code_scopes_sql(
                &self.code_checkpoint_table,
                &self.code_branch_table,
                &changed,
            )))
            .await?
            .into_iter()
            .map(|row| sql::Scope {
                path: row[0].clone(),
                project_id: row[1].clone(),
                branch: row[2].clone(),
                bound: row[3].clone(),
            })
            .collect();
        let total = scopes.len() as u64;
        let chunks = scopes.chunks(SCOPES_PER_STATEMENT).count();
        let mut failed_chunks = 0usize;
        for (chunk, scopes) in scopes.chunks(SCOPES_PER_STATEMENT).enumerate() {
            let mut paths: Vec<String> = scopes.iter().map(|scope| scope.path.clone()).collect();
            paths.dedup();
            let prune = sql::path_prune_sql(&paths);
            let scopes = sql::scopes_literal_sql(scopes);
            for (table, exclude) in tables.iter().zip(&excludes) {
                let Some(exclude) = exclude else {
                    continue;
                };
                let statement = match table.code {
                    CodeRole::Project => sql::code_snapshot_statement(
                        &table.name,
                        &scopes,
                        &prune,
                        exclude,
                        self.config.statement_timeout_secs,
                    ),
                    CodeRole::SharedEdge => sql::shared_edge_snapshot_statement(
                        &table.name,
                        &self.code_checkpoint_table,
                        &scopes,
                        &prune,
                        exclude,
                        self.config.statement_timeout_secs,
                    ),
                    CodeRole::None => continue,
                };
                // A failed history chunk is left to the project's next re-index instead of repeating the whole sweep.
                match self.execute(&table.name, &statement).await {
                    Ok(()) => {}
                    Err(error) if history => {
                        failed_chunks += 1;
                        self.metrics.record_error(TASK_NAME, "code_history");
                        warn!(table = table.name, chunk, %error, "code history chunk failed");
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        if total > 0 {
            info!(
                scopes = total,
                chunks, failed_chunks, history, deferred, "removed superseded code snapshots"
            );
        }
        let saved = if history {
            BlockCursor {
                block: high_block,
                ..cursor
            }
        } else {
            cursor
        };
        let hold = if history {
            deferred.then_some(0)
        } else {
            cursor.next_hold(deferred)
        };
        self.save_block_cursor(&key, pass_at, &saved, high_block, None, hold)
            .await?;
        Ok(total)
    }

    /// The largest parts never merge, so their patches are folded in by size or age.
    async fn apply_patches_if_due(&self) -> Result<(), TaskError> {
        let mut last = self.last_patch_apply.lock().await;
        let overdue = last.elapsed().as_secs() >= self.config.apply_patches_after_secs;
        let names: Vec<String> = self
            .safe_tables(|_| true)
            .await
            .into_iter()
            .map(|table| table.name.clone())
            .collect();
        if names.is_empty() {
            return Ok(());
        }
        let pending: BTreeSet<String> = self
            .column(sql::pending_apply_patches_sql())
            .await?
            .into_iter()
            .collect();
        let mut applied = 0usize;
        for row in self
            .rows(self.graph.query(&sql::patch_bytes_sql(&names)))
            .await?
        {
            let bytes: u64 = row[1].parse().unwrap_or(0);
            let due = overdue || bytes >= self.config.apply_patches_after_bytes;
            if pending.contains(&row[0]) || !due {
                continue;
            }
            self.graph
                .query(&sql::apply_patches_statement(&row[0]))
                .execute()
                .await
                .map_err(TaskError::new)?;
            applied += 1;
        }
        if applied > 0 {
            info!(tables = applied, "applied pending patch parts");
        }
        if overdue {
            *last = Instant::now();
        }
        Ok(())
    }
}

fn cleanup_tables(
    ontology: &ontology::Ontology,
    code_tables: &CodeTableNames,
) -> Vec<CleanupTable> {
    let mut tables: Vec<CleanupTable> = ontology
        .nodes()
        .map(|node| node.destination_table.as_str())
        .chain(ontology.edge_tables())
        .filter_map(|logical| {
            let sort_key = ontology.sort_key_for_table(logical)?;
            let name = prefixed_table_name(logical, *SCHEMA_VERSION);
            let code = code_role(code_tables, &name, sort_key);
            Some(CleanupTable {
                name,
                key: sort_key.join(", "),
                code,
            })
        })
        .collect();
    tables.sort_by(|a, b| a.name.cmp(&b.name));
    tables
}

fn code_role(code_tables: &CodeTableNames, table: &str, sort_key: &[String]) -> CodeRole {
    if code_tables.node_tables().contains(&table) {
        CodeRole::Project
    } else if code_tables.edge_table_names().contains(&table) {
        if sort_key.iter().any(|column| column == "project_id") {
            CodeRole::Project
        } else {
            CodeRole::SharedEdge
        }
    } else {
        CodeRole::None
    }
}

fn candidate_set(
    table: &CleanupTable,
    paths: &[String],
    filter: &str,
    chunk: Option<(usize, usize)>,
) -> CandidateSet {
    let prune = sql::path_prune_sql(paths);
    CandidateSet {
        sql: sql::candidates_sql(
            &table.name,
            &table.key,
            &format!("{prune} AND _deleted{filter}"),
            chunk,
        ),
        prune: Some(prune),
    }
}

fn path_list_bytes(max_query_size: usize) -> usize {
    (max_query_size / PATH_LIST_SHARE).clamp(MIN_PATH_LIST_BYTES, MAX_PATH_LIST_BYTES)
}

fn group_paths(counts: Vec<(String, u64)>, limit: u64, list_bytes: usize) -> (u64, Vec<PathGroup>) {
    let mut groups = Vec::new();
    let mut group = Vec::new();
    let mut group_size = 0u64;
    let mut group_bytes = 0usize;
    let mut total = 0u64;
    for (path, count) in counts {
        total += count;
        if count > limit {
            let chunks = count.div_ceil(limit) as usize;
            groups.push(PathGroup::Chunked { path, chunks });
            continue;
        }
        let bytes = sql::list_item_len(&path);
        let full = group_bytes + bytes > list_bytes || group_size + count > limit;
        if !group.is_empty() && full {
            groups.push(PathGroup::Paths(std::mem::take(&mut group)));
            group_size = 0;
            group_bytes = 0;
        }
        group.push(path);
        group_size += count;
        group_bytes += bytes;
    }
    if !group.is_empty() {
        groups.push(PathGroup::Paths(group));
    }
    (total, groups)
}

#[async_trait]
impl ScheduledTask for TableCleanup {
    fn name(&self) -> &str {
        TASK_NAME
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let started = Instant::now();
        let ready = match self.version_active().await {
            Ok(true) => self.prepare().await,
            Ok(false) => {
                info!(
                    version = *SCHEMA_VERSION,
                    "schema version is not active yet; table cleanup waits for the migration"
                );
                Ok(false)
            }
            Err(error) => Err(error),
        };
        let supported = match ready {
            Ok(supported) => supported,
            Err(error) => {
                self.metrics.record_error(TASK_NAME, "prepare");
                self.metrics
                    .record_run(TASK_NAME, "error", started.elapsed().as_secs_f64());
                return Err(error);
            }
        };
        if !supported {
            self.metrics.record_requests_skipped(TASK_NAME, 1);
            return Ok(());
        }
        let mut failed = 0usize;
        if let Err(error) = self.ratchet_identity().await {
            failed += 1;
            self.metrics.record_error(TASK_NAME, "identity");
            warn!(%error, "block identity check failed");
        }
        let skipped = self.unsafe_tables.lock().await.len() as u64;
        if skipped > 0 {
            self.metrics.record_requests_skipped(TASK_NAME, skipped);
        }
        let pass_at = Utc::now();
        let mut candidates = 0u64;
        match self.cleanup_code_snapshots(pass_at).await {
            Ok(scopes) => candidates += scopes,
            Err(error) => {
                failed += 1;
                self.metrics.record_error(TASK_NAME, "code_snapshots");
                warn!(%error, "code snapshot cleanup failed");
            }
        }
        let mut sweeps = Vec::new();
        for table in self.safe_tables(|role| role != CodeRole::Project).await {
            sweeps.push(async move { (table, self.collapse_tombstones(table, pass_at).await) });
        }
        let outcomes: Vec<(&CleanupTable, Result<u64, TaskError>)> = futures::stream::iter(sweeps)
            .buffer_unordered(self.config.concurrent_tables.max(1))
            .collect()
            .await;
        for (table, outcome) in outcomes {
            match outcome {
                Ok(count) => candidates += count,
                Err(error) => {
                    failed += 1;
                    self.metrics.record_error(TASK_NAME, "collapse");
                    warn!(table = table.name, %error, "tombstone collapse failed");
                }
            }
        }
        if let Err(error) = self.apply_patches_if_due().await {
            failed += 1;
            self.metrics.record_error(TASK_NAME, "apply_patches");
            warn!(%error, "applying patch parts failed");
        }
        let outcome = if failed == 0 { "success" } else { "error" };
        self.metrics
            .record_run(TASK_NAME, outcome, started.elapsed().as_secs_f64());
        info!(candidates, failed, "table cleanup pass complete");
        if failed > 0 {
            return Err(TaskError::new(format!("{failed} cleanup steps failed")));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paths(group: &PathGroup) -> Vec<&str> {
        match group {
            PathGroup::Paths(paths) => paths.iter().map(String::as_str).collect(),
            PathGroup::Chunked { path, .. } => vec![path.as_str()],
        }
    }

    #[test]
    fn every_node_and_edge_table_is_cleaned_and_no_auxiliary_table_is() {
        let ontology = ontology::Ontology::load_embedded().unwrap();
        let code_tables = CodeTableNames::from_ontology(&ontology).unwrap();
        let tables = cleanup_tables(&ontology, &code_tables);
        assert_eq!(
            tables.len(),
            ontology.nodes().count() + ontology.edge_tables().len()
        );
        assert!(
            tables
                .iter()
                .all(|table| !table.name.contains("checkpoint"))
        );
        assert!(
            tables.iter().any(
                |table| table.name.ends_with("gl_code_edge") && table.code == CodeRole::Project
            )
        );
        assert!(
            tables
                .iter()
                .any(|table| table.name.ends_with("_gl_edge") && table.code == CodeRole::SharedEdge)
        );
    }

    #[test]
    fn paths_are_grouped_up_to_the_limit_and_split_when_one_path_exceeds_it() {
        let counts = vec![
            ("a".to_string(), 3),
            ("b".to_string(), 3),
            ("c".to_string(), 12),
            ("d".to_string(), 1),
            ("e".to_string(), 4),
        ];
        let (total, groups) = group_paths(counts, 5, MIN_PATH_LIST_BYTES);
        assert_eq!(total, 23);
        assert_eq!(groups.len(), 4);
        assert_eq!(paths(&groups[0]), ["a"]);
        assert!(matches!(&groups[1], PathGroup::Chunked { path, chunks: 3 } if path == "c"));
        assert_eq!(paths(&groups[2]), ["b", "d"]);
        assert_eq!(paths(&groups[3]), ["e"]);
    }

    #[test]
    fn a_group_never_exceeds_the_path_list_byte_budget() {
        let path = "1/".repeat(50);
        let per_group = MIN_PATH_LIST_BYTES / sql::list_item_len(&path);
        let counts = (0..per_group * 2 + 1).map(|_| (path.clone(), 1)).collect();
        let (total, groups) = group_paths(counts, 1_000_000, MIN_PATH_LIST_BYTES);
        assert_eq!(total, (per_group * 2 + 1) as u64);
        assert_eq!(
            groups
                .iter()
                .map(|group| paths(group).len())
                .collect::<Vec<_>>(),
            [per_group, per_group, 1]
        );
    }

    #[test]
    fn the_path_list_budget_follows_the_session_query_size_within_bounds() {
        assert_eq!(path_list_bytes(256 * 1024), 32 * 1024);
        assert_eq!(path_list_bytes(1024), 32 * 1024);
        assert_eq!(path_list_bytes(10 * 1024 * 1024), 1024 * 1024);
        assert_eq!(path_list_bytes(64 * 1024 * 1024), 1024 * 1024);
    }

    #[test]
    fn a_chunked_candidate_set_prunes_by_its_single_path() {
        let table = CleanupTable {
            name: "t".to_string(),
            key: "traversal_path, id".to_string(),
            code: CodeRole::None,
        };
        let set = candidate_set(&table, &["1/2/".to_string()], " AND x", Some((3, 1)));
        assert_eq!(set.prune.as_deref(), Some("traversal_path IN ('1/2/')"));
        assert_eq!(
            set.sql,
            "SELECT traversal_path, id FROM t WHERE traversal_path IN ('1/2/') AND _deleted AND x AND cityHash64(traversal_path, id) % 3 = 1"
        );
    }

    #[test]
    fn busy_passes_keep_their_window_and_the_first_quiet_pass_catches_up_from_the_hold() {
        let quiet = BlockCursor {
            block: 90,
            previous_block: 70,
            ..Default::default()
        };
        assert_eq!(quiet.window_start(false), 70);
        assert_eq!(quiet.next_hold(false), None);
        assert_eq!(quiet.window_start(true), 70);
        assert_eq!(quiet.next_hold(true), Some(70));

        let held = BlockCursor {
            block: 120,
            previous_block: 90,
            hold_block: Some(70),
            ..Default::default()
        };
        assert_eq!(held.window_start(true), 90);
        assert_eq!(held.next_hold(true), Some(70));
        assert_eq!(held.window_start(false), 70);
        assert_eq!(held.next_hold(false), None);
    }
}
