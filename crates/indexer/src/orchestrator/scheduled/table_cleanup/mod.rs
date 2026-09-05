mod sql;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use arrow::array::{Array, StringArray};
use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use tokio::sync::Mutex;
use tracing::{debug, info, warn};

use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::clickhouse::{ArrowClickHouseClient, ArrowQuery};
use crate::modules::code::checkpoint::CODE_INDEXING_CHECKPOINT_TABLE;
use crate::modules::code::config::CodeTableNames;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use orbit_server_config::{ScheduleConfiguration, TableCleanupConfig};

const TASK_NAME: &str = "maintenance.table_cleanup";
const CURSOR_KEY_PREFIX: &str = "maintenance.table_cleanup.";
/// A refusal must outlive the condition that caused it: attached parts stop looking foreign once the table's own numbering passes them.
const IDENTITY_KEY_PREFIX: &str = "maintenance.table_cleanup.identity.";
const REFUSED: &str = "refused";
const ADMITTED: &str = "admitted";
/// Bounds the scope tuples and path literals inlined into one code statement.
const SCOPES_PER_STATEMENT: u64 = 2000;
/// Each path literal appears four times per statement; 500 keeps it under ClickHouse's default 256 KiB `max_query_size`.
const PATHS_PER_STATEMENT: usize = 500;
/// Cron passes land a few seconds after the minute, so an exact interval would skip a pass.
const PURGE_SLACK: TimeDelta = TimeDelta::seconds(60);

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

#[derive(Default)]
struct BlockCursor {
    last_pass: Option<DateTime<Utc>>,
    block: u64,
    previous_block: u64,
    last_purge: Option<DateTime<Utc>>,
}

pub struct TableCleanup {
    graph: ArrowClickHouseClient,
    checkpoints: Arc<dyn CheckpointStore>,
    tables: Vec<CleanupTable>,
    code_checkpoint_table: String,
    code_branch_table: String,
    metrics: ScheduledTaskMetrics,
    config: TableCleanupConfig,
    prepared: AtomicBool,
    supported: AtomicBool,
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
            metrics,
            config,
            prepared: AtomicBool::new(false),
            supported: AtomicBool::new(false),
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
        let mut unsafe_tables = self.unsafe_tables.lock().await;
        for table in &self.tables {
            if self.refused(&table.name).await? {
                unsafe_tables.insert(table.name.clone());
            }
        }
        self.supported.store(true, Ordering::Release);
        self.prepared.store(true, Ordering::Release);
        Ok(true)
    }

    /// The verdict is stored per schema version; delete the identity row to re-check a rebuilt table.
    async fn refused(&self, table: &str) -> Result<bool, TaskError> {
        let key = format!("{IDENTITY_KEY_PREFIX}{table}");
        let stored = self.checkpoints.load(&key).await.map_err(TaskError::new)?;
        if let Some(verdict) = stored.and_then(|checkpoint| checkpoint.cursor_values) {
            return Ok(verdict.first().is_some_and(|value| value == REFUSED));
        }
        let reason = self.unsafe_reason(table).await?;
        if let Some(reason) = reason {
            warn!(table, reason, "refusing table cleanup for this table");
        }
        let verdict = if reason.is_some() { REFUSED } else { ADMITTED };
        self.checkpoints
            .save_progress(
                &key,
                &Checkpoint {
                    watermark: Utc::now(),
                    cursor_values: Some(vec![verdict.to_string()]),
                    resume_floor: None,
                },
            )
            .await
            .map_err(TaskError::new)?;
        Ok(reason.is_some())
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

    async fn safe_tables(&self, role: impl Fn(CodeRole) -> bool) -> Vec<&CleanupTable> {
        let unsafe_tables = self.unsafe_tables.lock().await;
        self.tables
            .iter()
            .filter(|table| role(table.code) && !unsafe_tables.contains(&table.name))
            .collect()
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
    ) -> Result<(), TaskError> {
        for candidates in candidate_sets {
            let statement = sql::collapse_statement(
                &table.name,
                &table.key,
                &candidates.sql,
                candidates.prune.as_deref(),
                keep,
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
                .count(&sql::tombstone_count_sql(&table.name, filter))
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
        let (total, groups) = group_paths(counts, limit);
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
    ) -> Result<u64, TaskError> {
        let (total, sets) = self
            .candidate_sets(table, &sql::version_filter("<", cutoff))
            .await?;
        self.run_collapse(
            table,
            &sets,
            sql::Keep::NewestUnlessExpiredTombstone(cutoff),
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
    ) -> Result<u64, TaskError> {
        let purged = self.purge_tombstones(table, cutoff).await?;
        let (total, sets) = self
            .candidate_sets(table, &sql::version_filter(">=", cutoff))
            .await?;
        self.run_collapse(table, &sets, sql::Keep::Newest).await?;
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
        if cursor.last_pass.is_none() {
            let total = if self.config.sweep_history {
                self.sweep_history(table, cutoff).await?
            } else {
                0
            };
            let swept = BlockCursor {
                block: high_block,
                ..cursor
            };
            self.save_block_cursor(&key, pass_at, &swept, high_block, Some(pass_at))
                .await?;
            return Ok(total);
        }
        let (total, sets) = self
            .candidate_sets(table, &sql::new_rows_filter(cursor.previous_block))
            .await?;
        if total > 0 {
            self.run_collapse(table, &sets, sql::Keep::Newest).await?;
            info!(
                table = table.name,
                candidates = total,
                statements = sets.len(),
                "collapsed tombstoned keys"
            );
        }
        // Saved before the purge so a failing purge cannot stall the incremental window.
        self.save_block_cursor(&key, pass_at, &cursor, high_block, cursor.last_purge)
            .await?;
        let purge_due = cursor.last_purge.is_none_or(|at| {
            pass_at - at >= TimeDelta::seconds(self.config.purge_interval_secs as i64) - PURGE_SLACK
        });
        if !purge_due {
            return Ok(total);
        }
        let purged = self.purge_tombstones(table, cutoff).await?;
        self.save_block_cursor(&key, pass_at, &cursor, high_block, Some(pass_at))
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
            self.save_block_cursor(&key, pass_at, &swept, high_block, None)
                .await?;
            return Ok(0);
        }
        let after_block = (!history).then_some(cursor.previous_block);
        let scopes = |chunk| {
            sql::code_scopes_sql(
                &self.code_checkpoint_table,
                &self.code_branch_table,
                after_block,
                chunk,
            )
        };
        let total = self.count(&scopes(None)).await?;
        let chunks = total.div_ceil(SCOPES_PER_STATEMENT) as usize;
        let tables = self.safe_tables(|role| role != CodeRole::None).await;
        let mut failed_chunks = 0usize;
        for chunk in 0..chunks {
            let scopes = scopes((chunks > 1).then_some((chunks, chunk)));
            let paths = self.column(&sql::scope_paths_sql(&scopes)).await?;
            if paths.is_empty() {
                continue;
            }
            let prune = sql::path_prune_sql(&paths);
            for table in &tables {
                let statement = match table.code {
                    CodeRole::Project => sql::code_snapshot_statement(
                        &table.name,
                        &scopes,
                        &prune,
                        self.config.statement_timeout_secs,
                    ),
                    CodeRole::SharedEdge => sql::shared_edge_snapshot_statement(
                        &table.name,
                        &self.code_checkpoint_table,
                        &scopes,
                        &prune,
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
                chunks, failed_chunks, history, "removed superseded code snapshots"
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
        self.save_block_cursor(&key, pass_at, &saved, high_block, None)
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

fn group_paths(counts: Vec<(String, u64)>, limit: u64) -> (u64, Vec<PathGroup>) {
    let mut groups = Vec::new();
    let mut group = Vec::new();
    let mut group_size = 0u64;
    let mut total = 0u64;
    for (path, count) in counts {
        total += count;
        if count > limit {
            let chunks = count.div_ceil(limit) as usize;
            groups.push(PathGroup::Chunked { path, chunks });
            continue;
        }
        let full = group.len() >= PATHS_PER_STATEMENT || group_size + count > limit;
        if !group.is_empty() && full {
            groups.push(PathGroup::Paths(std::mem::take(&mut group)));
            group_size = 0;
        }
        group.push(path);
        group_size += count;
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
        let supported = match self.prepare().await {
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
        let skipped = self.unsafe_tables.lock().await.len() as u64;
        if skipped > 0 {
            self.metrics.record_requests_skipped(TASK_NAME, skipped);
        }
        let pass_at = Utc::now();
        let mut failed = 0usize;
        let mut candidates = 0u64;
        match self.cleanup_code_snapshots(pass_at).await {
            Ok(scopes) => candidates += scopes,
            Err(error) => {
                failed += 1;
                self.metrics.record_error(TASK_NAME, "code_snapshots");
                warn!(%error, "code snapshot cleanup failed");
            }
        }
        for table in self.safe_tables(|role| role != CodeRole::Project).await {
            match self.collapse_tombstones(table, pass_at).await {
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
        let (total, groups) = group_paths(counts, 5);
        assert_eq!(total, 23);
        assert_eq!(groups.len(), 4);
        assert_eq!(paths(&groups[0]), ["a"]);
        assert!(matches!(&groups[1], PathGroup::Chunked { path, chunks: 3 } if path == "c"));
        assert_eq!(paths(&groups[2]), ["b", "d"]);
        assert_eq!(paths(&groups[3]), ["e"]);
    }

    #[test]
    fn a_group_never_holds_more_paths_than_one_statement_can_carry() {
        let counts = (0..PATHS_PER_STATEMENT * 2 + 1)
            .map(|i| (format!("1/{i}/"), 1))
            .collect();
        let (total, groups) = group_paths(counts, 1_000_000);
        assert_eq!(total, (PATHS_PER_STATEMENT * 2 + 1) as u64);
        assert_eq!(
            groups
                .iter()
                .map(|group| paths(group).len())
                .collect::<Vec<_>>(),
            [PATHS_PER_STATEMENT, PATHS_PER_STATEMENT, 1]
        );
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
}
