use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use arrow::array::{Array, StringArray};
use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::clickhouse::{ArrowClickHouseClient, ArrowQuery, TIMESTAMP_FORMAT};
use crate::modules::code::checkpoint::CODE_INDEXING_CHECKPOINT_TABLE;
use crate::modules::code::config::CodeTableNames;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use orbit_server_config::{ScheduleConfiguration, TableCleanupConfig};

const TASK_NAME: &str = "maintenance.table_cleanup";
const CURSOR_KEY_PREFIX: &str = "maintenance.table_cleanup.";
const PATCH_DELETE_MODE: &str = "lightweight_update_force";
const PATCH_PART_PREFIX: &str = "patch";
const REQUIRED_SETTING: &str = "lightweight_delete_mode";
const CODE_SCOPE: &str = "traversal_path, project_id, branch";
const PATH_COLUMN: &str = "traversal_path";
const SCOPES_PER_STATEMENT: u64 = 2000;
const PURGE_SLACK: TimeDelta = TimeDelta::seconds(60);
/// Part names are `<partition>_<min block>_<max block>_<level>[_<mutation>]`.
const PART_MAX_BLOCK: &str = "toUInt64OrZero(splitByChar('_', _part)[3])";

#[derive(Clone, Copy, PartialEq, Eq)]
enum CodeRole {
    None,
    Project,
    SharedEdge,
}

struct ReclaimTable {
    name: String,
    key: String,
    code: CodeRole,
}

struct CandidateSet {
    sql: String,
    prune: Option<String>,
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
    tables: Vec<ReclaimTable>,
    code_checkpoint_table: String,
    code_branch_table: String,
    metrics: ScheduledTaskMetrics,
    config: TableCleanupConfig,
    prepared: AtomicBool,
    supported: AtomicBool,
    unsafe_tables: Mutex<BTreeSet<String>>,
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
        let mut tables: Vec<ReclaimTable> = ontology
            .nodes()
            .map(|node| node.destination_table.as_str())
            .chain(ontology.edge_tables())
            .filter_map(|logical| {
                let sort_key = ontology.sort_key_for_table(logical)?;
                let name = prefixed_table_name(logical, *SCHEMA_VERSION);
                let code = if code_tables.node_tables().contains(&name.as_str()) {
                    CodeRole::Project
                } else if code_tables.edge_table_names().contains(&name.as_str()) {
                    if sort_key.iter().any(|column| column == "project_id") {
                        CodeRole::Project
                    } else {
                        CodeRole::SharedEdge
                    }
                } else {
                    CodeRole::None
                };
                Some(ReclaimTable {
                    name,
                    key: sort_key.join(", "),
                    code,
                })
            })
            .collect();
        tables.sort_by(|a, b| a.name.cmp(&b.name));
        Self {
            graph,
            checkpoints,
            tables,
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
        info!(
            table,
            statement_bytes = sql.len(),
            duration_ms = (elapsed * 1000.0) as u64,
            "table cleanup statement finished"
        );
        Ok(())
    }

    /// Patch-part deletes need a ClickHouse release that runs their subqueries correctly and the
    /// block columns on every graph table. A table whose parts carry block numbers above their own range was cloned with
    /// `ATTACH PARTITION FROM`; a patch applied to such a part after a merge can hit a look-alike
    /// row, so the task refuses to touch it until the table is renumbered.
    async fn prepare(&self) -> Result<bool, TaskError> {
        if self.prepared.load(Ordering::Acquire) {
            return Ok(self.supported.load(Ordering::Acquire));
        }
        let version = self
            .rows(self.graph.query("SELECT version()"))
            .await?
            .first()
            .and_then(|row| row.first().cloned())
            .unwrap_or_default();
        let has_setting = self
            .count(&format!(
                "SELECT name FROM system.settings WHERE name = '{REQUIRED_SETTING}'"
            ))
            .await?
            > 0;
        if !has_setting || !supports_patch_deletes(&version) {
            warn!(
                version,
                "ClickHouse is older than the first release with working patch-part deletes; table cleanup stays idle"
            );
            self.prepared.store(true, Ordering::Release);
            return Ok(false);
        }
        let mut unsafe_tables = self.unsafe_tables.lock().await;
        for table in &self.tables {
            if self.count(&block_settings_missing_sql(&table.name)).await? > 0 {
                warn!(
                    table = table.name,
                    "table does not persist both block columns; skipping table cleanup until its DDL declares them"
                );
                unsafe_tables.insert(table.name.clone());
            } else if self.count(&offset_only_parts_sql(&table.name)).await? > 0 {
                warn!(
                    table = table.name,
                    "parts persist _block_offset without _block_number, so patch deletes could hit look-alike rows; skipping table cleanup until the table is rebuilt"
                );
                unsafe_tables.insert(table.name.clone());
            } else if self.count(&foreign_block_numbers_sql(&table.name)).await? > 0 {
                warn!(
                    table = table.name,
                    "parts carry block numbers from another table; skipping table cleanup until the table is rebuilt"
                );
                unsafe_tables.insert(table.name.clone());
            }
        }
        self.supported.store(true, Ordering::Release);
        self.prepared.store(true, Ordering::Release);
        Ok(true)
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
        self.scalar(&format!(
            "SELECT toString(max(max_block_number)) FROM system.parts \
             WHERE database = currentDatabase() AND table = '{table}' AND active \
               AND NOT startsWith(name, '{PATCH_PART_PREFIX}')"
        ))
        .await
    }

    async fn run_collapse(
        &self,
        table: &ReclaimTable,
        candidate_sets: &[CandidateSet],
        keep_newest_tombstone: bool,
    ) -> Result<(), TaskError> {
        for candidates in candidate_sets {
            let statement = collapse_statement(
                &table.name,
                &table.key,
                &candidates.sql,
                candidates.prune.as_deref(),
                keep_newest_tombstone,
                self.config.statement_timeout_secs,
            );
            self.execute(&table.name, &statement).await?;
        }
        Ok(())
    }

    /// Candidate sets for every tombstoned key matching `filter`, each bounded by
    /// `max_candidates_per_statement`: grouped by traversal path where the sort key lets the
    /// primary key prune, split by key hash otherwise.
    async fn candidate_sets(
        &self,
        table: &ReclaimTable,
        filter: &str,
    ) -> Result<(u64, Vec<CandidateSet>), TaskError> {
        let limit = self.config.max_candidates_per_statement;
        if !table.key.starts_with(PATH_COLUMN) {
            let total = self
                .count(&format!(
                    "SELECT 1 FROM {} WHERE _deleted{filter}",
                    table.name
                ))
                .await?;
            let chunks = total.div_ceil(limit) as usize;
            let sets = (0..chunks)
                .map(|chunk| CandidateSet {
                    sql: hash_chunk_sql(
                        &table.name,
                        &table.key,
                        &format!("_deleted{filter}"),
                        chunks,
                        chunk,
                    ),
                    prune: None,
                })
                .collect();
            return Ok((total, sets));
        }
        let rows = self
            .rows(self.graph.query(&format!(
                "SELECT {PATH_COLUMN}, toString(count()) FROM {} WHERE _deleted{filter} \
                 GROUP BY {PATH_COLUMN} ORDER BY {PATH_COLUMN}",
                table.name
            )))
            .await?;
        let mut sets = Vec::new();
        let mut group: Vec<String> = Vec::new();
        let mut group_size = 0u64;
        let mut total = 0u64;
        for row in rows {
            let count: u64 = row[1].parse().unwrap_or(0);
            total += count;
            if count > limit {
                let chunks = count.div_ceil(limit) as usize;
                let path_filter =
                    format!("{PATH_COLUMN} = '{}' AND _deleted{filter}", escape(&row[0]));
                for chunk in 0..chunks {
                    sets.push(CandidateSet {
                        sql: hash_chunk_sql(&table.name, &table.key, &path_filter, chunks, chunk),
                        prune: Some(path_prune_sql(std::slice::from_ref(&row[0]))),
                    });
                }
                continue;
            }
            if !group.is_empty() && group_size + count > limit {
                sets.push(CandidateSet {
                    sql: path_group_sql(&table.name, &table.key, &group, filter),
                    prune: Some(path_prune_sql(&group)),
                });
                group.clear();
                group_size = 0;
            }
            group.push(row[0].clone());
            group_size += count;
        }
        if !group.is_empty() {
            sets.push(CandidateSet {
                sql: path_group_sql(&table.name, &table.key, &group, filter),
                prune: Some(path_prune_sql(&group)),
            });
        }
        Ok((total, sets))
    }

    /// Removes dead keys whose newest tombstone is older than the retention window, with every
    /// row they hide. Younger tombstones stay so a late row with an older `_version` cannot
    /// resurface behind them.
    async fn purge_tombstones(
        &self,
        table: &ReclaimTable,
        cutoff: DateTime<Utc>,
    ) -> Result<u64, TaskError> {
        let filter = format!(
            " AND _version < toDateTime64('{}', 6, 'UTC')",
            cutoff.format(TIMESTAMP_FORMAT)
        );
        let (total, sets) = self.candidate_sets(table, &filter).await?;
        self.run_collapse(table, &sets, false).await?;
        info!(
            table = table.name,
            tombstones = total,
            statements = sets.len(),
            "purged expired tombstones"
        );
        Ok(total)
    }

    /// Rows written before the block columns existed report their part's first block number, so
    /// the incremental window cannot see them. The first pass for a table therefore purges its
    /// expired tombstones and collapses every remaining tombstoned key.
    async fn sweep_history(
        &self,
        table: &ReclaimTable,
        cutoff: DateTime<Utc>,
    ) -> Result<u64, TaskError> {
        let purged = self.purge_tombstones(table, cutoff).await?;
        let filter = format!(
            " AND _version >= toDateTime64('{}', 6, 'UTC')",
            cutoff.format(TIMESTAMP_FORMAT)
        );
        let (total, sets) = self.candidate_sets(table, &filter).await?;
        self.run_collapse(table, &sets, true).await?;
        info!(
            table = table.name,
            tombstones = total,
            statements = sets.len(),
            "collapsed historical tombstoned keys"
        );
        Ok(purged + total)
    }

    /// Removes the rows superseded by tombstones written since the previous pass, keeping each
    /// key's newest row. Tombstones themselves leave with `purge_tombstones` after the retention
    /// window, so a hidden row can never outlive its tombstone. Project-scoped code tables are
    /// not collapsed here: `reclaim_code_snapshots` removes their whole previous snapshot.
    async fn collapse_tombstones(
        &self,
        table: &ReclaimTable,
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
            .candidate_sets(table, &new_rows_filter(cursor.previous_block))
            .await?;
        if total > 0 {
            self.run_collapse(table, &sets, true).await?;
            info!(
                table = table.name,
                candidates = total,
                statements = sets.len(),
                "collapsed tombstoned keys"
            );
        }
        // Cron passes land a few seconds after the minute, so an exact interval would skip a pass.
        let purge_due = cursor.last_purge.is_none_or(|at| {
            pass_at - at >= TimeDelta::seconds(self.config.purge_interval_secs as i64) - PURGE_SLACK
        });
        let mut purged = 0;
        let last_purge = if purge_due {
            purged = self.purge_tombstones(table, cutoff).await?;
            Some(pass_at)
        } else {
            cursor.last_purge
        };
        self.save_block_cursor(&key, pass_at, &cursor, high_block, last_purge)
            .await?;
        Ok(total + purged)
    }

    /// Removes every code row older than its project's newest checkpoint: superseded snapshots,
    /// orphans of failed runs and the tombstones the cleaner wrote for vanished keys. Scopes are
    /// discovered by checkpoint rows inserted since the previous pass, because a checkpoint lands
    /// when the job ends while its `indexed_at` is the job start. A scope whose newest checkpoint
    /// wrote no Branch row recorded an empty repository, possibly after a transient fetch failure,
    /// and is left alone.
    async fn reclaim_code_snapshots(&self, pass_at: DateTime<Utc>) -> Result<u64, TaskError> {
        let key = Self::cursor_key(&self.code_checkpoint_table);
        let cursor = self.block_cursor(&key).await?;
        let high_block = self
            .high_block(&self.code_checkpoint_table)
            .await?
            .max(cursor.block);
        // The first pass covers every scope ever checkpointed; a failed chunk is logged and left
        // to the project's next re-index rather than repeating the whole sweep forever.
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
        let unsafe_tables = self.unsafe_tables.lock().await.clone();
        let after_block = if history {
            None
        } else {
            Some(cursor.previous_block)
        };
        let scopes = code_scopes_sql(
            &self.code_checkpoint_table,
            &self.code_branch_table,
            after_block,
            None,
        );
        let total = self.count(&scopes).await?;
        let chunks = total.div_ceil(SCOPES_PER_STATEMENT) as usize;
        let mut failed_chunks = 0usize;
        for chunk in 0..chunks {
            let scopes = code_scopes_sql(
                &self.code_checkpoint_table,
                &self.code_branch_table,
                after_block,
                (chunks > 1).then_some((chunks, chunk)),
            );
            let paths: Vec<String> = self
                .rows(self.graph.query(&format!(
                    "SELECT DISTINCT {PATH_COLUMN} FROM ({scopes}) ORDER BY {PATH_COLUMN}"
                )))
                .await?
                .into_iter()
                .map(|row| row[0].clone())
                .collect();
            if paths.is_empty() {
                continue;
            }
            let prune = path_prune_sql(&paths);
            for table in &self.tables {
                if unsafe_tables.contains(&table.name) {
                    self.metrics.record_requests_skipped(TASK_NAME, 1);
                    continue;
                }
                let statement = match table.code {
                    CodeRole::None => continue,
                    CodeRole::Project => code_snapshot_statement(
                        &table.name,
                        &scopes,
                        &prune,
                        self.config.statement_timeout_secs,
                    ),
                    CodeRole::SharedEdge => shared_edge_snapshot_statement(
                        &table.name,
                        &self.code_checkpoint_table,
                        &scopes,
                        &prune,
                        self.config.statement_timeout_secs,
                    ),
                };
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
                chunks, failed_chunks, history, "reclaimed superseded code snapshots"
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

    /// Patches materialize on their own only when the base part merges, which the largest parts
    /// never do, so pending patches are folded in by size or age.
    async fn apply_patches_if_due(&self) -> Result<(), TaskError> {
        let mut last = self.last_patch_apply.lock().await;
        let overdue = last.elapsed().as_secs() >= self.config.apply_patches_after_secs;
        let rows = self
            .rows(self.graph.query(&format!(
                "SELECT table, toString(sum(data_uncompressed_bytes)) \
                 FROM system.parts \
                 WHERE database = currentDatabase() AND active \
                   AND startsWith(name, '{PATCH_PART_PREFIX}') \
                   AND table IN ({}) \
                 GROUP BY table",
                self.tables
                    .iter()
                    .map(|table| format!("'{}'", table.name))
                    .collect::<Vec<_>>()
                    .join(", ")
            )))
            .await?;
        let mut applied = 0usize;
        for row in rows {
            let bytes: u64 = row[1].parse().unwrap_or(0);
            if overdue || bytes >= self.config.apply_patches_after_bytes {
                self.graph
                    .query(&apply_patches_statement(&row[0]))
                    .execute()
                    .await
                    .map_err(TaskError::new)?;
                applied += 1;
            }
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

/// Lightweight updates with subqueries silently did nothing before ClickHouse PR #87285, which
/// shipped in 25.10.1 and was backported to 25.7.8, 25.8.8 and 25.9.3.
fn supports_patch_deletes(version: &str) -> bool {
    let mut parts = version
        .split('.')
        .map(|part| part.parse::<u32>().unwrap_or(0));
    let (major, minor, patch) = (
        parts.next().unwrap_or(0),
        parts.next().unwrap_or(0),
        parts.next().unwrap_or(0),
    );
    match (major, minor) {
        (26.., _) => true,
        (25, 10..) => true,
        (25, 9) => patch >= 3,
        (25, 8) => patch >= 8,
        (25, 7) => patch >= 8,
        _ => false,
    }
}

fn escape(literal: &str) -> String {
    literal.replace('\\', "\\\\").replace('\'', "\\'")
}

fn block_settings_missing_sql(table: &str) -> String {
    format!(
        "SELECT 1 FROM system.tables WHERE database = currentDatabase() AND name = '{table}' \
         AND (engine_full NOT LIKE '%enable_block_number_column = 1%' \
              OR engine_full NOT LIKE '%enable_block_offset_column = 1%')"
    )
}

/// ClickHouse Cloud persists `_block_offset` on merged parts while `_block_number` stays virtual,
/// so the `(_block_number, _block_offset)` identity a patch joins on repeats inside one part.
fn offset_only_parts_sql(table: &str) -> String {
    format!(
        "SELECT 1 FROM system.parts_columns \
         WHERE database = currentDatabase() AND table = '{table}' AND active AND column = '_block_offset' \
           AND name NOT IN (SELECT name FROM system.parts_columns \
                            WHERE database = currentDatabase() AND table = '{table}' AND active AND column = '_block_number')"
    )
}

fn apply_patches_statement(table: &str) -> String {
    format!("ALTER TABLE {table} APPLY PATCHES SETTINGS mutations_sync = 0")
}

fn foreign_block_numbers_sql(table: &str) -> String {
    format!(
        "SELECT 1 FROM (SELECT _part, max(_block_number) AS persisted FROM {table} GROUP BY _part) AS rows_by_part \
         INNER JOIN (SELECT name, max_block_number FROM system.parts \
                     WHERE database = currentDatabase() AND table = '{table}' AND active) AS parts \
           ON parts.name = rows_by_part._part \
         WHERE rows_by_part.persisted > parts.max_block_number"
    )
}

/// Rows written since the cursor: only parts whose name ends above it are read at all.
fn new_rows_filter(after_block: u64) -> String {
    format!(" AND {PART_MAX_BLOCK} > {after_block} AND _block_number > {after_block}")
}

fn path_list_sql(paths: &[String]) -> String {
    paths
        .iter()
        .map(|path| format!("'{}'", escape(path)))
        .collect::<Vec<_>>()
        .join(", ")
}

fn path_prune_sql(paths: &[String]) -> String {
    format!("{PATH_COLUMN} IN ({})", path_list_sql(paths))
}

fn path_group_sql(table: &str, key: &str, paths: &[String], filter: &str) -> String {
    format!(
        "SELECT {key} FROM {table} WHERE {} AND _deleted{filter}",
        path_prune_sql(paths)
    )
}

fn hash_chunk_sql(table: &str, key: &str, filter: &str, chunks: usize, chunk: usize) -> String {
    let chunk = if chunks > 1 {
        format!(" AND cityHash64({key}) % {chunks} = {chunk}")
    } else {
        String::new()
    };
    format!("SELECT {key} FROM {table} WHERE {filter}{chunk}")
}

/// Deletes every row of the candidate keys except the newest one. `update_sequential_consistency`
/// is off: with it a lightweight update waits behind every pending replication-queue entry (300 s
/// timeouts on the bench after a restart), and the cursor overlap already covers parts a replica
/// fetches late. With `keep_newest_tombstone`
/// the newest row survives whatever its flag; without it a key whose newest row is a tombstone
/// loses all its rows. A live row tied with a tombstone at the same `_version` counts as live.
fn collapse_statement(
    table: &str,
    key: &str,
    candidates: &str,
    prune: Option<&str>,
    keep_newest_tombstone: bool,
    timeout_secs: u64,
) -> String {
    let having = if keep_newest_tombstone {
        ""
    } else {
        " HAVING maxIf(_version, NOT _deleted) = max(_version)"
    };
    let prune = prune
        .map(|prune| format!("{prune} AND "))
        .unwrap_or_default();
    format!(
        "DELETE FROM {table} WHERE {prune}({key}) IN ({candidates}) \
         AND ({key}, _version) NOT IN (\
           SELECT {key}, max(_version) FROM {table} WHERE {prune}({key}) IN ({candidates}) \
           GROUP BY {key}{having}) \
         SETTINGS lightweight_delete_mode = '{PATCH_DELETE_MODE}', update_sequential_consistency = 0, max_execution_time = {timeout_secs}"
    )
}

fn code_scopes_sql(
    checkpoint_table: &str,
    branch_table: &str,
    after_block: Option<u64>,
    chunk: Option<(usize, usize)>,
) -> String {
    let block = after_block
        .map(|block| format!("_block_number > {block}"))
        .unwrap_or_else(|| "1".to_string());
    let chunk = chunk
        .map(|(chunks, index)| format!(" AND cityHash64({CODE_SCOPE}) % {chunks} = {index}"))
        .unwrap_or_default();
    let changed = format!("SELECT {CODE_SCOPE} FROM {checkpoint_table} WHERE {block}{chunk}");
    format!(
        "SELECT s.traversal_path AS traversal_path, s.project_id AS project_id, s.branch AS branch, s.bound AS bound FROM (\
           SELECT {CODE_SCOPE}, max(indexed_at) AS bound FROM {checkpoint_table} \
           WHERE NOT _deleted AND ({CODE_SCOPE}) IN ({changed}) GROUP BY {CODE_SCOPE}) AS s \
         INNER JOIN (\
           SELECT traversal_path, project_id, name AS branch, max(_version) AS branch_version FROM {branch_table} \
           WHERE (traversal_path, project_id, name) IN ({changed}) AND NOT _deleted \
           GROUP BY traversal_path, project_id, name) AS b \
           ON s.traversal_path = b.traversal_path AND s.project_id = b.project_id AND s.branch = b.branch \
         WHERE b.branch_version >= s.bound"
    )
}

fn code_snapshot_statement(table: &str, scopes: &str, prune: &str, timeout_secs: u64) -> String {
    format!(
        "DELETE FROM {table} WHERE {prune} AND ({CODE_SCOPE}) IN (SELECT {CODE_SCOPE} FROM ({scopes})) \
         AND ({CODE_SCOPE}, _version) IN (\
           SELECT e.traversal_path, e.project_id, e.branch, e._version FROM {table} AS e \
           INNER JOIN ({scopes}) AS c \
             ON e.traversal_path = c.traversal_path AND e.project_id = c.project_id AND e.branch = c.branch \
           WHERE e.{prune} AND (e.traversal_path, e.project_id, e.branch) IN (SELECT {CODE_SCOPE} FROM ({scopes})) \
             AND e._version < c.bound) \
         SETTINGS lightweight_delete_mode = '{PATCH_DELETE_MODE}', update_sequential_consistency = 0, max_execution_time = {timeout_secs}"
    )
}

/// Shared edge tables carry no project or branch, so only paths with a single indexed branch are
/// reclaimed; a second branch may be mid-run without a checkpoint to bound it.
fn shared_edge_snapshot_statement(
    table: &str,
    checkpoint_table: &str,
    scopes: &str,
    prune: &str,
    timeout_secs: u64,
) -> String {
    let kinds = CodeTableNames::node_kinds_sql_list();
    let paths = format!(
        "SELECT traversal_path, min(scope_bound) AS bound FROM (\
           SELECT {CODE_SCOPE}, max(indexed_at) AS scope_bound FROM {checkpoint_table} \
           WHERE NOT _deleted GROUP BY {CODE_SCOPE}) \
         WHERE traversal_path IN (SELECT DISTINCT traversal_path FROM ({scopes})) \
         GROUP BY traversal_path HAVING count() = 1"
    );
    format!(
        "DELETE FROM {table} WHERE {prune} AND traversal_path IN (SELECT traversal_path FROM ({paths})) \
         AND source_kind IN ({kinds}) \
         AND (traversal_path, _version) IN (\
           SELECT e.traversal_path, e._version FROM {table} AS e \
           INNER JOIN ({paths}) AS c ON e.traversal_path = c.traversal_path \
           WHERE e.{prune} AND e.traversal_path IN (SELECT traversal_path FROM ({paths})) \
             AND e.source_kind IN ({kinds}) AND e._version < c.bound) \
         SETTINGS lightweight_delete_mode = '{PATCH_DELETE_MODE}', update_sequential_consistency = 0, max_execution_time = {timeout_secs}"
    )
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
        if !self.prepare().await? {
            self.metrics.record_requests_skipped(TASK_NAME, 1);
            return Ok(());
        }
        let pass_at = Utc::now();
        let mut failed = 0usize;
        let mut candidates = 0u64;
        match self.reclaim_code_snapshots(pass_at).await {
            Ok(scopes) => candidates += scopes,
            Err(error) => {
                failed += 1;
                self.metrics.record_error(TASK_NAME, "code_snapshots");
                warn!(%error, "code snapshot reclaim failed");
            }
        }
        let unsafe_tables = self.unsafe_tables.lock().await.clone();
        for table in &self.tables {
            if table.code == CodeRole::Project {
                continue;
            }
            if unsafe_tables.contains(&table.name) {
                self.metrics.record_requests_skipped(TASK_NAME, 1);
                continue;
            }
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
            return Err(TaskError::new(format!("{failed} reclaim steps failed")));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_gate_requires_the_subquery_fix() {
        for ok in [
            "26.4.1.2212",
            "25.10.1.3832",
            "25.9.3.48",
            "25.8.8.26",
            "25.7.8.71",
        ] {
            assert!(supports_patch_deletes(ok), "{ok}");
        }
        for bad in ["25.8.3.1", "25.7.1.1", "25.9.2.5", "24.12.1.1", "garbage"] {
            assert!(!supports_patch_deletes(bad), "{bad}");
        }
    }

    #[test]
    fn candidates_are_limited_to_parts_and_rows_above_the_cursor() {
        let sql = path_group_sql(
            "v1_gl_edge",
            "traversal_path, id",
            &["1/2/".to_string()],
            &new_rows_filter(42),
        );
        assert_eq!(
            sql,
            "SELECT traversal_path, id FROM v1_gl_edge WHERE traversal_path IN ('1/2/') AND _deleted AND toUInt64OrZero(splitByChar('_', _part)[3]) > 42 AND _block_number > 42"
        );
    }

    #[test]
    fn candidate_chunks_partition_by_key_hash() {
        let sql = hash_chunk_sql("t", "k", &format!("_deleted{}", new_rows_filter(0)), 4, 3);
        assert!(sql.ends_with("AND _block_number > 0 AND cityHash64(k) % 4 = 3"));
    }

    #[test]
    fn path_groups_are_pruned_by_traversal_path_and_filter() {
        let sql = path_group_sql(
            "t",
            "traversal_path, id",
            &["1/2/".to_string(), "1/3/4/".to_string()],
            " AND _version < x",
        );
        assert_eq!(
            sql,
            "SELECT traversal_path, id FROM t WHERE traversal_path IN ('1/2/', '1/3/4/') AND _deleted AND _version < x"
        );
    }

    #[test]
    fn incremental_collapse_keeps_the_newest_row_of_each_key() {
        let sql = collapse_statement(
            "t",
            "a, b",
            "SELECT a, b FROM t WHERE _deleted",
            Some("traversal_path IN ('1/2/')"),
            true,
            30,
        );
        assert!(sql.starts_with(
            "DELETE FROM t WHERE traversal_path IN ('1/2/') AND (a, b) IN (SELECT a, b FROM t WHERE _deleted) AND (a, b, _version) NOT IN (\
             SELECT a, b, max(_version) FROM t WHERE traversal_path IN ('1/2/') AND (a, b) IN ("
        ));
        assert!(sql.contains("GROUP BY a, b) SETTINGS"));
        assert!(sql.ends_with(
            "SETTINGS lightweight_delete_mode = 'lightweight_update_force', update_sequential_consistency = 0, max_execution_time = 30"
        ));
    }

    #[test]
    fn purge_collapse_drops_dead_keys_entirely_and_keeps_ties_alive() {
        let sql = collapse_statement(
            "t",
            "a, b",
            "SELECT a, b FROM t WHERE _deleted",
            None,
            false,
            30,
        );
        assert!(sql.starts_with("DELETE FROM t WHERE (a, b) IN ("));
        assert!(
            sql.contains("GROUP BY a, b HAVING maxIf(_version, NOT _deleted) = max(_version))")
        );
    }

    #[test]
    fn code_scopes_require_a_branch_row_at_or_after_the_checkpoint_bound() {
        let sql = code_scopes_sql("cp", "br", Some(17), None);
        assert_eq!(
            sql,
            "SELECT s.traversal_path AS traversal_path, s.project_id AS project_id, s.branch AS branch, s.bound AS bound FROM (\
               SELECT traversal_path, project_id, branch, max(indexed_at) AS bound FROM cp \
               WHERE NOT _deleted AND (traversal_path, project_id, branch) IN (\
                 SELECT traversal_path, project_id, branch FROM cp WHERE _block_number > 17) \
               GROUP BY traversal_path, project_id, branch) AS s \
             INNER JOIN (\
               SELECT traversal_path, project_id, name AS branch, max(_version) AS branch_version FROM br \
               WHERE (traversal_path, project_id, name) IN (\
                 SELECT traversal_path, project_id, branch FROM cp WHERE _block_number > 17) AND NOT _deleted \
               GROUP BY traversal_path, project_id, name) AS b \
               ON s.traversal_path = b.traversal_path AND s.project_id = b.project_id AND s.branch = b.branch \
             WHERE b.branch_version >= s.bound"
        );
    }

    #[test]
    fn missing_block_settings_are_detected_from_engine_full() {
        let sql = block_settings_missing_sql("v99_gl_edge");
        assert!(
            sql.contains("engine_full NOT LIKE '%enable_block_number_column = 1%'"),
            "{sql}"
        );
        assert!(
            sql.contains("engine_full NOT LIKE '%enable_block_offset_column = 1%'"),
            "{sql}"
        );
    }

    #[test]
    fn offset_only_parts_are_detected_from_parts_columns() {
        let sql = offset_only_parts_sql("v99_gl_edge");
        assert!(sql.contains("column = '_block_offset'"), "{sql}");
        assert!(
            sql.contains("NOT IN (SELECT name FROM system.parts_columns"),
            "{sql}"
        );
        assert!(sql.contains("column = '_block_number'"), "{sql}");
    }

    #[test]
    fn code_history_covers_every_checkpointed_scope() {
        let sql = code_scopes_sql("cp", "br", None, None);
        assert!(sql.contains("SELECT traversal_path, project_id, branch FROM cp WHERE 1)"));
    }

    #[test]
    fn code_scope_chunks_partition_by_scope_hash() {
        let sql = code_scopes_sql("cp", "br", Some(0), Some((3, 1)));
        assert!(sql.contains(
            "_block_number > 0 AND cityHash64(traversal_path, project_id, branch) % 3 = 1)"
        ));
    }

    #[test]
    fn code_snapshot_delete_is_bounded_by_each_scope_checkpoint() {
        let scopes = "SELECT traversal_path, project_id, branch, bound FROM cp";
        let sql = code_snapshot_statement(
            "v1_gl_definition",
            scopes,
            "traversal_path IN ('1/2/', '1/3/')",
            60,
        );
        assert!(sql.contains("AND e._version < c.bound"));
        assert!(sql.contains(
            "WHERE traversal_path IN ('1/2/', '1/3/') AND (traversal_path, project_id, branch) IN (SELECT traversal_path, project_id, branch FROM (SELECT traversal_path, project_id, branch, bound FROM cp))"
        ));
        assert!(sql.contains("WHERE e.traversal_path IN ('1/2/', '1/3/') AND (e.traversal_path"));
    }

    #[test]
    fn shared_edge_delete_only_covers_single_branch_paths() {
        let sql = shared_edge_snapshot_statement(
            "v1_gl_edge",
            "v1_code_indexing_checkpoint",
            "SELECT 1",
            "traversal_path IN ('1/2/')",
            60,
        );
        assert!(sql.starts_with(
            "DELETE FROM v1_gl_edge WHERE traversal_path IN ('1/2/') AND traversal_path IN (SELECT"
        ));
        assert!(sql.contains("WHERE e.traversal_path IN ('1/2/') AND e.traversal_path IN (SELECT"));
        assert!(sql.contains("GROUP BY traversal_path HAVING count() = 1"));
        assert!(
            sql.contains("source_kind IN ('Directory', 'File', 'Definition', 'ImportedSymbol')")
        );
        assert!(sql.contains("AND e._version < c.bound"));
    }

    #[test]
    fn foreign_block_numbers_compare_persisted_values_with_part_names() {
        let sql = foreign_block_numbers_sql("v1_gl_edge");
        assert!(sql.contains("max(_block_number) AS persisted FROM v1_gl_edge GROUP BY _part"));
        assert!(sql.ends_with("WHERE rows_by_part.persisted > parts.max_block_number"));
    }
}
