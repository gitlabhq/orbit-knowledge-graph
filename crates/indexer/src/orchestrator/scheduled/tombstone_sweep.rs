//! Physically reclaims ReplacingMergeTree tombstones (`_deleted = true`).
//!
//! Reads already hide a tombstone the instant it lands (FINAL, `argMax`,
//! hydration's `_version` dedup). Variable-depth edge traversal is the one
//! exception: `build_depth_arm` scans edge tables with no FINAL and no per-key
//! `_version` dedup, so a retired edge keeps surfacing in k-hop paths until its
//! row is physically gone. This sweep bounds that window to one sweep interval.
//!
//! Every delete predicate is a flat literal tuple list (`(keys) IN ((…),(…))`).
//! ClickHouse stores a delete as a mutation command and re-parses it on replay;
//! a subquery, a `UNION`, or a reference to a scratch table that is later dropped
//! all turn into a permanently-failing, queue-wedging mutation (#1221). Literals
//! carry no such dependency.

use std::sync::Arc;
use std::time::Instant;

use arrow::array::ArrayRef;
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use tracing::{info, warn};

use crate::checkpoint::{Checkpoint, CheckpointStore};
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::durability::WriteDurability;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use orbit_server_config::{ScheduleConfiguration, TombstoneSweepConfig};

const CONCURRENT_TABLE_SWEEPS: usize = 4;
const STATEMENT_TIMEOUT_SECS: u64 = 7200;

/// Fraction of `max_query_size_bytes` a rendered statement may fill before it is
/// flushed, leaving headroom for the literal timestamp and the settings clause.
const QUERY_SIZE_SAFETY_NUMERATOR: usize = 9;
const QUERY_SIZE_SAFETY_DENOMINATOR: usize = 10;

#[derive(Clone)]
struct SweptTable {
    name: String,
    sort_key: Vec<String>,
}

pub struct TombstoneSweep {
    task_name: &'static str,
    graph: ArrowClickHouseClient,
    checkpoint_store: Arc<dyn CheckpointStore>,
    tables: Vec<SweptTable>,
    metrics: ScheduledTaskMetrics,
    config: TombstoneSweepConfig,
}

impl TombstoneSweep {
    /// Weekly backstop over every node and edge table. Its lookback is so large
    /// the first pass is effectively unbounded, reclaiming tombstones that predate
    /// this task and any the edge instance missed during a gap wider than its
    /// lookback; then the checkpoint carries it forward at the weekly cadence.
    pub fn for_all_tables(
        graph: ArrowClickHouseClient,
        ontology: &ontology::Ontology,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: ScheduledTaskMetrics,
        config: TombstoneSweepConfig,
    ) -> Self {
        let tables = swept_tables(
            ontology,
            ontology
                .nodes()
                .map(|n| n.destination_table.as_str())
                .chain(ontology.edge_tables()),
        );
        Self {
            task_name: "maintenance.table_cleanup",
            graph,
            checkpoint_store,
            tables,
            metrics,
            config,
        }
    }

    /// Tight-cadence sweep over edge tables: the only tombstones a read path
    /// (variable-depth traversal) can still see until they are physically gone.
    /// A small lookback keeps every run's scan bounded (see [`Self::window_start`]).
    pub fn for_edge_tables(
        graph: ArrowClickHouseClient,
        ontology: &ontology::Ontology,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: ScheduledTaskMetrics,
        config: TombstoneSweepConfig,
    ) -> Self {
        let tables = swept_tables(ontology, ontology.edge_tables().into_iter());
        Self {
            task_name: "maintenance.edge_tombstone_collapse",
            graph,
            checkpoint_store,
            tables,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl ScheduledTask for TombstoneSweep {
    fn name(&self) -> &str {
        self.task_name
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let started = Instant::now();
        let result = self.sweep_all_tables().await;
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics
            .record_run(self.name(), outcome, started.elapsed().as_secs_f64());
        result
    }
}

impl TombstoneSweep {
    async fn sweep_all_tables(&self) -> Result<(), TaskError> {
        let sweeps = self
            .tables
            .iter()
            .cloned()
            .map(|table| self.sweep_table(table));
        let failed = futures::stream::iter(sweeps)
            .buffer_unordered(CONCURRENT_TABLE_SWEEPS)
            .filter(|succeeded| futures::future::ready(!succeeded))
            .count()
            .await;

        let tables = self.tables.len();
        info!(
            task = self.task_name,
            tables, failed, "tombstone sweep complete"
        );
        if failed > 0 {
            return Err(TaskError::new(format!(
                "{failed}/{tables} tables failed to sweep"
            )));
        }
        Ok(())
    }

    async fn sweep_table(&self, table: SweptTable) -> bool {
        let started = Instant::now();
        match self.sweep_table_inner(&table).await {
            Ok(()) => {
                self.metrics
                    .record_query_duration(&table.name, started.elapsed().as_secs_f64());
                true
            }
            Err(error) => {
                self.metrics.record_error(self.task_name, "sweep_table");
                warn!(task = self.task_name, table = table.name, %error, "tombstone sweep failed");
                false
            }
        }
    }

    async fn sweep_table_inner(&self, table: &SweptTable) -> Result<(), TaskError> {
        let key = format!("{}.{}", self.task_name, table.name);
        let window_end = Utc::now();
        let window_start = self.window_start(&key, window_end).await?;

        // One extra key marks a run that could not drain the whole window; it stops
        // short and leaves the checkpoint so the next run picks up where it left off.
        let budget = self.config.max_keys_per_run;
        let batches = self
            .select_tombstoned_keys(table, window_start, window_end, budget + 1)
            .await?;

        let tuples = render_key_tuples(&batches, &table.sort_key)?;
        let fully_drained = tuples.len() <= budget;
        let to_delete = &tuples[..tuples.len().min(budget)];

        if !to_delete.is_empty() {
            self.delete_keys(table, to_delete, window_end).await?;
        }

        if fully_drained {
            self.checkpoint_store
                .save_completed(&key, &window_end, WriteDurability::Durable)
                .await
                .map_err(TaskError::new)?;
        } else {
            // Not advancing the checkpoint re-scans this window next run; already
            // collapsed keys no longer match, so the re-scan is idempotent.
            warn!(
                task = self.task_name,
                table = table.name,
                budget,
                "tombstone sweep hit its per-run key budget; draining the remainder on later runs"
            );
        }
        Ok(())
    }

    /// Window floor: the checkpoint, clamped to be never *older* than
    /// `now - lookback`. This caps every run's scan at `lookback + cadence` no
    /// matter how stale (or absent) the checkpoint is, so the 15-minute edge
    /// instance can never fall back to a multi-billion-row full scan. With no
    /// checkpoint the floor seeds at `now - lookback`; the weekly backstop uses a
    /// lookback so large that this seed is effectively the epoch (an unbounded
    /// first pass), after which the checkpoint carries it forward.
    ///
    /// A tombstone whose `_version` predates the floor is left for the weekly
    /// backstop rather than widening the tight instance's scan.
    async fn window_start(
        &self,
        key: &str,
        window_end: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, TaskError> {
        let checkpoint = self
            .checkpoint_store
            .load(key)
            .await
            .map_err(TaskError::new)?;
        Ok(bounded_window_start(
            checkpoint.map(|c: Checkpoint| c.watermark),
            window_end,
            self.config.lookback(),
        ))
    }

    async fn select_tombstoned_keys(
        &self,
        table: &SweptTable,
        window_start: DateTime<Utc>,
        window_end: DateTime<Utc>,
        limit: usize,
    ) -> Result<Vec<RecordBatch>, TaskError> {
        self.graph
            .query(&select_tombstoned_keys_sql(table, limit))
            .param(
                "window_start",
                window_start.format(TIMESTAMP_FORMAT).to_string(),
            )
            .param(
                "window_end",
                window_end.format(TIMESTAMP_FORMAT).to_string(),
            )
            .with_setting("max_execution_time", STATEMENT_TIMEOUT_SECS.to_string())
            .fetch_arrow()
            .await
            .map_err(TaskError::new)
    }

    async fn delete_keys(
        &self,
        table: &SweptTable,
        tuples: &[String],
        window_end: DateTime<Utc>,
    ) -> Result<(), TaskError> {
        let budget = self.config.max_query_size_bytes * QUERY_SIZE_SAFETY_NUMERATOR
            / QUERY_SIZE_SAFETY_DENOMINATOR;
        let window_end = window_end.format(TIMESTAMP_FORMAT).to_string();
        for statement in build_delete_statements(table, tuples, &window_end, budget) {
            ensure_flat_delete_predicate(&statement).map_err(TaskError::new)?;
            self.graph
                .query(&statement)
                .with_setting(
                    "max_query_size",
                    self.config.max_query_size_bytes.to_string(),
                )
                .execute()
                .await
                .map_err(TaskError::new)?;
        }
        Ok(())
    }
}

/// A delete predicate ClickHouse can safely replay. It stores a delete as a
/// mutation command and re-parses that text on replay; a subquery or `UNION`
/// fails there and wedges the table's mutation queue forever (#1221). The sweep
/// only ever builds flat literals, so this is a belt-and-braces guard that, unlike
/// a `debug_assert`, also fires in release: on a violation the caller fails the
/// table rather than issuing an unreplayable mutation.
fn ensure_flat_delete_predicate(sql: &str) -> Result<(), String> {
    let upper = sql.to_ascii_uppercase();
    if upper.contains("SELECT") {
        return Err(format!("delete predicate is not a flat literal: {sql}"));
    }
    if upper.contains("UNION") {
        return Err(format!("delete predicate contains UNION: {sql}"));
    }
    Ok(())
}

/// The checkpoint, clamped never older than `window_end - lookback`. Bounds a
/// run's scan regardless of how stale or absent the checkpoint is.
fn bounded_window_start(
    checkpoint: Option<DateTime<Utc>>,
    window_end: DateTime<Utc>,
    lookback: chrono::TimeDelta,
) -> DateTime<Utc> {
    let floor = window_end - lookback;
    checkpoint.map(|c| c.max(floor)).unwrap_or(floor)
}

fn swept_tables<'a>(
    ontology: &ontology::Ontology,
    tables: impl Iterator<Item = &'a str>,
) -> Vec<SweptTable> {
    tables
        .filter_map(|table| {
            Some(SweptTable {
                name: prefixed_table_name(table, *SCHEMA_VERSION),
                sort_key: ontology.sort_key_for_table(table)?.to_vec(),
            })
        })
        .collect()
}

/// Keys whose newest in-window version is a tombstone. `LIMIT 1 BY` keeps one
/// row per key so a superseded live row above the tombstone still wins and drops
/// the key from the result.
fn select_tombstoned_keys_sql(table: &SweptTable, limit: usize) -> String {
    let keys = table.sort_key.join(", ");
    format!(
        "SELECT {keys} FROM ( \
           SELECT {keys}, _version, _deleted FROM {source} \
           WHERE _version > {{window_start:String}} AND _version <= {{window_end:String}} \
           ORDER BY {keys}, _version DESC \
           LIMIT 1 BY {keys} \
         ) WHERE _deleted ORDER BY {keys} LIMIT {limit}",
        source = table.name,
    )
}

/// Packs rendered key tuples into flat-literal `DELETE`s that each stay under
/// `budget` bytes. `_version <= window_end` protects a row re-created after the
/// key snapshot from being removed.
fn build_delete_statements(
    table: &SweptTable,
    tuples: &[String],
    window_end: &str,
    budget: usize,
) -> Vec<String> {
    let keys = table.sort_key.join(", ");
    let mut statements = Vec::new();
    let mut chunk: Vec<&str> = Vec::new();
    let mut chunk_bytes = 0usize;
    let fixed = delete_statement(&table.name, &keys, "", window_end).len();

    for tuple in tuples {
        let added = tuple.len() + ", ".len();
        if !chunk.is_empty() && fixed + chunk_bytes + added > budget {
            statements.push(delete_statement(
                &table.name,
                &keys,
                &chunk.join(", "),
                window_end,
            ));
            chunk.clear();
            chunk_bytes = 0;
        }
        chunk_bytes += added;
        chunk.push(tuple);
    }
    if !chunk.is_empty() {
        statements.push(delete_statement(
            &table.name,
            &keys,
            &chunk.join(", "),
            window_end,
        ));
    }
    statements
}

fn delete_statement(table: &str, keys: &str, tuple_list: &str, window_end: &str) -> String {
    format!(
        "DELETE FROM {table} WHERE ({keys}) IN ({tuple_list}) AND _version <= '{window_end}' \
         SETTINGS lightweight_deletes_sync = 0, max_execution_time = {STATEMENT_TIMEOUT_SECS}"
    )
}

fn render_key_tuples(
    batches: &[RecordBatch],
    sort_key: &[String],
) -> Result<Vec<String>, TaskError> {
    let mut tuples = Vec::new();
    for batch in batches {
        let columns = sort_key_columns(batch, sort_key)?;
        for row in 0..batch.num_rows() {
            tuples.push(key_tuple_literal(&columns, row)?);
        }
    }
    Ok(tuples)
}

/// Resolves each sort key column to a plain array, casting dictionary-encoded
/// columns (how ClickHouse returns `LowCardinality` kinds) to their value type.
fn sort_key_columns(batch: &RecordBatch, sort_key: &[String]) -> Result<Vec<ArrayRef>, TaskError> {
    sort_key
        .iter()
        .map(|column| {
            let array = batch
                .column_by_name(column)
                .ok_or_else(|| TaskError::new(format!("sort key column '{column}' missing")))?;
            let plain = match array.data_type() {
                DataType::Dictionary(_, value_type) => compute::cast(array, value_type)
                    .map_err(|e| TaskError::new(format!("cast '{column}': {e}")))?,
                _ => Arc::clone(array),
            };
            Ok(plain)
        })
        .collect()
}

fn key_tuple_literal(columns: &[ArrayRef], row: usize) -> Result<String, TaskError> {
    let values = columns
        .iter()
        .map(|column| {
            orbit_utils::clickhouse::render_arrow_sql_literal(column, row).map_err(TaskError::new)
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(format!("({})", values.join(", ")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, BooleanArray, DictionaryArray, Int64Array};
    use arrow::datatypes::{Field, Int32Type, Schema};

    fn edge_table() -> SweptTable {
        SweptTable {
            name: "v99_gl_edge".to_string(),
            sort_key: vec![
                "relationship_kind".to_string(),
                "source_id".to_string(),
                "target_id".to_string(),
            ],
        }
    }

    fn all_tables(ontology: &ontology::Ontology) -> Vec<SweptTable> {
        swept_tables(
            ontology,
            ontology
                .nodes()
                .map(|n| n.destination_table.as_str())
                .chain(ontology.edge_tables()),
        )
    }

    #[test]
    fn edge_sweep_is_edges_only_and_the_backstop_covers_everything() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let all = all_tables(&ontology);
        let edges = swept_tables(&ontology, ontology.edge_tables().into_iter());
        let all_names: Vec<&str> = all.iter().map(|t| t.name.as_str()).collect();

        for edge_table in ontology.edge_tables() {
            let prefixed = prefixed_table_name(edge_table, *SCHEMA_VERSION);
            assert!(
                edges.iter().any(|t| t.name == prefixed),
                "edge sweep missing {prefixed}"
            );
            assert!(
                all_names.contains(&prefixed.as_str()),
                "backstop missing edge {prefixed}"
            );
        }
        for node in ontology.nodes() {
            let prefixed = prefixed_table_name(&node.destination_table, *SCHEMA_VERSION);
            assert!(
                all_names.contains(&prefixed.as_str()),
                "backstop missing node {prefixed}"
            );
            assert!(
                !edges.iter().any(|t| t.name == prefixed),
                "edge sweep must not touch node table {prefixed}"
            );
        }
        assert_eq!(
            all.len(),
            ontology.nodes().count() + ontology.edge_tables().len()
        );
        assert!(!edges.is_empty());
    }

    #[test]
    fn window_start_is_bounded_by_lookback_regardless_of_checkpoint() {
        let now = DateTime::parse_from_rfc3339("2026-08-29T12:00:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let lookback = chrono::TimeDelta::hours(1);
        let floor = now - lookback;

        assert_eq!(bounded_window_start(None, now, lookback), floor);

        let recent = now - chrono::TimeDelta::minutes(15);
        assert_eq!(
            bounded_window_start(Some(recent), now, lookback),
            recent,
            "a fresh checkpoint carries the window forward"
        );

        let ancient = now - chrono::TimeDelta::days(30);
        assert_eq!(
            bounded_window_start(Some(ancient), now, lookback),
            floor,
            "a stale checkpoint must never widen the scan past the lookback floor"
        );
    }

    #[test]
    fn every_swept_table_has_a_sort_key() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        for table in all_tables(&ontology).iter() {
            assert!(!table.sort_key.is_empty(), "table '{}'", table.name);
        }
    }

    #[test]
    fn select_scopes_to_the_version_window_and_newest_tombstone() {
        let sql = select_tombstoned_keys_sql(&edge_table(), 500);
        assert!(sql.contains("_version > {window_start:String}"), "{sql}");
        assert!(sql.contains("_version <= {window_end:String}"), "{sql}");
        assert!(
            sql.contains("LIMIT 1 BY relationship_kind, source_id, target_id"),
            "{sql}"
        );
        assert!(sql.contains("_version DESC"), "{sql}");
        assert!(sql.contains(") WHERE _deleted ORDER BY"), "{sql}");
        assert!(sql.trim_end().ends_with("LIMIT 500"), "{sql}");
    }

    #[test]
    fn delete_is_a_flat_literal_bounded_to_the_snapshot() {
        let stmts = build_delete_statements(
            &edge_table(),
            &[
                "('MENTIONS', 1, 2)".to_string(),
                "('CONTAINS', 3, 4)".to_string(),
            ],
            "2020-01-01 00:00:00.000000",
            1_000_000,
        );
        assert_eq!(stmts.len(), 1);
        let sql = &stmts[0];
        assert!(
            sql.starts_with(
                "DELETE FROM v99_gl_edge WHERE (relationship_kind, source_id, target_id) IN \
             (('MENTIONS', 1, 2), ('CONTAINS', 3, 4)) AND _version <= '2020-01-01 00:00:00.000000'"
            ),
            "{sql}"
        );
        assert!(sql.contains("lightweight_deletes_sync = 0"), "{sql}");
        assert!(sql.contains("max_execution_time = 7200"), "{sql}");
        assert!(!sql.contains("SELECT"), "{sql}");
        assert!(!sql.contains("UNION"), "{sql}");
        assert!(!sql.contains("allow_nondeterministic_mutations"), "{sql}");
        assert!(ensure_flat_delete_predicate(sql).is_ok(), "{sql}");
    }

    #[test]
    fn flat_predicate_guard_rejects_subqueries_and_unions() {
        assert!(ensure_flat_delete_predicate("DELETE FROM t WHERE id IN (1, 2)").is_ok());
        assert!(
            ensure_flat_delete_predicate("DELETE FROM t WHERE id IN (SELECT id FROM s)").is_err()
        );
        assert!(
            ensure_flat_delete_predicate("DELETE FROM t WHERE id IN (1) UNION ALL SELECT 2")
                .is_err()
        );
    }

    #[test]
    fn key_lists_pack_into_multiple_statements_under_the_byte_budget() {
        let tuples: Vec<String> = (0..100)
            .map(|i| format!("('MENTIONS', {i}, {i})"))
            .collect();
        let budget = 400;
        let stmts =
            build_delete_statements(&edge_table(), &tuples, "2020-01-01 00:00:00.000000", budget);
        assert!(
            stmts.len() > 1,
            "a small budget must split the key list: {}",
            stmts.len()
        );
        for sql in &stmts {
            assert!(
                sql.len() <= budget + longest_tuple_overshoot(),
                "{}: {sql}",
                sql.len()
            );
        }
        let total: usize = stmts.iter().map(|s| s.matches("('MENTIONS'").count()).sum();
        assert_eq!(total, 100);
    }

    /// One tuple can push a statement past `budget` because a chunk never flushes
    /// empty; the overshoot is bounded by the longest single tuple.
    fn longest_tuple_overshoot() -> usize {
        "('MENTIONS', 99, 99), ".len()
    }

    #[test]
    fn dictionary_and_int_sort_keys_render_as_literals() {
        let kinds: DictionaryArray<Int32Type> = vec!["MENTIONS", "CONTAINS"].into_iter().collect();
        let schema = Schema::new(vec![
            Field::new("relationship_kind", kinds.data_type().clone(), false),
            Field::new("source_id", DataType::Int64, false),
            Field::new("target_id", DataType::Int64, false),
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(kinds),
                Arc::new(Int64Array::from(vec![1_i64, 3])),
                Arc::new(Int64Array::from(vec![2_i64, 4])),
            ],
        )
        .unwrap();

        let tuples = render_key_tuples(
            &[batch],
            &[
                "relationship_kind".to_string(),
                "source_id".to_string(),
                "target_id".to_string(),
            ],
        )
        .unwrap();
        assert_eq!(tuples, vec!["('MENTIONS', 1, 2)", "('CONTAINS', 3, 4)"]);
    }

    #[test]
    fn missing_sort_key_column_is_an_error() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1_i64]))],
        )
        .unwrap();
        let err = render_key_tuples(&[batch], &["not_a_column".to_string()]).unwrap_err();
        assert!(err.to_string().contains("not_a_column"));
    }

    #[test]
    fn unsupported_sort_key_type_is_an_error() {
        let schema = Schema::new(vec![Field::new("flag", DataType::Boolean, false)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(BooleanArray::from(vec![true]))],
        )
        .unwrap();
        let err = render_key_tuples(&[batch], &["flag".to_string()]).unwrap_err();
        assert!(err.to_string().contains("unsupported sort key type"));
    }
}
