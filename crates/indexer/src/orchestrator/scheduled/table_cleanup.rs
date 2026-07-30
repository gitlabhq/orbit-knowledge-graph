use std::time::{Duration, Instant};

use arrow::datatypes::UInt64Type;
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use tracing::{info, warn};

use gkg_utils::arrow::ArrowUtils;

use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use gkg_server_config::{ScheduleConfiguration, TableCleanupConfig};

const TASK_NAME: &str = "maintenance.table_cleanup";
const TOMBSTONED_KEYS_TABLE_PREFIX: &str = "tombstone_sweep_keys";

// ── Pacing ───────────────────────────────────────────────────────────
const CONCURRENT_TABLE_SWEEPS: usize = 4;
const REMAINING_ROWS_POLL_INTERVAL: Duration = Duration::from_secs(5);

// ── ClickHouse statement settings ────────────────────────────────────
/// Bounds a wedged sweep, not a normal one: prod's largest tables took 2-27min.
const STATEMENT_TIMEOUT_SECS: u64 = 5400;
/// Headroom for the external sort, against a measured sub-1-GiB peak.
const MAX_STATEMENT_MEMORY_BYTES: u64 = 8_000_000_000;
/// Spilling has to start before the memory limit kills the sort.
const SPILL_SORT_TO_DISK_ABOVE_BYTES: u64 = 2_000_000_000;
const MAX_KEYS_FOR_INDEX_ANALYSIS: u64 = 1_000_000;

#[derive(Clone)]
struct ReplacingMergeTreeTable {
    name: String,
    sort_key: Vec<String>,
}

pub struct TableCleanup {
    graph: ArrowClickHouseClient,
    tables: Vec<ReplacingMergeTreeTable>,
    metrics: ScheduledTaskMetrics,
    config: TableCleanupConfig,
}

impl TableCleanup {
    pub fn new(
        graph: ArrowClickHouseClient,
        ontology: &ontology::Ontology,
        metrics: ScheduledTaskMetrics,
        config: TableCleanupConfig,
    ) -> Self {
        Self {
            graph,
            tables: list_replacing_merge_tree_tables(ontology),
            metrics,
            config,
        }
    }
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
        let result = self.sweep_tombstones_from_all_tables().await;
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics
            .record_run(self.name(), outcome, started.elapsed().as_secs_f64());
        result
    }
}

impl TableCleanup {
    /// Delete cost tracks rows scanned, not tombstones removed, so concurrency
    /// is the only lever on how long a pass takes.
    async fn sweep_tombstones_from_all_tables(&self) -> Result<(), TaskError> {
        let sweeps = self
            .tables
            .iter()
            .cloned()
            .map(|table| self.sweep_table_and_drop_its_keys_table(table));
        let failed = futures::stream::iter(sweeps)
            .buffer_unordered(CONCURRENT_TABLE_SWEEPS)
            .filter(|succeeded| futures::future::ready(!succeeded))
            .count()
            .await;

        let tables = self.tables.len();
        info!(tables, failed, "tombstone sweep complete");

        if failed > 0 {
            return Err(TaskError::new(format!(
                "{failed}/{tables} tables failed to sweep"
            )));
        }
        Ok(())
    }

    async fn sweep_table_and_drop_its_keys_table(&self, table: ReplacingMergeTreeTable) -> bool {
        let table = &table;
        let started = Instant::now();
        let result = self.sweep_tombstones_from_table(table).await;
        if let Err(error) = self.drop_tombstoned_keys_table(table).await {
            warn!(table = table.name, %error, "failed to drop the tombstoned keys table");
        }
        match result {
            Ok(()) => {
                let elapsed = started.elapsed().as_secs_f64();
                self.metrics.record_query_duration(&table.name, elapsed);
                info!(
                    table = table.name,
                    duration_ms = (elapsed * 1000.0) as u64,
                    "swept tombstoned keys"
                );
                true
            }
            Err(error) => {
                self.metrics.record_error(TASK_NAME, "sweep_tombstones");
                warn!(table = table.name, %error, "failed to sweep tombstoned keys");
                false
            }
        }
    }

    async fn sweep_tombstones_from_table(
        &self,
        table: &ReplacingMergeTreeTable,
    ) -> Result<(), TaskError> {
        let window_end = Utc::now();
        let window_start = window_end - self.config.lookback();

        self.drop_tombstoned_keys_table(table).await?;
        self.graph
            .query(&build_tombstoned_keys_table_sql(table))
            .param(
                "window_start",
                window_start.format(TIMESTAMP_FORMAT).to_string(),
            )
            .param(
                "window_end",
                window_end.format(TIMESTAMP_FORMAT).to_string(),
            )
            .execute()
            .await
            .map_err(TaskError::new)?;
        self.graph
            .query(&build_delete_tombstoned_keys_sql(table))
            .execute()
            .await
            .map_err(TaskError::new)?;
        self.wait_until_tombstoned_keys_are_gone(table).await
    }

    /// `system.mutations` is not readable by the graph user, so the data itself
    /// is the only completion signal.
    async fn wait_until_tombstoned_keys_are_gone(
        &self,
        table: &ReplacingMergeTreeTable,
    ) -> Result<(), TaskError> {
        let deadline = Instant::now() + Duration::from_secs(STATEMENT_TIMEOUT_SECS);
        loop {
            if self.count_remaining_tombstoned_rows(table).await? == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(TaskError::new(format!(
                    "delete on {} left rows behind after {STATEMENT_TIMEOUT_SECS}s",
                    table.name
                )));
            }
            tokio::time::sleep(REMAINING_ROWS_POLL_INTERVAL).await;
        }
    }

    async fn count_remaining_tombstoned_rows(
        &self,
        table: &ReplacingMergeTreeTable,
    ) -> Result<u64, TaskError> {
        let batches = self
            .graph
            .query(&build_count_remaining_tombstoned_rows_sql(table))
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;
        Ok(batches
            .first()
            .and_then(|batch| ArrowUtils::get_column::<UInt64Type>(batch, "remaining", 0))
            .unwrap_or(0))
    }

    async fn drop_tombstoned_keys_table(
        &self,
        table: &ReplacingMergeTreeTable,
    ) -> Result<(), TaskError> {
        self.graph
            .query(&format!(
                "DROP TABLE IF EXISTS {}",
                tombstoned_keys_table_name(table)
            ))
            .execute()
            .await
            .map_err(TaskError::new)
    }
}

fn list_replacing_merge_tree_tables(ontology: &ontology::Ontology) -> Vec<ReplacingMergeTreeTable> {
    ontology
        .nodes()
        .map(|node| node.destination_table.as_str())
        .chain(ontology.edge_tables())
        .filter_map(|table| {
            Some(ReplacingMergeTreeTable {
                name: prefixed_table_name(table, *SCHEMA_VERSION),
                sort_key: ontology.sort_key_for_table(table)?.to_vec(),
            })
        })
        .collect()
}

fn tombstoned_keys_table_name(table: &ReplacingMergeTreeTable) -> String {
    format!("{TOMBSTONED_KEYS_TABLE_PREFIX}_{}", table.name)
}

fn build_tombstoned_keys_table_sql(table: &ReplacingMergeTreeTable) -> String {
    let keys = table.sort_key.join(", ");
    let keys_table = tombstoned_keys_table_name(table);
    let source = &table.name;
    format!(
        "CREATE TABLE {keys_table} ENGINE = MergeTree ORDER BY ({keys}) \
         AS SELECT {keys} FROM ( \
           SELECT {keys}, _deleted FROM {source} \
           WHERE _version > {{window_start:String}} AND _version <= {{window_end:String}} \
           ORDER BY {keys}, _version DESC \
           LIMIT 1 BY {keys} \
         ) WHERE _deleted \
         SETTINGS max_memory_usage = {MAX_STATEMENT_MEMORY_BYTES}, \
                  max_bytes_before_external_sort = {SPILL_SORT_TO_DISK_ABOVE_BYTES}, \
                  optimize_read_in_order = 1, max_execution_time = {STATEMENT_TIMEOUT_SECS}"
    )
}

fn build_delete_tombstoned_keys_sql(table: &ReplacingMergeTreeTable) -> String {
    let keys = table.sort_key.join(", ");
    format!(
        "DELETE FROM {} WHERE ({keys}) IN (SELECT {keys} FROM {}) \
         SETTINGS lightweight_deletes_sync = 0, \
                  max_execution_time = {STATEMENT_TIMEOUT_SECS}",
        table.name,
        tombstoned_keys_table_name(table),
    )
}

fn build_count_remaining_tombstoned_rows_sql(table: &ReplacingMergeTreeTable) -> String {
    let keys = table.sort_key.join(", ");
    format!(
        "SELECT count() AS remaining FROM {} WHERE ({keys}) IN (SELECT {keys} FROM {}) \
         SETTINGS use_index_for_in_with_subqueries_max_values = {MAX_KEYS_FOR_INDEX_ANALYSIS}, \
                  max_execution_time = {STATEMENT_TIMEOUT_SECS}",
        table.name,
        tombstoned_keys_table_name(table),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeDelta;

    fn all_swept_tables() -> Vec<ReplacingMergeTreeTable> {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        list_replacing_merge_tree_tables(&ontology)
    }

    fn find_table_ending_in<'a>(
        tables: &'a [ReplacingMergeTreeTable],
        suffix: &str,
    ) -> &'a ReplacingMergeTreeTable {
        tables
            .iter()
            .find(|table| table.name.ends_with(suffix))
            .unwrap_or_else(|| panic!("expected a table ending in {suffix}"))
    }

    #[test]
    fn every_table_has_a_sort_key() {
        for table in &all_swept_tables() {
            assert!(!table.sort_key.is_empty(), "table '{}'", table.name);
        }
    }

    #[test]
    fn auxiliary_tables_are_not_swept() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        for aux in ontology.auxiliary_tables() {
            assert!(
                !all_swept_tables()
                    .iter()
                    .any(|table| table.name.ends_with(&aux.name)),
                "auxiliary table '{}' must not be swept",
                aux.name
            );
        }
    }

    #[test]
    fn tombstoned_keys_table_names_avoid_the_schema_version_prefix() {
        for table in &all_swept_tables() {
            assert!(
                !tombstoned_keys_table_name(table).starts_with(&format!("v{}_", *SCHEMA_VERSION)),
                "schema garbage collection globs v<version>_* and would drop '{}'",
                tombstoned_keys_table_name(table)
            );
        }
    }

    #[test]
    fn each_table_gets_its_own_keys_table() {
        let tables = all_swept_tables();
        let mut names: Vec<String> = tables.iter().map(tombstoned_keys_table_name).collect();
        names.sort();
        names.dedup();
        assert_eq!(
            names.len(),
            tables.len(),
            "concurrent sweeps must not share a keys table"
        );
    }

    #[test]
    fn the_lookback_reaches_back_past_the_sweep_cadence() {
        let config = TableCleanupConfig::default();
        let cadence =
            TimeDelta::from_std(config.schedule.interval_hint()).expect("cadence fits a TimeDelta");

        assert!(
            config.lookback() > cadence,
            "the sweep keeps no cursor, so windows that do not overlap leave tombstones \
             nothing will ever pick up"
        );
    }

    #[test]
    fn keys_are_bounded_to_one_version_window() {
        let tables = all_swept_tables();
        let sql = build_tombstoned_keys_table_sql(find_table_ending_in(&tables, "gl_edge"));

        assert!(
            sql.contains("_version > {window_start:String}"),
            "sql: {sql}"
        );
        assert!(
            sql.contains("_version <= {window_end:String}"),
            "sql: {sql}"
        );
    }

    #[test]
    fn keys_are_the_newest_version_of_each_sort_key() {
        let tables = all_swept_tables();
        let sql = build_tombstoned_keys_table_sql(find_table_ending_in(&tables, "gl_edge"));

        assert!(sql.contains("LIMIT 1 BY"), "sql: {sql}");
        assert!(sql.contains("_version DESC"), "sql: {sql}");
        assert!(sql.contains("WHERE _deleted"), "sql: {sql}");
    }

    #[test]
    fn delete_reads_keys_from_the_materialised_table_not_the_source() {
        let tables = all_swept_tables();
        let table = find_table_ending_in(&tables, "gl_edge");
        let sql = build_delete_tombstoned_keys_sql(table);

        assert!(
            sql.contains(&format!(
                "IN (SELECT {} FROM {})",
                table.sort_key.join(", "),
                tombstoned_keys_table_name(table)
            )),
            "sql: {sql}"
        );
    }

    #[test]
    fn completion_is_confirmed_against_the_data_not_system_mutations() {
        let tables = all_swept_tables();
        let table = find_table_ending_in(&tables, "gl_edge");
        let sql = build_count_remaining_tombstoned_rows_sql(table);

        assert!(sql.contains("count() AS remaining"), "sql: {sql}");
        assert!(
            sql.contains(&tombstoned_keys_table_name(table)),
            "sql: {sql}"
        );
        assert!(!sql.contains("system.mutations"), "sql: {sql}");
    }

    #[test]
    fn completion_count_skips_index_analysis_for_large_key_sets() {
        let tables = all_swept_tables();
        let sql =
            build_count_remaining_tombstoned_rows_sql(find_table_ending_in(&tables, "gl_edge"));

        assert!(
            sql.contains("use_index_for_in_with_subqueries_max_values"),
            "an uncapped IN-set index analysis is unkillable and ignores max_execution_time; sql: {sql}"
        );
    }

    #[test]
    fn delete_is_submitted_without_a_synchronous_wait() {
        let tables = all_swept_tables();
        let sql = build_delete_tombstoned_keys_sql(find_table_ending_in(&tables, "gl_edge"));

        assert!(
            sql.contains("lightweight_deletes_sync = 0"),
            "a synchronous wait holds a silent connection the Cloud path drops at ~20min; sql: {sql}"
        );
        assert!(!sql.contains("system.mutations"), "sql: {sql}");
    }
}
