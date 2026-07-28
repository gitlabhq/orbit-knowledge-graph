use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::UInt64Type;
use async_trait::async_trait;
use chrono::{DateTime, TimeDelta, Utc};
use tracing::{info, warn};

use gkg_utils::arrow::ArrowUtils;

use crate::checkpoint::CheckpointStore;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::durability::WriteDurability;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use gkg_server_config::{ScheduleConfiguration, TableCleanupConfig};

const TASK_NAME: &str = "maintenance.table_cleanup";
const KEYS_TABLE: &str = "tombstone_sweep_keys";

struct ReplacingMergeTreeTable {
    name: String,
    sort_key: Vec<String>,
}

struct VersionWindow {
    start: DateTime<Utc>,
    end: DateTime<Utc>,
}

pub struct TableCleanup {
    graph: ArrowClickHouseClient,
    tables: Vec<ReplacingMergeTreeTable>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    metrics: ScheduledTaskMetrics,
    config: TableCleanupConfig,
}

impl TableCleanup {
    pub fn new(
        graph: ArrowClickHouseClient,
        ontology: &ontology::Ontology,
        checkpoint_store: Arc<dyn CheckpointStore>,
        metrics: ScheduledTaskMetrics,
        config: TableCleanupConfig,
    ) -> Self {
        Self {
            graph,
            tables: list_replacing_merge_tree_tables(ontology),
            checkpoint_store,
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
    async fn sweep_tombstones_from_all_tables(&self) -> Result<(), TaskError> {
        let mut failed = 0u64;

        for table in &self.tables {
            let started = Instant::now();
            match self.sweep_tombstones_from_table(table).await {
                Ok(()) => {
                    let elapsed = started.elapsed().as_secs_f64();
                    self.metrics.record_query_duration(&table.name, elapsed);
                    info!(
                        table = table.name,
                        duration_ms = (elapsed * 1000.0) as u64,
                        "swept tombstoned keys"
                    );
                }
                Err(error) => {
                    failed += 1;
                    self.metrics.record_error(self.name(), "sweep_tombstones");
                    warn!(table = table.name, %error, "failed to sweep tombstoned keys");
                }
            }
            if let Err(error) = self.drop_keys_table().await {
                warn!(table = table.name, %error, "failed to drop the keys table");
            }
        }

        let tables = self.tables.len();
        info!(tables, failed, "tombstone sweep complete");

        if failed > 0 {
            return Err(TaskError::new(format!(
                "{failed}/{tables} tables failed to sweep"
            )));
        }
        Ok(())
    }

    async fn sweep_tombstones_from_table(
        &self,
        table: &ReplacingMergeTreeTable,
    ) -> Result<(), TaskError> {
        let window = self.next_window(&table.name).await?;

        self.drop_keys_table().await?;
        self.graph
            .query(&build_tombstoned_keys_table_sql(table, &self.config))
            .param(
                "window_start",
                window.start.format(TIMESTAMP_FORMAT).to_string(),
            )
            .param(
                "window_end",
                window.end.format(TIMESTAMP_FORMAT).to_string(),
            )
            .execute()
            .await
            .map_err(TaskError::new)?;
        self.graph
            .query(&build_delete_tombstoned_keys_sql(table, &self.config))
            .execute()
            .await
            .map_err(TaskError::new)?;
        self.wait_until_tombstoned_keys_are_gone(table).await?;

        self.save_cursor(&table.name, &window.end).await
    }

    /// The window always ends at now so a run applies the newest tombstones, and
    /// always spans at least `window_secs` so a first run with no checkpoint is
    /// still useful. A missed run widens it back to the last checkpoint, capped
    /// at `max_backlog_secs` to keep any single run bounded.
    async fn next_window(&self, table: &str) -> Result<VersionWindow, TaskError> {
        let completed = self
            .checkpoint_store
            .load(&checkpoint_key_for_table(table))
            .await
            .map_err(TaskError::new)?
            .map(|checkpoint| checkpoint.watermark);
        let end = Utc::now();
        let ago = |secs: u64| end - TimeDelta::seconds(secs as i64);
        let resume = completed
            .map(|watermark| watermark - TimeDelta::seconds(self.config.lookback_secs as i64))
            .unwrap_or_else(|| ago(self.config.window_secs));
        let start = resume
            .min(ago(self.config.window_secs))
            .max(ago(self.config.max_backlog_secs));
        Ok(VersionWindow { start, end })
    }

    async fn save_cursor(&self, table: &str, watermark: &DateTime<Utc>) -> Result<(), TaskError> {
        self.checkpoint_store
            .save_completed(
                &checkpoint_key_for_table(table),
                watermark,
                WriteDurability::Durable,
            )
            .await
            .map_err(TaskError::new)
    }

    /// `lightweight_deletes_sync = 1` returns before the mutation is guaranteed
    /// applied, and `system.mutations` is not readable by the graph user, so
    /// completion is confirmed against the data itself.
    async fn wait_until_tombstoned_keys_are_gone(
        &self,
        table: &ReplacingMergeTreeTable,
    ) -> Result<(), TaskError> {
        let deadline = Instant::now() + Duration::from_secs(self.config.statement_timeout_secs);
        loop {
            if self.count_remaining_tombstoned_rows(table).await? == 0 {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(TaskError::new(format!(
                    "delete on {} left rows behind after {}s",
                    table.name, self.config.statement_timeout_secs
                )));
            }
            tokio::time::sleep(Duration::from_secs(self.config.verify_poll_interval_secs)).await;
        }
    }

    async fn count_remaining_tombstoned_rows(
        &self,
        table: &ReplacingMergeTreeTable,
    ) -> Result<u64, TaskError> {
        let batches = self
            .graph
            .query(&build_count_remaining_tombstoned_rows_sql(
                table,
                &self.config,
            ))
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;
        Ok(batches
            .first()
            .and_then(|batch| ArrowUtils::get_column::<UInt64Type>(batch, "remaining", 0))
            .unwrap_or(0))
    }

    async fn drop_keys_table(&self) -> Result<(), TaskError> {
        self.graph
            .query(&format!("DROP TABLE IF EXISTS {KEYS_TABLE}"))
            .execute()
            .await
            .map_err(TaskError::new)
    }
}

fn build_tombstoned_keys_table_sql(
    table: &ReplacingMergeTreeTable,
    config: &TableCleanupConfig,
) -> String {
    let keys = table.sort_key.join(", ");
    let source = &table.name;
    format!(
        "CREATE TABLE {KEYS_TABLE} ENGINE = MergeTree ORDER BY ({keys}) \
         AS SELECT {keys} FROM ( \
           SELECT {keys}, _deleted FROM {source} \
           WHERE _version > {{window_start:String}} AND _version <= {{window_end:String}} \
           ORDER BY {keys}, _version DESC \
           LIMIT 1 BY {keys} \
         ) WHERE _deleted \
         SETTINGS max_memory_usage = {}, max_bytes_before_external_sort = {}, \
                  optimize_read_in_order = 1, max_execution_time = {}",
        config.max_memory_bytes,
        config.max_memory_bytes / 4,
        config.statement_timeout_secs
    )
}

fn build_delete_tombstoned_keys_sql(
    table: &ReplacingMergeTreeTable,
    config: &TableCleanupConfig,
) -> String {
    let keys = table.sort_key.join(", ");
    format!(
        "DELETE FROM {} WHERE ({keys}) IN (SELECT {keys} FROM {KEYS_TABLE}) \
         SETTINGS lightweight_deletes_sync = 1, max_execution_time = {}",
        table.name, config.statement_timeout_secs
    )
}

fn checkpoint_key_for_table(table: &str) -> String {
    format!("{TASK_NAME}.{table}")
}

fn build_count_remaining_tombstoned_rows_sql(
    table: &ReplacingMergeTreeTable,
    config: &TableCleanupConfig,
) -> String {
    let keys = table.sort_key.join(", ");
    format!(
        "SELECT count() AS remaining FROM {} WHERE ({keys}) IN (SELECT {keys} FROM {KEYS_TABLE}) \
         SETTINGS max_execution_time = {}",
        table.name, config.statement_timeout_secs
    )
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

#[cfg(test)]
mod tests {
    use super::*;

    fn all_tables() -> Vec<ReplacingMergeTreeTable> {
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
        for table in &all_tables() {
            assert!(!table.sort_key.is_empty(), "table '{}'", table.name);
        }
    }

    #[test]
    fn auxiliary_tables_are_not_swept() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        for aux in ontology.auxiliary_tables() {
            assert!(
                !all_tables()
                    .iter()
                    .any(|table| table.name.ends_with(&aux.name)),
                "auxiliary table '{}' must not be swept",
                aux.name
            );
        }
    }

    #[test]
    fn keys_table_name_avoids_the_schema_version_prefix() {
        assert!(!KEYS_TABLE.starts_with('v'));
        assert!(
            !KEYS_TABLE.contains(&format!("v{}_", *SCHEMA_VERSION)),
            "schema garbage collection globs v<version>_* and would drop the keys table"
        );
    }

    #[test]
    fn a_lookback_window_absorbs_late_arriving_tombstones() {
        let config = TableCleanupConfig::default();
        assert!(
            config.lookback_secs >= 86400,
            "_version is a source watermark, so the window must outlast replication delay"
        );
    }

    #[tokio::test]
    async fn a_first_run_sweeps_the_recent_past_not_1970() {
        let config = TableCleanupConfig::default();
        let end = Utc::now();
        let earliest = end - TimeDelta::seconds(config.max_backlog_secs as i64);
        let standard = end - TimeDelta::seconds(config.window_secs as i64);

        assert!(
            standard > earliest,
            "the standard window must sit inside the backlog cap so a first run covers recent time"
        );
        assert!(
            earliest > DateTime::<Utc>::UNIX_EPOCH,
            "a window anchored at the epoch would take thousands of runs to reach the present"
        );
    }

    #[test]
    fn keys_are_bounded_to_one_version_window() {
        let tables = all_tables();
        let sql = build_tombstoned_keys_table_sql(
            find_table_ending_in(&tables, "gl_edge"),
            &TableCleanupConfig::default(),
        );

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
        let tables = all_tables();
        let sql = build_tombstoned_keys_table_sql(
            find_table_ending_in(&tables, "gl_edge"),
            &TableCleanupConfig::default(),
        );

        assert!(sql.contains("LIMIT 1 BY"), "sql: {sql}");
        assert!(sql.contains("_version DESC"), "sql: {sql}");
        assert!(sql.contains("WHERE _deleted"), "sql: {sql}");
    }

    #[test]
    fn delete_reads_keys_from_the_materialised_table_not_the_source() {
        let tables = all_tables();
        let table = find_table_ending_in(&tables, "gl_edge");
        let sql = build_delete_tombstoned_keys_sql(table, &TableCleanupConfig::default());

        assert!(
            sql.contains(&format!(
                "IN (SELECT {} FROM {KEYS_TABLE})",
                table.sort_key.join(", ")
            )),
            "sql: {sql}"
        );
    }

    #[test]
    fn completion_is_confirmed_against_the_data_not_system_mutations() {
        let tables = all_tables();
        let table = find_table_ending_in(&tables, "gl_edge");
        let sql = build_count_remaining_tombstoned_rows_sql(table, &TableCleanupConfig::default());

        assert!(sql.contains("count() AS remaining"), "sql: {sql}");
        assert!(sql.contains(KEYS_TABLE), "sql: {sql}");
        assert!(!sql.contains("system.mutations"), "sql: {sql}");
    }

    #[test]
    fn delete_waits_synchronously_instead_of_reading_system_mutations() {
        let tables = all_tables();
        let sql = build_delete_tombstoned_keys_sql(
            find_table_ending_in(&tables, "gl_edge"),
            &TableCleanupConfig::default(),
        );

        assert!(sql.contains("lightweight_deletes_sync = 1"), "sql: {sql}");
        assert!(!sql.contains("system.mutations"), "sql: {sql}");
    }
}
