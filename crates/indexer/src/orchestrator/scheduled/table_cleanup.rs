use std::time::{Duration, Instant};

use arrow::datatypes::UInt64Type;
use async_trait::async_trait;
use chrono::Utc;
use futures::StreamExt;
use tracing::{info, warn};

use orbit_utils::arrow::ArrowUtils;

use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use orbit_server_config::{ScheduleConfiguration, TableCleanupConfig};

const TASK_NAME: &str = "maintenance.table_cleanup";
const TOMBSTONED_KEYS_TABLE_PREFIX: &str = "tombstone_sweep_keys";

// ── Pacing ───────────────────────────────────────────────────────────
const CONCURRENT_TABLE_SWEEPS: usize = 4;
const REMAINING_ROWS_POLL_INTERVAL: Duration = Duration::from_secs(5);

// ── ClickHouse settings ────────────────────────────────────
const STATEMENT_TIMEOUT_SECS: u64 = 7200; // 2 hours

const MAX_STATEMENT_MEMORY_BYTES: u64 = 8_000_000_000; // caps build memory
const SPILL_SORT_TO_DISK_ABOVE_BYTES: u64 = 2_000_000_000; // spill before the cap
const MAX_KEYS_FOR_INDEX_ANALYSIS: u64 = 1_000_000; // avoids stuck queries

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
    /// Delete cost tracks rows scanned, not rows removed, so concurrency is the only wall-clock lever.
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
        let window_end = window_end.format(TIMESTAMP_FORMAT).to_string();
        let quorum_writes = self.graph.has_quorum_writes();

        self.drop_tombstoned_keys_table(table).await?;
        for statement in build_tombstoned_keys_table_statements(table, quorum_writes) {
            self.graph
                .query(&statement)
                .param(
                    "window_start",
                    window_start.format(TIMESTAMP_FORMAT).to_string(),
                )
                .param("window_end", window_end.clone())
                .execute()
                .await
                .map_err(TaskError::new)?;
        }
        self.graph
            .query(&build_delete_tombstoned_keys_sql(table, quorum_writes))
            .param("window_end", window_end.clone())
            .execute()
            .await
            .map_err(TaskError::new)?;
        self.wait_until_tombstoned_keys_are_gone(table, &window_end)
            .await
    }

    /// `system.mutations` is not readable by the graph user; the data is the only completion signal.
    async fn wait_until_tombstoned_keys_are_gone(
        &self,
        table: &ReplacingMergeTreeTable,
        window_end: &str,
    ) -> Result<(), TaskError> {
        let deadline = Instant::now() + Duration::from_secs(STATEMENT_TIMEOUT_SECS);
        loop {
            match self
                .count_remaining_tombstoned_rows(table, window_end)
                .await
            {
                Ok(0) => return Ok(()),
                Ok(_) => {}
                Err(error)
                    if self.graph.has_quorum_writes() && is_replica_not_in_quorum(&error) => {}
                Err(error) => return Err(error),
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
        window_end: &str,
    ) -> Result<u64, TaskError> {
        let batches = self
            .graph
            .query(&build_count_remaining_tombstoned_rows_sql(
                table,
                self.graph.has_quorum_writes(),
            ))
            .param("window_end", window_end)
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

fn build_tombstoned_keys_table_statements(
    table: &ReplacingMergeTreeTable,
    quorum_writes: bool,
) -> Vec<String> {
    let keys = table.sort_key.join(", ");
    let keys_table = tombstoned_keys_table_name(table);

    if quorum_writes {
        let keys_with_tombstone_version = format!("{keys}, _version AS tombstone_version");
        let tombstoned_keys = select_tombstoned_keys(table, &keys_with_tombstone_version);
        // Replicated databases reject CREATE AS SELECT: schema inferred EMPTY, scan runs in a quorum-acked INSERT.
        vec![
            format!(
                "CREATE TABLE {keys_table} ENGINE = ReplicatedMergeTree ORDER BY ({keys}) \
                 EMPTY AS {tombstoned_keys}"
            ),
            format!(
                "INSERT INTO {keys_table} {tombstoned_keys} {}",
                full_table_scan_settings()
            ),
        ]
    } else {
        let tombstoned_keys = select_tombstoned_keys(table, &keys);
        vec![format!(
            "CREATE TABLE {keys_table} ENGINE = MergeTree ORDER BY ({keys}) \
             AS {tombstoned_keys} {}",
            full_table_scan_settings()
        )]
    }
}

/// Every key whose newest in-window version is a tombstone, as `columns`.
fn select_tombstoned_keys(table: &ReplacingMergeTreeTable, columns: &str) -> String {
    let keys = table.sort_key.join(", ");
    let source = &table.name;
    format!(
        "SELECT {columns} FROM ( \
           SELECT {keys}, _version, _deleted FROM {source} \
           WHERE _version > {{window_start:String}} AND _version <= {{window_end:String}} \
           ORDER BY {keys}, _version DESC \
           LIMIT 1 BY {keys} \
         ) WHERE _deleted"
    )
}

fn full_table_scan_settings() -> String {
    format!(
        "SETTINGS max_memory_usage = {MAX_STATEMENT_MEMORY_BYTES}, \
                  max_bytes_before_external_sort = {SPILL_SORT_TO_DISK_ABOVE_BYTES}, \
                  optimize_read_in_order = 1, max_execution_time = {STATEMENT_TIMEOUT_SECS}, \
                  send_progress_in_http_headers = 1"
    )
}

fn swept_rows_predicate(table: &ReplacingMergeTreeTable, quorum_writes: bool) -> String {
    let keys = table.sort_key.join(", ");
    let keys_table = tombstoned_keys_table_name(table);
    let mut predicate = format!(
        "({keys}) IN (SELECT {keys} FROM {keys_table}) AND _version <= {{window_end:String}}"
    );
    if quorum_writes {
        // Keeps a lagging replica from resurrecting older live rows.
        predicate.push_str(&format!(
            " AND ({keys}, _version) NOT IN (SELECT {keys}, tombstone_version FROM {keys_table})"
        ));
    }
    predicate
}

fn build_delete_tombstoned_keys_sql(
    table: &ReplacingMergeTreeTable,
    quorum_writes: bool,
) -> String {
    let nondeterministic_opt_in = if quorum_writes {
        ", allow_nondeterministic_mutations = 1"
    } else {
        ""
    };
    format!(
        "DELETE FROM {} WHERE {} \
         SETTINGS lightweight_deletes_sync = 0{nondeterministic_opt_in}, \
                  max_execution_time = {STATEMENT_TIMEOUT_SECS}",
        table.name,
        swept_rows_predicate(table, quorum_writes),
    )
}

fn build_count_remaining_tombstoned_rows_sql(
    table: &ReplacingMergeTreeTable,
    quorum_writes: bool,
) -> String {
    format!(
        "SELECT count() AS remaining FROM {} WHERE {} \
         SETTINGS use_index_for_in_with_subqueries_max_values = {MAX_KEYS_FOR_INDEX_ANALYSIS}, \
                  max_execution_time = {STATEMENT_TIMEOUT_SECS}",
        table.name,
        swept_rows_predicate(table, quorum_writes),
    )
}

fn is_replica_not_in_quorum(error: &TaskError) -> bool {
    let message = error.to_string();
    message.contains("REPLICA_IS_NOT_IN_QUORUM") || message.contains("Code: 289")
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

    fn joined_build_statements(table: &ReplacingMergeTreeTable, quorum_writes: bool) -> String {
        build_tombstoned_keys_table_statements(table, quorum_writes).join("; ")
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
        let sql = joined_build_statements(find_table_ending_in(&tables, "gl_edge"), false);

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
        let sql = joined_build_statements(find_table_ending_in(&tables, "gl_edge"), false);

        assert!(sql.contains("LIMIT 1 BY"), "sql: {sql}");
        assert!(sql.contains("_version DESC"), "sql: {sql}");
        assert!(sql.contains("WHERE _deleted"), "sql: {sql}");
    }

    #[test]
    fn delete_reads_keys_from_the_materialised_table_not_the_source() {
        let tables = all_swept_tables();
        let table = find_table_ending_in(&tables, "gl_edge");
        let sql = build_delete_tombstoned_keys_sql(table, false);

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
    fn delete_is_bounded_to_the_version_window() {
        let tables = all_swept_tables();
        let table = find_table_ending_in(&tables, "gl_edge");

        for quorum_writes in [false, true] {
            let sql = build_delete_tombstoned_keys_sql(table, quorum_writes);
            assert!(
                sql.contains("_version <= {window_end:String}"),
                "a row re-created after the key snapshot must survive the delete; sql: {sql}"
            );
        }
    }

    #[test]
    fn completion_is_confirmed_against_the_data_not_system_mutations() {
        let tables = all_swept_tables();
        let table = find_table_ending_in(&tables, "gl_edge");
        let sql = build_count_remaining_tombstoned_rows_sql(table, false);

        assert!(sql.contains("count() AS remaining"), "sql: {sql}");
        assert!(
            sql.contains(&tombstoned_keys_table_name(table)),
            "sql: {sql}"
        );
        assert!(!sql.contains("system.mutations"), "sql: {sql}");
    }

    #[test]
    fn completion_count_matches_the_delete_predicate() {
        let tables = all_swept_tables();
        let table = find_table_ending_in(&tables, "gl_edge");

        for quorum_writes in [false, true] {
            let predicate = swept_rows_predicate(table, quorum_writes);
            assert!(
                build_delete_tombstoned_keys_sql(table, quorum_writes).contains(&predicate),
                "quorum_writes={quorum_writes}"
            );
            assert!(
                build_count_remaining_tombstoned_rows_sql(table, quorum_writes)
                    .contains(&predicate),
                "\"0 remaining\" must mean the delete's exact target set is gone; \
                 quorum_writes={quorum_writes}"
            );
        }
    }

    #[test]
    fn completion_count_skips_index_analysis_for_large_key_sets() {
        let tables = all_swept_tables();
        let sql = build_count_remaining_tombstoned_rows_sql(
            find_table_ending_in(&tables, "gl_edge"),
            false,
        );

        assert!(
            sql.contains("use_index_for_in_with_subqueries_max_values"),
            "an uncapped IN-set index analysis is unkillable and ignores max_execution_time; sql: {sql}"
        );
    }

    #[test]
    fn the_build_keeps_the_http_connection_alive() {
        let tables = all_swept_tables();
        let sql = joined_build_statements(find_table_ending_in(&tables, "gl_edge"), false);

        assert!(
            sql.contains("send_progress_in_http_headers = 1"),
            "the biggest tables build for ~45min and write nothing until done; without \
             periodic progress headers an idle-connection timeout drops the build; sql: {sql}"
        );
    }

    #[test]
    fn delete_is_submitted_without_a_synchronous_wait() {
        let tables = all_swept_tables();
        let sql = build_delete_tombstoned_keys_sql(find_table_ending_in(&tables, "gl_edge"), false);

        assert!(
            sql.contains("lightweight_deletes_sync = 0"),
            "a synchronous wait holds a silent connection the Cloud path drops at ~20min; sql: {sql}"
        );
        assert!(!sql.contains("system.mutations"), "sql: {sql}");
    }

    #[test]
    fn quorum_scratch_is_replicated_and_carries_the_tombstone_version() {
        let tables = all_swept_tables();
        let table = find_table_ending_in(&tables, "gl_edge");

        let statements = build_tombstoned_keys_table_statements(table, true);
        assert_eq!(
            statements.len(),
            2,
            "Replicated databases reject CREATE AS SELECT; the schema and the scan \
             must be separate statements"
        );
        assert!(
            statements[0].contains("ENGINE = ReplicatedMergeTree"),
            "a Replicated database replicates DDL only; a plain MergeTree scratch \
             would leave every other replica's delete reading an empty key set; sql: {}",
            statements[0]
        );
        assert!(statements[0].contains("EMPTY AS"), "sql: {}", statements[0]);
        assert!(
            statements[1].starts_with(&format!(
                "INSERT INTO {}",
                tombstoned_keys_table_name(table)
            )),
            "sql: {}",
            statements[1]
        );
        for statement in &statements {
            assert!(
                statement.contains("_version AS tombstone_version"),
                "sql: {statement}"
            );
        }

        let statements = build_tombstoned_keys_table_statements(table, false);
        assert_eq!(statements.len(), 1);
        assert!(
            statements[0].contains("ENGINE = MergeTree"),
            "sql: {}",
            statements[0]
        );
        assert!(
            !statements[0].contains("tombstone_version"),
            "sql: {}",
            statements[0]
        );
    }

    #[test]
    fn quorum_delete_never_removes_the_newest_tombstone_row() {
        let tables = all_swept_tables();
        let table = find_table_ending_in(&tables, "gl_edge");
        let keys = table.sort_key.join(", ");

        let sql = build_delete_tombstoned_keys_sql(table, true);
        assert!(
            sql.contains(&format!(
                "({keys}, _version) NOT IN (SELECT {keys}, tombstone_version FROM {})",
                tombstoned_keys_table_name(table)
            )),
            "deleting the tombstone against a partially replicated key set can \
             leave an older live row as the key's newest version; sql: {sql}"
        );

        assert!(
            !build_delete_tombstoned_keys_sql(table, false).contains("NOT IN"),
            "the shared-storage path has no replication race and removes the whole key"
        );
    }

    #[test]
    fn quorum_delete_declares_its_subquery_mutation_nondeterministic() {
        let tables = all_swept_tables();
        let table = find_table_ending_in(&tables, "gl_edge");

        assert!(
            build_delete_tombstoned_keys_sql(table, true)
                .contains("allow_nondeterministic_mutations = 1"),
            "replicated tables reject subquery mutations outright without the opt-in"
        );
        assert!(
            !build_delete_tombstoned_keys_sql(table, false)
                .contains("allow_nondeterministic_mutations")
        );
    }

    #[test]
    fn a_lagging_replica_read_is_not_a_sweep_failure() {
        assert!(is_replica_not_in_quorum(&TaskError::new(
            "Code: 289. DB::Exception: Replica doesn't have part ... REPLICA_IS_NOT_IN_QUORUM"
        )));
        assert!(!is_replica_not_in_quorum(&TaskError::new(
            "Code: 241. DB::Exception: Memory limit exceeded"
        )));
    }
}
