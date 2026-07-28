use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::datatypes::UInt64Type;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use gkg_utils::arrow::ArrowUtils;
use tracing::{info, warn};

use crate::checkpoint::CheckpointStore;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::durability::WriteDurability;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use gkg_server_config::{ScheduleConfiguration, TableCleanupConfig};

const CHECKPOINT_KEY_PREFIX: &str = "maintenance.table_cleanup";
const MUTATION_POLL_INTERVAL: Duration = Duration::from_secs(15);

struct ReplacingMergeTreeTable {
    name: String,
    sort_key: Vec<String>,
}

impl ReplacingMergeTreeTable {
    fn pending_deletes_table_name(&self) -> String {
        format!("{}_pending_deletes", self.name)
    }

    fn sort_key_as_sql_list(&self) -> String {
        self.sort_key.join(", ")
    }
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
        let tables = list_replacing_merge_tree_tables(ontology);
        Self {
            graph,
            tables,
            checkpoint_store,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl ScheduledTask for TableCleanup {
    fn name(&self) -> &str {
        CHECKPOINT_KEY_PREFIX
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let start = Instant::now();

        let result = self.apply_tombstones_to_all_tables().await;

        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics.record_run(self.name(), outcome, duration);

        result
    }
}

impl TableCleanup {
    async fn apply_tombstones_to_all_tables(&self) -> Result<(), TaskError> {
        let mut cleaned = 0u64;
        let mut failed = 0u64;
        let mut rows_deleted = 0u64;

        for table in &self.tables {
            let table_start = Instant::now();
            match self.apply_tombstones_to_table(table).await {
                Ok(keys) => {
                    cleaned += 1;
                    rows_deleted += keys;
                    let elapsed = table_start.elapsed().as_secs_f64();
                    self.metrics.record_query_duration(&table.name, elapsed);
                    info!(
                        table = table.name,
                        keys,
                        duration_ms = (elapsed * 1000.0) as u64,
                        "applied pending tombstones"
                    );
                }
                Err(error) => {
                    failed += 1;
                    self.metrics.record_error(self.name(), "apply_tombstones");
                    warn!(table = table.name, %error, "failed to apply pending tombstones");
                }
            }
        }

        info!(cleaned, failed, rows_deleted, "table cleanup complete");

        if failed > 0 {
            return Err(TaskError::new(format!(
                "{failed}/{} tables failed to apply tombstones",
                self.tables.len()
            )));
        }

        Ok(())
    }

    async fn apply_tombstones_to_table(
        &self,
        table: &ReplacingMergeTreeTable,
    ) -> Result<u64, TaskError> {
        let watermark = Utc::now();
        let cursor = self.load_cursor(&table.name).await?;

        self.drop_pending_deletes_table(table).await?;
        self.graph
            .query(&build_pending_deletes_table_sql(table))
            .param("cursor", cursor.format(TIMESTAMP_FORMAT).to_string())
            .execute()
            .await
            .map_err(TaskError::new)?;

        let keys = self.count_pending_delete_keys(table).await?;
        if keys > 0 {
            self.graph
                .query(&build_delete_pending_keys_sql(table))
                .execute()
                .await
                .map_err(TaskError::new)?;
            self.wait_for_delete_mutation_to_finish(table).await?;
        }

        self.drop_pending_deletes_table(table).await?;
        self.save_cursor(&table.name, &watermark).await?;
        Ok(keys)
    }

    async fn load_cursor(&self, table: &str) -> Result<DateTime<Utc>, TaskError> {
        Ok(self
            .checkpoint_store
            .load(&checkpoint_key_for_table(table))
            .await
            .map_err(TaskError::new)?
            .map(|checkpoint| checkpoint.watermark)
            .unwrap_or(DateTime::<Utc>::UNIX_EPOCH))
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

    async fn count_pending_delete_keys(
        &self,
        table: &ReplacingMergeTreeTable,
    ) -> Result<u64, TaskError> {
        let batches = self
            .graph
            .query("SELECT count() AS keys FROM {table:Identifier}")
            .param("table", table.pending_deletes_table_name())
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;
        Ok(batches
            .first()
            .and_then(|batch| ArrowUtils::get_column::<UInt64Type>(batch, "keys", 0))
            .unwrap_or(0))
    }

    async fn drop_pending_deletes_table(
        &self,
        table: &ReplacingMergeTreeTable,
    ) -> Result<(), TaskError> {
        self.graph
            .query("DROP TABLE IF EXISTS {table:Identifier}")
            .param("table", table.pending_deletes_table_name())
            .execute()
            .await
            .map_err(TaskError::new)
    }

    async fn wait_for_delete_mutation_to_finish(
        &self,
        table: &ReplacingMergeTreeTable,
    ) -> Result<(), TaskError> {
        let deadline = Instant::now() + self.config.mutation_timeout();
        loop {
            if self.delete_mutation_finished(table).await? {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(TaskError::new(format!(
                    "delete mutation on {} did not finish within {:?}",
                    table.name,
                    self.config.mutation_timeout()
                )));
            }
            tokio::time::sleep(MUTATION_POLL_INTERVAL).await;
        }
    }

    async fn delete_mutation_finished(
        &self,
        table: &ReplacingMergeTreeTable,
    ) -> Result<bool, TaskError> {
        let batches = self
            .graph
            .query(
                "SELECT count() AS unfinished FROM system.mutations \
                 WHERE database = currentDatabase() AND table = {table:String} AND is_done = 0",
            )
            .param("table", table.name.clone())
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;
        let unfinished = batches
            .first()
            .and_then(|batch| ArrowUtils::get_column::<UInt64Type>(batch, "unfinished", 0))
            .unwrap_or(0);
        Ok(unfinished == 0)
    }
}

fn checkpoint_key_for_table(table: &str) -> String {
    format!("{CHECKPOINT_KEY_PREFIX}.{table}")
}

fn build_pending_deletes_table_sql(table: &ReplacingMergeTreeTable) -> String {
    let keys = table.sort_key_as_sql_list();
    let source = &table.name;
    let pending = table.pending_deletes_table_name();
    format!(
        "CREATE TABLE {pending} ENGINE = MergeTree ORDER BY ({keys}) \
         AS SELECT {keys} FROM ( \
           SELECT {keys}, _deleted FROM {source} \
           WHERE _version > {{cursor:String}} \
           ORDER BY {keys}, _version DESC \
           LIMIT 1 BY {keys} \
         ) WHERE _deleted"
    )
}

fn build_delete_pending_keys_sql(table: &ReplacingMergeTreeTable) -> String {
    let keys = table.sort_key_as_sql_list();
    format!(
        "DELETE FROM {} WHERE ({keys}) IN (SELECT {keys} FROM {})",
        table.name,
        table.pending_deletes_table_name()
    )
}

fn list_replacing_merge_tree_tables(ontology: &ontology::Ontology) -> Vec<ReplacingMergeTreeTable> {
    let mut tables = Vec::new();

    for node in ontology.nodes() {
        let Some(sort_key) = ontology.sort_key_for_table(&node.destination_table) else {
            continue;
        };
        tables.push(ReplacingMergeTreeTable {
            name: prefixed_table_name(&node.destination_table, *SCHEMA_VERSION),
            sort_key: sort_key.to_vec(),
        });
    }
    for edge_table in ontology.edge_tables() {
        let Some(sort_key) = ontology.sort_key_for_table(edge_table) else {
            continue;
        };
        tables.push(ReplacingMergeTreeTable {
            name: prefixed_table_name(edge_table, *SCHEMA_VERSION),
            sort_key: sort_key.to_vec(),
        });
    }

    tables
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_replacing_merge_tree_tables() -> Vec<ReplacingMergeTreeTable> {
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
        for table in &all_replacing_merge_tree_tables() {
            assert!(
                !table.sort_key.is_empty(),
                "table '{}' has no sort key, so its rows have no dedup identity",
                table.name
            );
        }
    }

    #[test]
    fn auxiliary_tables_are_not_swept() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let tables = all_replacing_merge_tree_tables();

        for aux in ontology.auxiliary_tables() {
            assert!(
                !tables.iter().any(|table| table.name.ends_with(&aux.name)),
                "auxiliary table '{}' must not be swept",
                aux.name
            );
        }
    }

    #[test]
    fn every_table_carries_the_schema_version_prefix() {
        let prefix = format!("v{}_", *SCHEMA_VERSION);
        for table in &all_replacing_merge_tree_tables() {
            assert!(
                table.name.starts_with(&prefix),
                "table '{}' is missing the schema version prefix",
                table.name
            );
        }
    }

    #[test]
    fn pending_deletes_selects_keys_whose_newest_version_is_a_tombstone() {
        let tables = all_replacing_merge_tree_tables();
        let sql = build_pending_deletes_table_sql(find_table_ending_in(&tables, "gl_edge"));

        assert!(sql.contains("LIMIT 1 BY"), "sql: {sql}");
        assert!(sql.contains("_version DESC"), "sql: {sql}");
        assert!(sql.contains("WHERE _deleted"), "sql: {sql}");
        assert!(sql.contains("_version > {cursor:String}"), "sql: {sql}");
    }

    #[test]
    fn delete_reads_keys_from_the_materialised_table_not_the_source() {
        let tables = all_replacing_merge_tree_tables();
        let table = find_table_ending_in(&tables, "gl_edge");
        let sql = build_delete_pending_keys_sql(table);

        assert!(
            sql.starts_with(&format!("DELETE FROM {}", table.name)),
            "sql: {sql}"
        );
        assert!(
            sql.contains(&format!(
                "IN (SELECT {} FROM {}",
                table.sort_key_as_sql_list(),
                table.pending_deletes_table_name()
            )),
            "sql: {sql}"
        );
    }

    #[test]
    fn checkpoint_key_is_scoped_per_table() {
        assert_eq!(
            checkpoint_key_for_table("v85_gl_edge"),
            "maintenance.table_cleanup.v85_gl_edge"
        );
    }
}
