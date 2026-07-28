use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse_client::FromArrowColumn;
use ontology::constants::TRAVERSAL_PATH_COLUMN;
use tracing::{info, warn};

use crate::checkpoint::CheckpointStore;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::durability::WriteDurability;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use gkg_server_config::{ScheduleConfiguration, TableCleanupConfig};

const CHECKPOINT_KEY_PREFIX: &str = "maintenance.table_cleanup";
const NAMESPACE_ROOT_EXPRESSION: &str = "arrayStringConcat(arraySlice(arrayFilter(part -> part != \'\', splitByChar(\'/\', traversal_path)), 1, 2), \'/\') || \'/\'";
const NAMESPACE_ROOT_PREDICATE: &str = "startsWith(traversal_path, {root:String})";

struct ReplacingMergeTreeTable {
    name: String,
    sort_key: Vec<String>,
}

impl ReplacingMergeTreeTable {
    fn can_scope_by_namespace_root(&self) -> bool {
        self.sort_key.first().map(String::as_str) == Some(TRAVERSAL_PATH_COLUMN)
    }

    fn namespace_root_expression(&self) -> &'static str {
        if self.can_scope_by_namespace_root() {
            NAMESPACE_ROOT_EXPRESSION
        } else {
            "\'\'"
        }
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
        CHECKPOINT_KEY_PREFIX
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let started = Instant::now();
        let result = self.apply_tombstones_to_all_tables().await;
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics
            .record_run(self.name(), outcome, started.elapsed().as_secs_f64());
        result
    }
}

impl TableCleanup {
    async fn apply_tombstones_to_all_tables(&self) -> Result<(), TaskError> {
        let mut failed = 0u64;
        let mut scopes_swept = 0u64;

        for table in &self.tables {
            let started = Instant::now();
            match self.apply_tombstones_to_table(table).await {
                Ok(scopes) => {
                    scopes_swept += scopes;
                    let elapsed = started.elapsed().as_secs_f64();
                    self.metrics.record_query_duration(&table.name, elapsed);
                    info!(
                        table = table.name,
                        scopes,
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

        let tables = self.tables.len();
        info!(tables, failed, scopes_swept, "table cleanup complete");

        if failed > 0 {
            return Err(TaskError::new(format!(
                "{failed}/{tables} tables failed to apply tombstones"
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
        let roots = self
            .list_namespace_roots_with_new_tombstones(table, cursor)
            .await?;

        for root in &roots {
            self.delete_tombstoned_keys(table, root.as_deref()).await?;
        }

        self.save_cursor(&table.name, &watermark).await?;
        Ok(roots.len() as u64)
    }

    async fn list_namespace_roots_with_new_tombstones(
        &self,
        table: &ReplacingMergeTreeTable,
        cursor: DateTime<Utc>,
    ) -> Result<Vec<Option<String>>, TaskError> {
        let batches = self
            .graph
            .query(&build_namespace_roots_with_new_tombstones_sql(table))
            .param("cursor", cursor.format(TIMESTAMP_FORMAT).to_string())
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;
        Ok(String::extract_column(&batches, 0)
            .map_err(TaskError::new)?
            .into_iter()
            .map(|root| Some(root).filter(|root| !root.is_empty()))
            .collect())
    }

    async fn delete_tombstoned_keys(
        &self,
        table: &ReplacingMergeTreeTable,
        namespace_root: Option<&str>,
    ) -> Result<(), TaskError> {
        self.graph
            .query(&build_delete_tombstoned_keys_sql(
                table,
                namespace_root,
                self.config.delete_timeout_secs,
            ))
            .param("root", namespace_root.unwrap_or_default().to_string())
            .execute()
            .await
            .map_err(TaskError::new)
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
}

fn checkpoint_key_for_table(table: &str) -> String {
    format!("{CHECKPOINT_KEY_PREFIX}.{table}")
}

fn build_namespace_roots_with_new_tombstones_sql(table: &ReplacingMergeTreeTable) -> String {
    format!(
        "SELECT DISTINCT {} AS root FROM {} WHERE _deleted AND _version > {{cursor:String}}",
        table.namespace_root_expression(),
        table.name
    )
}

fn build_delete_tombstoned_keys_sql(
    table: &ReplacingMergeTreeTable,
    namespace_root: Option<&str>,
    delete_timeout_secs: u64,
) -> String {
    let keys = table.sort_key.join(", ");
    let name = &table.name;
    let scope = if namespace_root.is_some() {
        NAMESPACE_ROOT_PREDICATE
    } else {
        "1 = 1"
    };
    format!(
        "DELETE FROM {name} WHERE {scope} AND ({keys}) IN ( \
           SELECT {keys} FROM ( \
             SELECT {keys}, _deleted FROM {name} \
             WHERE {scope} \
             ORDER BY {keys}, _version DESC \
             LIMIT 1 BY {keys} \
           ) WHERE _deleted \
         ) \
         SETTINGS lightweight_deletes_sync = 2, max_execution_time = {delete_timeout_secs}"
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
    fn delete_considers_only_the_newest_version_of_each_key() {
        let tables = all_replacing_merge_tree_tables();
        let table = find_table_ending_in(&tables, "gl_edge");
        let sql = build_delete_tombstoned_keys_sql(table, Some("1/9/"), 60);

        assert!(sql.contains("LIMIT 1 BY"), "sql: {sql}");
        assert!(sql.contains("_version DESC"), "sql: {sql}");
        assert!(sql.contains("WHERE _deleted"), "sql: {sql}");
    }

    #[test]
    fn delete_waits_for_the_mutation_instead_of_polling_system_mutations() {
        let tables = all_replacing_merge_tree_tables();
        let sql =
            build_delete_tombstoned_keys_sql(find_table_ending_in(&tables, "gl_edge"), None, 900);

        assert!(sql.contains("lightweight_deletes_sync = 2"), "sql: {sql}");
        assert!(sql.contains("max_execution_time = 900"), "sql: {sql}");
        assert!(!sql.contains("system.mutations"), "sql: {sql}");
    }

    #[test]
    fn namespace_scoped_delete_prunes_both_the_outer_and_inner_scan() {
        let tables = all_replacing_merge_tree_tables();
        let sql = build_delete_tombstoned_keys_sql(
            find_table_ending_in(&tables, "gl_edge"),
            Some("1/9970/"),
            60,
        );

        assert_eq!(
            sql.matches("startsWith(traversal_path, {root:String})")
                .count(),
            2,
            "sql: {sql}"
        );
    }

    #[test]
    fn global_tables_are_swept_without_a_path_predicate() {
        let tables = all_replacing_merge_tree_tables();
        let user = find_table_ending_in(&tables, "gl_user");
        assert!(!user.can_scope_by_namespace_root());

        let sql = build_delete_tombstoned_keys_sql(user, None, 60);
        assert!(!sql.contains("traversal_path"), "sql: {sql}");
    }

    #[test]
    fn checkpoint_key_is_scoped_per_table() {
        assert_eq!(
            checkpoint_key_for_table("v85_gl_edge"),
            "maintenance.table_cleanup.v85_gl_edge"
        );
    }
}
