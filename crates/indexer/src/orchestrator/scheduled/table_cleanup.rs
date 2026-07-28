use std::sync::Arc;
use std::time::Instant;

use arrow::datatypes::UInt64Type;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use clickhouse_client::FromArrowColumn;
use gkg_utils::arrow::ArrowUtils;
use ontology::constants::TRAVERSAL_PATH_COLUMN;
use tracing::{info, warn};

use crate::checkpoint::CheckpointStore;
use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::durability::WriteDurability;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use gkg_server_config::{ScheduleConfiguration, TableCleanupConfig};

const CHECKPOINT_KEY_PREFIX: &str = "maintenance.table_cleanup";

struct ReplacingMergeTreeTable {
    name: String,
    sort_key: Vec<String>,
}

impl ReplacingMergeTreeTable {
    fn sort_key_as_sql_list(&self) -> String {
        self.sort_key.join(", ")
    }

    fn sorts_by_traversal_path(&self) -> bool {
        self.sort_key.first().map(String::as_str) == Some(TRAVERSAL_PATH_COLUMN)
    }
}

enum DeleteScope {
    NamespaceRoot(String),
    WholeTable,
}

impl DeleteScope {
    fn predicate(&self) -> &'static str {
        match self {
            Self::NamespaceRoot(_) => "startsWith(traversal_path, {root:String})",
            Self::WholeTable => "1 = 1",
        }
    }

    fn root(&self) -> &str {
        match self {
            Self::NamespaceRoot(root) => root,
            Self::WholeTable => "",
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
        let mut scopes_swept = 0u64;

        for table in &self.tables {
            let table_start = Instant::now();
            match self.apply_tombstones_to_table(table).await {
                Ok(scopes) => {
                    cleaned += 1;
                    scopes_swept += scopes;
                    let elapsed = table_start.elapsed().as_secs_f64();
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

        info!(cleaned, failed, scopes_swept, "table cleanup complete");

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

        let scopes = self.list_scopes_with_new_tombstones(table, cursor).await?;
        for scope in &scopes {
            self.delete_tombstoned_keys_in_scope(table, scope).await?;
        }

        self.save_cursor(&table.name, &watermark).await?;
        Ok(scopes.len() as u64)
    }

    async fn list_scopes_with_new_tombstones(
        &self,
        table: &ReplacingMergeTreeTable,
        cursor: DateTime<Utc>,
    ) -> Result<Vec<DeleteScope>, TaskError> {
        let cursor = cursor.format(TIMESTAMP_FORMAT).to_string();
        if !table.sorts_by_traversal_path() {
            let batches = self
                .graph
                .query(&build_count_new_tombstones_sql(table))
                .param("cursor", cursor)
                .fetch_arrow()
                .await
                .map_err(TaskError::new)?;
            let tombstones = batches
                .first()
                .and_then(|batch| ArrowUtils::get_column::<UInt64Type>(batch, "tombstones", 0))
                .unwrap_or(0);
            return Ok(if tombstones > 0 {
                vec![DeleteScope::WholeTable]
            } else {
                Vec::new()
            });
        }

        let batches = self
            .graph
            .query(&build_namespace_roots_with_new_tombstones_sql(table))
            .param("cursor", cursor)
            .fetch_arrow()
            .await
            .map_err(TaskError::new)?;
        Ok(String::extract_column(&batches, 0)
            .map_err(TaskError::new)?
            .into_iter()
            .map(DeleteScope::NamespaceRoot)
            .collect())
    }

    async fn delete_tombstoned_keys_in_scope(
        &self,
        table: &ReplacingMergeTreeTable,
        scope: &DeleteScope,
    ) -> Result<(), TaskError> {
        self.graph
            .query(&build_delete_tombstoned_keys_sql(
                table,
                scope,
                self.config.delete_timeout_secs,
            ))
            .param("root", scope.root().to_string())
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

fn build_count_new_tombstones_sql(table: &ReplacingMergeTreeTable) -> String {
    format!(
        "SELECT count() AS tombstones FROM {} WHERE _deleted AND _version > {{cursor:String}}",
        table.name
    )
}

fn build_namespace_roots_with_new_tombstones_sql(table: &ReplacingMergeTreeTable) -> String {
    format!(
        "SELECT DISTINCT arrayStringConcat(arraySlice(arrayFilter(part -> part != '', \
           splitByChar('/', traversal_path)), 1, 2), '/') || '/' AS root \
         FROM {} \
         WHERE _deleted AND _version > {{cursor:String}}",
        table.name
    )
}

fn build_delete_tombstoned_keys_sql(
    table: &ReplacingMergeTreeTable,
    scope: &DeleteScope,
    delete_timeout_secs: u64,
) -> String {
    let keys = table.sort_key_as_sql_list();
    let name = &table.name;
    let scope = scope.predicate();
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
    fn delete_considers_only_the_newest_version_of_each_key() {
        let tables = all_replacing_merge_tree_tables();
        let table = find_table_ending_in(&tables, "gl_edge");
        let sql =
            build_delete_tombstoned_keys_sql(table, &DeleteScope::NamespaceRoot("1/9/".into()), 60);

        assert!(sql.contains("LIMIT 1 BY"), "sql: {sql}");
        assert!(sql.contains("_version DESC"), "sql: {sql}");
        assert!(sql.contains("WHERE _deleted"), "sql: {sql}");
    }

    #[test]
    fn delete_waits_for_the_mutation_instead_of_polling_system_mutations() {
        let tables = all_replacing_merge_tree_tables();
        let sql = build_delete_tombstoned_keys_sql(
            find_table_ending_in(&tables, "gl_edge"),
            &DeleteScope::WholeTable,
            900,
        );

        assert!(sql.contains("lightweight_deletes_sync = 2"), "sql: {sql}");
        assert!(sql.contains("max_execution_time = 900"), "sql: {sql}");
        assert!(!sql.contains("system.mutations"), "sql: {sql}");
    }

    #[test]
    fn namespace_scoped_delete_prunes_both_the_outer_and_inner_scan() {
        let tables = all_replacing_merge_tree_tables();
        let sql = build_delete_tombstoned_keys_sql(
            find_table_ending_in(&tables, "gl_edge"),
            &DeleteScope::NamespaceRoot("1/9970/".into()),
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
        assert!(!user.sorts_by_traversal_path());

        let sql = build_delete_tombstoned_keys_sql(user, &DeleteScope::WholeTable, 60);
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
