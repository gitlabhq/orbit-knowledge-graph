use std::time::Instant;

use async_trait::async_trait;
use futures::StreamExt;
use tracing::{info, warn};

use crate::clickhouse::ArrowClickHouseClient;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use orbit_server_config::{ScheduleConfiguration, TableCleanupConfig};

const TASK_NAME: &str = "maintenance.table_cleanup";

const CONCURRENT_MASKS: usize = 4;
const STATEMENT_TIMEOUT_SECS: u64 = 7200;

pub struct TableCleanup {
    graph: ArrowClickHouseClient,
    tables: Vec<String>,
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
            tables: list_graph_tables(ontology),
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
        let result = self.apply_deleted_mask_to_all_tables().await;
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics
            .record_run(self.name(), outcome, started.elapsed().as_secs_f64());
        result
    }
}

impl TableCleanup {
    async fn apply_deleted_mask_to_all_tables(&self) -> Result<(), TaskError> {
        let masks = self
            .tables
            .iter()
            .cloned()
            .map(|table| self.apply_deleted_mask(table));
        let failed = futures::stream::iter(masks)
            .buffer_unordered(CONCURRENT_MASKS)
            .filter(|succeeded| futures::future::ready(!succeeded))
            .count()
            .await;

        let tables = self.tables.len();
        info!(tables, failed, "apply deleted mask complete");

        if failed > 0 {
            return Err(TaskError::new(format!(
                "{failed}/{tables} tables failed to apply deleted mask"
            )));
        }
        Ok(())
    }

    async fn apply_deleted_mask(&self, table: String) -> bool {
        let started = Instant::now();
        let sql = build_apply_deleted_mask_sql(&table);
        match self
            .graph
            .query(&sql)
            .execute()
            .await
            .map_err(TaskError::new)
        {
            Ok(()) => {
                let elapsed = started.elapsed().as_secs_f64();
                self.metrics.record_query_duration(&table, elapsed);
                info!(
                    table = table.as_str(),
                    duration_ms = (elapsed * 1000.0) as u64,
                    "applied deleted mask"
                );
                true
            }
            Err(error) => {
                self.metrics.record_error(TASK_NAME, "apply_deleted_mask");
                warn!(table = table.as_str(), %error, "failed to apply deleted mask");
                false
            }
        }
    }
}

fn list_graph_tables(ontology: &ontology::Ontology) -> Vec<String> {
    ontology
        .nodes()
        .map(|node| node.destination_table.as_str())
        .chain(ontology.edge_tables())
        .map(|table| prefixed_table_name(table, *SCHEMA_VERSION))
        .collect()
}

fn build_apply_deleted_mask_sql(table: &str) -> String {
    format!(
        "ALTER TABLE {table} APPLY DELETED MASK \
         SETTINGS mutations_sync = 0, max_execution_time = {STATEMENT_TIMEOUT_SECS}"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_tables() -> Vec<String> {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        list_graph_tables(&ontology)
    }

    #[test]
    fn covers_all_node_and_edge_tables() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let tables = all_tables();

        for node in ontology.nodes() {
            let prefixed = prefixed_table_name(&node.destination_table, *SCHEMA_VERSION);
            assert!(
                tables.contains(&prefixed),
                "missing node table {prefixed}: {tables:?}"
            );
        }

        for edge_table in ontology.edge_tables() {
            let prefixed = prefixed_table_name(edge_table, *SCHEMA_VERSION);
            assert!(
                tables.contains(&prefixed),
                "missing edge table {prefixed}: {tables:?}"
            );
        }
    }

    #[test]
    fn auxiliary_tables_are_not_included() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let tables = all_tables();
        for aux in ontology.auxiliary_tables() {
            assert!(
                !tables.iter().any(|t| t.ends_with(&aux.name)),
                "auxiliary table '{}' must not be swept",
                aux.name
            );
        }
    }

    #[test]
    fn apply_deleted_mask_sql_uses_alter_table() {
        let sql = build_apply_deleted_mask_sql("v99_gl_user");
        assert!(
            sql.starts_with("ALTER TABLE v99_gl_user APPLY DELETED MASK"),
            "sql: {sql}"
        );
    }

    #[test]
    fn apply_deleted_mask_sql_is_async() {
        let sql = build_apply_deleted_mask_sql("v99_gl_edge");
        assert!(
            sql.contains("mutations_sync = 0"),
            "APPLY DELETED MASK is a heavyweight mutation; run it asynchronously \
             to avoid holding the HTTP connection; sql: {sql}"
        );
    }

    #[test]
    fn apply_deleted_mask_sql_has_a_timeout() {
        let sql = build_apply_deleted_mask_sql("v99_gl_edge");
        assert!(sql.contains("max_execution_time"), "sql: {sql}");
    }
}
