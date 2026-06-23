use std::time::Instant;

use async_trait::async_trait;
use tracing::{info, warn};

use crate::clickhouse::ArrowClickHouseClient;
use crate::orchestrator::scheduled::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use gkg_server_config::{ScheduleConfiguration, TableCleanupConfig};

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
        let tables = table_names_from_ontology(ontology);
        Self {
            graph,
            tables,
            metrics,
            config,
        }
    }
}

#[async_trait]
impl ScheduledTask for TableCleanup {
    fn name(&self) -> &str {
        "maintenance.table_cleanup"
    }

    fn schedule(&self) -> &ScheduleConfiguration {
        &self.config.schedule
    }

    async fn run(&self) -> Result<(), TaskError> {
        let start = Instant::now();

        let result = self.cleanup_all_tables().await;

        let duration = start.elapsed().as_secs_f64();
        let outcome = if result.is_ok() { "success" } else { "error" };
        self.metrics.record_run(self.name(), outcome, duration);

        result
    }
}

impl TableCleanup {
    async fn cleanup_all_tables(&self) -> Result<(), TaskError> {
        let mut cleaned = 0u64;
        let mut failed = 0u64;
        let mut total_duration = 0.0f64;

        for table in &self.tables {
            let statement = format!("OPTIMIZE TABLE {table} FINAL CLEANUP");
            let table_start = Instant::now();

            let elapsed_secs = match self.graph.execute(&statement).await {
                Ok(()) => {
                    cleaned += 1;
                    let d = table_start.elapsed().as_secs_f64();
                    info!(table, duration_ms = (d * 1000.0) as u64, "cleaned up table");
                    d
                }
                Err(error) => {
                    failed += 1;
                    let d = table_start.elapsed().as_secs_f64();
                    self.metrics.record_error(self.name(), "cleanup");
                    warn!(table, duration_ms = (d * 1000.0) as u64, %error, "failed to clean up table");
                    d
                }
            };
            total_duration += elapsed_secs;
        }

        info!(
            cleaned,
            failed,
            duration_ms = (total_duration * 1000.0) as u64,
            "table cleanup complete"
        );

        if failed > 0 && cleaned == 0 {
            return Err(TaskError::new(format!(
                "all {failed} table cleanups failed"
            )));
        }

        Ok(())
    }
}

fn table_names_from_ontology(ontology: &ontology::Ontology) -> Vec<String> {
    let mut names = Vec::new();

    for aux in ontology.auxiliary_tables() {
        names.push(prefixed_table_name(&aux.name, *SCHEMA_VERSION));
    }
    for node in ontology.nodes() {
        names.push(prefixed_table_name(
            &node.destination_table,
            *SCHEMA_VERSION,
        ));
    }
    for edge_table in ontology.edge_tables() {
        names.push(prefixed_table_name(edge_table, *SCHEMA_VERSION));
    }

    names
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_names_cover_auxiliary_node_and_edge_tables() {
        let ontology = ontology::Ontology::load_embedded().expect("ontology must load");
        let names = table_names_from_ontology(&ontology);

        assert!(!names.is_empty());

        for name in &names {
            assert!(
                name.starts_with(&format!("v{}_", *SCHEMA_VERSION)),
                "table '{name}' should be prefixed with schema version"
            );
        }
    }
}
