use std::time::Instant;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::clickhouse::ArrowClickHouseClient;
use crate::configuration::ScheduleConfiguration;
use crate::scheduler::{ScheduledTask, ScheduledTaskMetrics, TaskError};

fn default_interval_secs() -> Option<u64> {
    Some(86400) // 1 day in seconds
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableCleanupConfig {
    #[serde(flatten)]
    pub schedule: ScheduleConfiguration,
}

impl Default for TableCleanupConfig {
    fn default() -> Self {
        Self {
            schedule: ScheduleConfiguration {
                interval_secs: default_interval_secs(),
            },
        }
    }
}

pub struct TableCleanup {
    graph: ArrowClickHouseClient,
    metrics: ScheduledTaskMetrics,
    config: TableCleanupConfig,
    tables: Vec<String>,
}

impl TableCleanup {
    pub fn new(
        graph: ArrowClickHouseClient,
        metrics: ScheduledTaskMetrics,
        config: TableCleanupConfig,
    ) -> Self {
        let ontology =
            ontology::Ontology::load_embedded().expect("embedded ontology must be valid");

        let mut tables: Vec<String> = ontology
            .nodes()
            .map(|node| node.destination_table.clone())
            .collect();
        tables.push(ontology.edge_table().to_owned());

        Self {
            graph,
            metrics,
            config,
            tables,
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

        for table in &self.tables {
            let statement = format!("OPTIMIZE TABLE {table} FINAL CLEANUP");

            match self.graph.execute(&statement).await {
                Ok(()) => {
                    cleaned += 1;
                    info!(table, "cleaned up table");
                }
                Err(error) => {
                    failed += 1;
                    self.metrics.record_error(self.name(), "cleanup");
                    warn!(table, %error, "failed to clean up table");
                }
            }
        }

        info!(cleaned, failed, "table cleanup complete");

        if failed > 0 && cleaned == 0 {
            return Err(TaskError::new(format!(
                "all {failed} table cleanups failed"
            )));
        }

        Ok(())
    }
}
