use std::time::Instant;

use async_trait::async_trait;
use clickhouse_client::FromArrowColumn;
use tracing::{info, warn};

use crate::clickhouse::ArrowClickHouseClient;
use crate::scheduler::{ScheduledTask, ScheduledTaskMetrics, TaskError};
use gkg_server_config::{ScheduleConfiguration, TableCleanupConfig};

pub struct TableCleanup {
    graph: ArrowClickHouseClient,
    metrics: ScheduledTaskMetrics,
    config: TableCleanupConfig,
}

impl TableCleanup {
    pub fn new(
        graph: ArrowClickHouseClient,
        metrics: ScheduledTaskMetrics,
        config: TableCleanupConfig,
    ) -> Self {
        Self {
            graph,
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
    async fn discover_tables(&self) -> Result<Vec<String>, TaskError> {
        let batches = self
            .graph
            .query_arrow(
                "SELECT table_name FROM information_schema.tables \
                 WHERE table_catalog = currentDatabase() \
                   AND table_type = 'BASE TABLE'",
            )
            .await
            .map_err(|error| TaskError::new(format!("failed to discover tables: {error}")))?;

        String::extract_column(&batches, 0)
            .map_err(|error| TaskError::new(format!("failed to extract table names: {error}")))
    }

    async fn cleanup_all_tables(&self) -> Result<(), TaskError> {
        let tables = self.discover_tables().await?;
        let mut cleaned = 0u64;
        let mut failed = 0u64;

        for table in &tables {
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
