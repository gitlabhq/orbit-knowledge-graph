use anyhow::{Context, Result};
use arrow::record_batch::RecordBatch;
use clickhouse_client::ArrowClickHouseClient;

use crate::config::ClickHouseConfig;

pub struct ClickHouseWriter {
    client: ArrowClickHouseClient,
    max_batches_per_insert: usize,
}

impl ClickHouseWriter {
    pub fn new(config: &ClickHouseConfig, max_batches_per_insert: usize) -> Self {
        Self {
            client: config.build_client(),
            max_batches_per_insert,
        }
    }

    pub async fn truncate_table(&self, table: &str) -> Result<()> {
        self.client
            .execute(&format!("TRUNCATE TABLE IF EXISTS {table}"))
            .await
            .with_context(|| format!("failed to truncate {table}"))?;
        Ok(())
    }

    pub async fn insert_batches(&self, table: &str, batches: &[RecordBatch]) -> Result<usize> {
        let mut total_rows = 0;

        for chunk in batches.chunks(self.max_batches_per_insert) {
            self.client
                .insert_arrow(table, chunk)
                .await
                .with_context(|| format!("failed to insert into {table}"))?;
            total_rows += chunk.iter().map(|b| b.num_rows()).sum::<usize>();
        }

        Ok(total_rows)
    }
}
