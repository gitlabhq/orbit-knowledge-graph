//! Batch writer for ClickHouse.
//!
//! # Backpressure
//!
//! This writer has no internal throttling. It writes as fast as the upstream reader feeds it
//! batches, so backpressure comes from the message source, not the writer itself.
//!
//! Handlers control batch sizes per entity. If an entity has large rows or writes frequently,
//! reduce its batch size. You can also limit concurrent workers in `EngineConfiguration`.
//!
//! Self-managed deployments with limited memory should watch ClickHouse's query queue. If
//! writes start backing up, shrink batch sizes or reduce worker concurrency. We don't yet
//! throttle based on batch byte size, but that would help here.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use clickhouse_client::ArrowClickHouseClient;

use crate::destination::{BatchWriter, DestinationError};
use crate::durability::WriteDurability;
use crate::metrics::EngineMetrics;

pub(crate) struct ClickHouseBatchWriter {
    client: ArrowClickHouseClient,
    table: String,
    insert_sql: String,
    metrics: Arc<EngineMetrics>,
}

impl ClickHouseBatchWriter {
    pub(crate) fn new(
        client: ArrowClickHouseClient,
        table: String,
        durability: WriteDurability,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        let insert_sql =
            client.build_insert_sql_with_overrides(&table, insert_overrides(durability));
        Self {
            client,
            table,
            insert_sql,
            metrics,
        }
    }
}

/// Empty for `FireAndForget` so the deployment's `insert_settings` apply unchanged.
fn insert_overrides(durability: WriteDurability) -> &'static [(&'static str, &'static str)] {
    match durability {
        WriteDurability::Durable => &[("async_insert", "1"), ("wait_for_async_insert", "1")],
        WriteDurability::FireAndForget => &[],
    }
}

#[async_trait]
impl BatchWriter for ClickHouseBatchWriter {
    async fn write_batch(&self, batches: &[RecordBatch]) -> Result<(), DestinationError> {
        if batches.is_empty() {
            return Ok(());
        }

        let start = std::time::Instant::now();

        if let Err(error) = self
            .client
            .insert_arrow_streaming_with_sql(&self.table, &self.insert_sql, batches)
            .await
        {
            self.metrics.record_write_error(&self.table);
            return Err(error.into());
        }

        let elapsed = start.elapsed().as_secs_f64();
        let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let total_bytes: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();
        self.metrics
            .record_write_success(&self.table, elapsed, total_rows, total_bytes);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_pins_async_insert_and_wait() {
        assert_eq!(
            insert_overrides(WriteDurability::Durable),
            &[("async_insert", "1"), ("wait_for_async_insert", "1")]
        );
    }

    #[test]
    fn fire_and_forget_defers_to_config() {
        assert!(insert_overrides(WriteDurability::FireAndForget).is_empty());
    }
}
