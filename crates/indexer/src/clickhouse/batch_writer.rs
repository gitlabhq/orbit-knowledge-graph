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
use opentelemetry::KeyValue;

use crate::destination::{BatchWriter, DestinationError};
use crate::metrics::EngineMetrics;

pub(crate) struct ClickHouseBatchWriter {
    client: ArrowClickHouseClient,
    table: String,
    metrics: Arc<EngineMetrics>,
}

impl ClickHouseBatchWriter {
    pub(crate) fn new(
        client: ArrowClickHouseClient,
        table: String,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        Self {
            client,
            table,
            metrics,
        }
    }
}

#[async_trait]
impl BatchWriter for ClickHouseBatchWriter {
    async fn write_batch(&self, batches: &[RecordBatch]) -> Result<(), DestinationError> {
        if batches.is_empty() {
            return Ok(());
        }

        let start = std::time::Instant::now();
        self.client.insert_arrow(&self.table, batches).await?;
        let elapsed = start.elapsed().as_secs_f64();

        let table_label = KeyValue::new("table", self.table.clone());
        self.metrics
            .destination_write_duration
            .record(elapsed, std::slice::from_ref(&table_label));

        let total_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        self.metrics
            .destination_rows_written
            .add(total_rows, std::slice::from_ref(&table_label));

        let total_bytes: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();
        self.metrics
            .destination_bytes_written
            .add(total_bytes, std::slice::from_ref(&table_label));

        Ok(())
    }
}
