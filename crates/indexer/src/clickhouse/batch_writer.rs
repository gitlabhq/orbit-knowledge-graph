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
use clickhouse_client::{ArrowClickHouseClient, ArrowStreamInsert};

use crate::destination::{BatchWriter, DestinationError, StreamingWriter};
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
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        let insert_sql = client.build_insert_sql(&table);
        Self {
            client,
            table,
            insert_sql,
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

/// Streams batches into a single open `INSERT ... FORMAT ArrowStream`. Rows,
/// bytes, and elapsed time are accumulated across writes and reported as one
/// write on `finish`, mirroring `ClickHouseBatchWriter`'s metrics.
pub(crate) struct ClickHouseStreamingWriter {
    insert: ArrowStreamInsert,
    table: String,
    metrics: Arc<EngineMetrics>,
    rows: u64,
    bytes: u64,
    write_seconds: f64,
    wrote_any: bool,
}

impl ClickHouseStreamingWriter {
    pub(crate) fn new(
        client: &ArrowClickHouseClient,
        table: String,
        metrics: Arc<EngineMetrics>,
    ) -> Self {
        let insert = client.open_arrow_stream(&table);
        Self {
            insert,
            table,
            metrics,
            rows: 0,
            bytes: 0,
            write_seconds: 0.0,
            wrote_any: false,
        }
    }
}

#[async_trait]
impl StreamingWriter for ClickHouseStreamingWriter {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DestinationError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let start = std::time::Instant::now();
        if let Err(error) = self.insert.write_batch(batch).await {
            self.metrics.record_write_error(&self.table);
            return Err(error.into());
        }
        self.write_seconds += start.elapsed().as_secs_f64();
        self.rows += batch.num_rows() as u64;
        self.bytes += batch.get_array_memory_size() as u64;
        self.wrote_any = true;
        Ok(())
    }

    async fn finish(mut self: Box<Self>) -> Result<(), DestinationError> {
        let start = std::time::Instant::now();
        if let Err(error) = self.insert.finish().await {
            self.metrics.record_write_error(&self.table);
            return Err(error.into());
        }
        self.write_seconds += start.elapsed().as_secs_f64();

        if self.wrote_any {
            self.metrics.record_write_success(
                &self.table,
                self.write_seconds,
                self.rows,
                self.bytes,
            );
        }
        Ok(())
    }
}
