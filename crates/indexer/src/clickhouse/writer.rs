use std::error::Error as StdError;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use gkg_server_config::ClickHouseConfiguration;
use thiserror::Error;

use crate::durability::WriteDurability;
use crate::metrics::EngineMetrics;

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("failed to write: {0}")]
    Write(String, #[source] Option<Box<dyn StdError + Send + Sync>>),

    #[error("connection error: {0}")]
    Connection(String, #[source] Option<Box<dyn StdError + Send + Sync>>),

    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
}

#[derive(Debug, Clone)]
pub struct WriteReport {
    pub table: String,
    pub rows: u64,
    pub bytes: u64,
}

/// No internal throttling. Backpressure comes from the message source (NATS ack window,
/// channel capacity). Handlers control batch sizes per entity; self-managed deployments
/// with limited memory should watch ClickHouse's query queue.
#[derive(Clone)]
pub struct ClickHouseWriter {
    client: ArrowClickHouseClient,
    metrics: Arc<EngineMetrics>,
    noop: bool,
}

impl ClickHouseWriter {
    pub fn new(
        configuration: ClickHouseConfiguration,
        metrics: Arc<EngineMetrics>,
    ) -> Result<Self, WriteError> {
        configuration
            .validate()
            .map_err(|e| WriteError::InvalidConfiguration(e.to_string()))?;
        let client = configuration.build_client();
        Ok(Self {
            client,
            metrics,
            noop: false,
        })
    }

    /// A writer that accepts all writes without connecting. For unit tests only.
    #[cfg(any(test, feature = "testkit"))]
    pub fn noop() -> Self {
        Self {
            client: ClickHouseConfiguration::default().build_client(),
            metrics: Arc::new(EngineMetrics::new()),
            noop: true,
        }
    }
}

/// Both variants pin `async_insert` so the many small per-page inserts coalesce into fewer parts.
fn insert_overrides(durability: WriteDurability) -> &'static [(&'static str, &'static str)] {
    match durability {
        WriteDurability::Durable => &[("async_insert", "1"), ("wait_for_async_insert", "1")],
        WriteDurability::FireAndForget => &[("async_insert", "1"), ("wait_for_async_insert", "0")],
    }
}

impl ClickHouseWriter {
    pub async fn write(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
        durability: Option<WriteDurability>,
    ) -> Result<WriteReport, WriteError> {
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let bytes: u64 = batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();

        if batches.is_empty() || self.noop {
            return Ok(WriteReport {
                table: table.to_string(),
                rows,
                bytes,
            });
        }

        let insert_sql = match durability {
            Some(d) => self
                .client
                .build_insert_sql_with_overrides(table, insert_overrides(d)),
            None => self.client.build_insert_sql(table),
        };

        let start = std::time::Instant::now();

        if let Err(error) = self
            .client
            .insert_arrow_streaming_with_sql(table, &insert_sql, &batches)
            .await
        {
            self.metrics.record_write_error(table);
            return Err(error.into());
        }

        self.metrics
            .record_write_success(table, start.elapsed().as_secs_f64(), rows, bytes);

        Ok(WriteReport {
            table: table.to_string(),
            rows,
            bytes,
        })
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
    fn fire_and_forget_pins_async_without_waiting() {
        assert_eq!(
            insert_overrides(WriteDurability::FireAndForget),
            &[("async_insert", "1"), ("wait_for_async_insert", "0")]
        );
    }
}
