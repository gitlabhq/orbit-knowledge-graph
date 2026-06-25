use std::sync::Arc;

use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use gkg_server_config::ClickHouseConfiguration;

use crate::destination::{DestinationError, TableWriter, Writable, WriteReport};
use crate::durability::WriteDurability;
use crate::metrics::EngineMetrics;

#[derive(Clone)]
pub struct ClickHouseWriter {
    client: ArrowClickHouseClient,
    metrics: Arc<EngineMetrics>,
}

impl ClickHouseWriter {
    pub fn new(
        configuration: ClickHouseConfiguration,
        metrics: Arc<EngineMetrics>,
    ) -> Result<Self, DestinationError> {
        configuration
            .validate()
            .map_err(|e| DestinationError::InvalidConfiguration(e.to_string()))?;
        let client = configuration.build_client();
        Ok(Self { client, metrics })
    }
}

fn insert_overrides(durability: WriteDurability) -> &'static [(&'static str, &'static str)] {
    match durability {
        WriteDurability::Durable => &[("async_insert", "1"), ("wait_for_async_insert", "1")],
        WriteDurability::FireAndForget => &[("async_insert", "1"), ("wait_for_async_insert", "0")],
    }
}

impl TableWriter for ClickHouseWriter {
    async fn write(&self, w: Writable) -> Result<WriteReport, DestinationError> {
        if w.batches.is_empty() {
            return Ok(WriteReport {
                table: w.table,
                rows: 0,
                bytes: 0,
            });
        }

        let insert_sql = match w.durability {
            Some(durability) => {
                self.client
                    .build_insert_sql_with_overrides(&w.table, insert_overrides(durability))
            }
            None => self.client.build_insert_sql(&w.table),
        };

        let start = std::time::Instant::now();
        let rows: u64 = w.batches.iter().map(|b| b.num_rows() as u64).sum();
        let bytes: u64 = w
            .batches
            .iter()
            .map(|b| b.get_array_memory_size() as u64)
            .sum();

        if let Err(error) = self
            .client
            .insert_arrow_streaming_with_sql(&w.table, &insert_sql, &w.batches)
            .await
        {
            self.metrics.record_write_error(&w.table);
            return Err(error.into());
        }

        self.metrics
            .record_write_success(&w.table, start.elapsed().as_secs_f64(), rows, bytes);

        Ok(WriteReport {
            table: w.table,
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
