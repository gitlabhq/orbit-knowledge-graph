use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use clickhouse_client::ArrowClickHouseClient;
use orbit_log_fields::{error, warn};
use thiserror::Error;

use crate::durability::WriteDurability;
use crate::metrics::EngineMetrics;
use crate::retry::{Backoff, LocalRetry, RetryExhausted, Step, drive};

#[derive(Debug, Error, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum WriteError {
    #[error("failed to write: {0}")]
    Write(String),

    #[error("graph store unavailable: {0}")]
    Unavailable(String),
}

impl From<RetryExhausted> for WriteError {
    fn from(e: RetryExhausted) -> Self {
        WriteError::Write(e.to_string())
    }
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
    pub fn new(client: ArrowClickHouseClient, metrics: Arc<EngineMetrics>) -> Self {
        Self {
            client,
            metrics,
            noop: false,
        }
    }

    /// A writer that accepts all writes without connecting. For unit tests only.
    #[cfg(any(test, feature = "testkit"))]
    pub fn noop() -> Self {
        use clickhouse_client::ClickHouseConfigurationExt;

        Self {
            client: orbit_server_config::AppConfig::embedded_defaults()
                .graph
                .build_client("graph"),
            metrics: Arc::new(EngineMetrics::new()),
            noop: true,
        }
    }
}

/// Both variants pin `async_insert` so the many small per-page inserts coalesce into fewer parts.
pub(crate) fn insert_overrides(
    durability: WriteDurability,
) -> &'static [(&'static str, &'static str)] {
    match durability {
        WriteDurability::Durable => &[
            ("async_insert", "1"),
            ("wait_for_async_insert", "1"),
            ("deduplicate_blocks_in_dependent_materialized_views", "1"),
        ],
        WriteDurability::FireAndForget => &[
            ("async_insert", "1"),
            ("wait_for_async_insert", "0"),
            ("deduplicate_blocks_in_dependent_materialized_views", "1"),
        ],
    }
}

/// Blanket-retries a transient ClickHouse insert (its error codes are not stable enough to classify).
const WRITE_RETRY: LocalRetry = LocalRetry {
    backoff: Backoff::Fixed(&[Duration::from_secs(2), Duration::from_secs(4)]),
    max_attempts: 3,
};

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
            .map(|batch| match orbit_arrow::logical_byte_size(batch) {
                Ok(n) => n,
                Err(e) => {
                    self.metrics.record_unmeterable_batch(table);
                    error!(
                        safe(TABLE) = table,
                        safe(ERROR_TYPE) = "unsupported_arrow_type",
                        expanded(ERROR_MESSAGE) = %e,
                        "batch has no logical-byte-size rule; counting 0 bytes and writing anyway"
                    );
                    0
                }
            })
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
            .insert_arrow_streaming_with_sql(table, &insert_sql, batches)
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

    pub async fn write_durable(
        &self,
        table: &str,
        batch: RecordBatch,
    ) -> Result<WriteReport, WriteError> {
        drive(&WRITE_RETRY, |attempt| {
            let fut = self.write(table, vec![batch.clone()], Some(WriteDurability::Durable));
            async move {
                match fut.await {
                    Ok(report) => Step::Done(report),
                    Err(e @ WriteError::Unavailable(_)) => Step::GiveUp(e),
                    Err(e) => {
                        warn!(
                            safe(TABLE) = table,
                            safe(ATTEMPT) = attempt + 1,
                            safe(ERROR_TYPE) = <&str>::from(&e),
                            expanded(ERROR_MESSAGE) = %e,
                            "durable write failed"
                        );
                        WRITE_RETRY.retry_or_give_up(attempt, (), e)
                    }
                }
            }
        })
        .await
    }

    /// Issues a `DELETE FROM` statement for rows that should be lightweight-deleted.
    pub async fn lightweight_delete(&self, sql: &str) -> Result<(), WriteError> {
        if self.noop {
            return Ok(());
        }
        self.client
            .query(sql)
            .execute()
            .await
            .map_err(WriteError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_pins_async_insert_and_wait() {
        assert_eq!(
            insert_overrides(WriteDurability::Durable),
            &[
                ("async_insert", "1"),
                ("wait_for_async_insert", "1"),
                ("deduplicate_blocks_in_dependent_materialized_views", "1"),
            ]
        );
    }

    #[test]
    fn durable_inserts_from_embedded_defaults() {
        use clickhouse_client::ClickHouseConfigurationExt;

        let mut graph = orbit_server_config::AppConfig::embedded_defaults().graph;
        let durable_sql = |graph: &orbit_server_config::ClickHouseConfiguration| {
            graph
                .build_client("graph")
                .build_insert_sql_with_overrides("t", insert_overrides(WriteDurability::Durable))
        };
        assert_eq!(
            durable_sql(&graph),
            "INSERT INTO t SETTINGS async_insert=1, async_insert_busy_timeout_max_ms=1000, \
             async_insert_busy_timeout_min_ms=1000, \
             deduplicate_blocks_in_dependent_materialized_views=1, optimize_on_insert=0, \
             wait_for_async_insert=1 FORMAT ArrowStream"
        );
        graph.replicated = true;
        assert_eq!(
            durable_sql(&graph),
            "INSERT INTO t SETTINGS async_insert_busy_timeout_max_ms=1000, \
             async_insert_busy_timeout_min_ms=1000, \
             deduplicate_blocks_in_dependent_materialized_views=1, optimize_on_insert=0 FORMAT ArrowStream"
        );
    }

    #[test]
    fn fire_and_forget_pins_async_without_waiting() {
        assert_eq!(
            insert_overrides(WriteDurability::FireAndForget),
            &[
                ("async_insert", "1"),
                ("wait_for_async_insert", "0"),
                ("deduplicate_blocks_in_dependent_materialized_views", "1"),
            ]
        );
    }
}
