use std::collections::HashMap;
use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use gkg_server_config::ClickHouseConfiguration;
use thiserror::Error;
use tokio::sync::{mpsc, watch};
use tracing::warn;

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

/// Coalesces durable writes across many callers into well-sized parts. Each `submit` carries a
/// monotonic `tag`; a table flushes when it reaches `max_rows` or after `max_age`. The
/// `flushed` watermark reports the highest tag whose rows are all durable — the largest tag
/// below every still-buffered tag, never crossing a tag whose flush failed. Callers must submit
/// a tag's batches contiguously (before the next tag) so the watermark cleanly partitions them.
#[derive(Clone)]
pub struct BufferedWriter {
    tx: mpsc::Sender<(String, RecordBatch, u64)>,
    flushed: watch::Receiver<u64>,
}

impl BufferedWriter {
    pub fn spawn(
        writer: Arc<ClickHouseWriter>,
        channel_capacity: usize,
        max_rows: usize,
        max_age: Duration,
    ) -> Self {
        let (tx, rx) = mpsc::channel(channel_capacity.max(1));
        let (flushed_tx, flushed) = watch::channel(0u64);
        tokio::spawn(drain(writer, rx, max_rows.max(1), max_age, flushed_tx));
        Self { tx, flushed }
    }

    /// Buffer one batch under `tag`. Uses `blocking_send`, for the blocking parse thread.
    pub fn submit(&self, table: String, batch: RecordBatch, tag: u64) -> Result<(), WriteError> {
        self.tx
            .blocking_send((table, batch, tag))
            .map_err(|_| WriteError::Write("buffered writer drain closed".into(), None))
    }

    /// Highest fully-durable tag. Watch it to learn when a tag's rows have landed.
    pub fn flushed(&self) -> watch::Receiver<u64> {
        self.flushed.clone()
    }
}

#[derive(Default)]
struct TableBuffer {
    batches: Vec<RecordBatch>,
    rows: usize,
    min_tag: u64,
}

async fn drain(
    writer: Arc<ClickHouseWriter>,
    mut rx: mpsc::Receiver<(String, RecordBatch, u64)>,
    max_rows: usize,
    max_age: Duration,
    flushed: watch::Sender<u64>,
) {
    let mut pending: HashMap<String, TableBuffer> = HashMap::new();
    let mut max_tag = 0u64;
    let mut failed_floor: Option<u64> = None;

    let mut ticker = tokio::time::interval(max_age);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        tokio::select! {
            msg = rx.recv() => {
                let Some((table, batch, tag)) = msg else { break };
                max_tag = max_tag.max(tag);
                let buf = pending.entry(table.clone()).or_insert(TableBuffer { min_tag: tag, ..Default::default() });
                buf.rows += batch.num_rows();
                buf.min_tag = buf.min_tag.min(tag);
                buf.batches.push(batch);
                if buf.rows < max_rows {
                    continue;
                }
                let buf = pending.remove(&table).unwrap();
                flush(&writer, &table, buf, &mut failed_floor).await;
            }
            _ = ticker.tick() => {
                for (table, buf) in std::mem::take(&mut pending) {
                    flush(&writer, &table, buf, &mut failed_floor).await;
                }
            }
        }
        publish(&pending, max_tag, failed_floor, &flushed);
    }

    for (table, buf) in std::mem::take(&mut pending) {
        flush(&writer, &table, buf, &mut failed_floor).await;
    }
    publish(&pending, max_tag, failed_floor, &flushed);
}

/// On failure, poison the part's tags so the watermark never reports them durable.
async fn flush(
    writer: &ClickHouseWriter,
    table: &str,
    buf: TableBuffer,
    failed_floor: &mut Option<u64>,
) {
    if let Err(e) = writer
        .write(table, buf.batches, Some(WriteDurability::Durable))
        .await
    {
        warn!(table, error = %e, "buffered write flush failed");
        *failed_floor = Some(failed_floor.map_or(buf.min_tag, |f| f.min(buf.min_tag)));
    }
}

fn publish(
    pending: &HashMap<String, TableBuffer>,
    max_tag: u64,
    failed_floor: Option<u64>,
    flushed: &watch::Sender<u64>,
) {
    let mut watermark = pending
        .values()
        .filter(|b| !b.batches.is_empty())
        .map(|b| b.min_tag)
        .min()
        .map_or(max_tag, |lowest| lowest.saturating_sub(1));
    if let Some(floor) = failed_floor {
        watermark = watermark.min(floor.saturating_sub(1));
    }
    flushed.send_if_modified(|cur| {
        let advance = watermark > *cur;
        if advance {
            *cur = watermark;
        }
        advance
    });
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

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![0i64; rows]))]).unwrap()
    }

    async fn submit(w: &BufferedWriter, table: &str, batch: RecordBatch, tag: u64) {
        let w = w.clone();
        let table = table.to_string();
        tokio::task::spawn_blocking(move || w.submit(table, batch, tag))
            .await
            .unwrap()
            .unwrap();
    }

    async fn await_tag(rx: &mut watch::Receiver<u64>, want: u64) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while *rx.borrow() < want {
                rx.changed().await.unwrap();
            }
        })
        .await
        .unwrap_or_else(|_| panic!("watermark stuck at {}", *rx.borrow()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watermark_advances_on_size_flush() {
        let w = BufferedWriter::spawn(
            Arc::new(ClickHouseWriter::noop()),
            16,
            50,
            Duration::from_secs(3600),
        );
        let mut flushed = w.flushed();
        submit(&w, "gl_edge", batch(60), 1).await;
        await_tag(&mut flushed, 1).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watermark_advances_on_age_flush() {
        let w = BufferedWriter::spawn(
            Arc::new(ClickHouseWriter::noop()),
            16,
            1_000_000,
            Duration::from_millis(40),
        );
        let mut flushed = w.flushed();
        submit(&w, "gl_edge", batch(5), 1).await;
        await_tag(&mut flushed, 1).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watermark_held_back_while_a_table_buffers_the_tag() {
        let w = BufferedWriter::spawn(
            Arc::new(ClickHouseWriter::noop()),
            16,
            100,
            Duration::from_secs(3600),
        );
        let flushed = w.flushed();
        // gl_edge flushes at the cap, gl_code_edge keeps tag 1 buffered, so tag 1 isn't durable.
        submit(&w, "gl_code_edge", batch(10), 1).await;
        submit(&w, "gl_edge", batch(100), 1).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(*flushed.borrow(), 0);
    }
}
