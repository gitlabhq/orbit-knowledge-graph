use std::collections::HashMap;
use std::error::Error as StdError;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use gkg_server_config::ClickHouseConfiguration;
use thiserror::Error;
use tokio::sync::watch;
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

/// A channel of `(table, batch, seq)` items. `seq` is a per-project monotonic id so the
/// drain can compute a durability watermark across projects.
struct SeqReceiver {
    rx: tokio::sync::mpsc::Receiver<(String, RecordBatch, u64)>,
}

impl SeqReceiver {
    async fn recv(&mut self) -> Option<(String, RecordBatch, u64)> {
        self.rx.recv().await
    }
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

    /// Cross-project drain. Coalesces `(table, batch, seq)` items into one ~`max_rows` part
    /// per table, also flushing a table once `max_buffer_age` elapses. After each successful
    /// flush it advances a watermark to the highest seq with no row still buffered, so a
    /// handler whose project seq is at or below the watermark knows its rows are durable. A
    /// failed flush propagates without advancing the watermark, so contributors don't
    /// checkpoint.
    async fn drain_buffered(
        self: &Arc<Self>,
        mut rx: SeqReceiver,
        max_rows: usize,
        flushed_seq_tx: tokio::sync::watch::Sender<u64>,
        max_buffer_age: Duration,
    ) -> Result<(), WriteError> {
        let max_rows = max_rows.max(1);
        let mut pending: HashMap<String, (Vec<RecordBatch>, usize, u64)> = HashMap::new();
        let mut max_seq = 0u64;

        let mut ticker = tokio::time::interval(max_buffer_age);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;

        loop {
            tokio::select! {
                item = rx.recv() => {
                    let Some((table, batch, seq)) = item else { break };
                    max_seq = max_seq.max(seq);
                    let (batches, rows, min_seq) =
                        pending.entry(table.clone()).or_insert((Vec::new(), 0, seq));
                    *rows += batch.num_rows();
                    *min_seq = (*min_seq).min(seq);
                    batches.push(batch);
                    if *rows >= max_rows {
                        let (batches, ..) = pending.remove(&table).unwrap();
                        self.write(&table, batches, Some(WriteDurability::Durable)).await?;
                    } else {
                        continue;
                    }
                }
                _ = ticker.tick() => {
                    for (table, (batches, ..)) in std::mem::take(&mut pending) {
                        self.write(&table, batches, Some(WriteDurability::Durable)).await?;
                    }
                }
            }
            publish_watermark(&pending, max_seq, &flushed_seq_tx);
        }

        for (table, (batches, ..)) in std::mem::take(&mut pending) {
            self.write(&table, batches, Some(WriteDurability::Durable))
                .await?;
        }
        publish_watermark(&pending, max_seq, &flushed_seq_tx);
        Ok(())
    }
}

/// Highest seq with no row still buffered: one below the lowest pending seq, or the highest
/// accepted seq when the buffer is empty. Seqs are per-project and contiguous, so this is
/// exactly the set of fully-durable projects.
fn publish_watermark(
    pending: &HashMap<String, (Vec<RecordBatch>, usize, u64)>,
    max_seq: u64,
    flushed_seq_tx: &watch::Sender<u64>,
) {
    let watermark = pending
        .values()
        .map(|(_, _, min_seq)| *min_seq)
        .min()
        .map_or(max_seq, |lowest| lowest.saturating_sub(1));
    flushed_seq_tx.send_if_modified(|cur| {
        let advance = watermark > *cur;
        if advance {
            *cur = watermark;
        }
        advance
    });
}

/// Process-wide write coalescer shared by every code-indexing job.
///
/// Each project takes a monotonic seq from [`begin_project`], streams its batches in with
/// that seq, then waits on [`subscribe`] until the flush watermark reaches the seq before
/// checkpointing. Many small projects coalesce into one well-sized part per table; big
/// projects cross the row cap on their own and flush promptly. A failed flush never
/// advances the watermark, so contributing projects do not checkpoint.
pub struct CodeWriteAggregator {
    tx: tokio::sync::mpsc::Sender<(String, RecordBatch, u64)>,
    flushed_rx: watch::Receiver<u64>,
    next_seq: AtomicU64,
}

impl CodeWriteAggregator {
    pub fn start(
        writer: Arc<ClickHouseWriter>,
        channel_capacity: usize,
        max_rows: usize,
        max_buffer_age: Duration,
    ) -> Arc<Self> {
        let (tx, rx) = tokio::sync::mpsc::channel(channel_capacity.max(1));
        let (flushed_tx, flushed_rx) = watch::channel(0u64);
        tokio::spawn(async move {
            if let Err(e) = writer
                .drain_buffered(SeqReceiver { rx }, max_rows, flushed_tx, max_buffer_age)
                .await
            {
                warn!(error = %e, "code write aggregator drain ended with error");
            }
        });
        Arc::new(Self {
            tx,
            flushed_rx,
            next_seq: AtomicU64::new(1),
        })
    }

    /// Reserve a seq for one project. All of that project's batches must be submitted with
    /// this seq and contiguously, so the watermark cleanly separates durable from buffered.
    pub fn begin_project(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Buffer one batch from the blocking parse thread, backpressuring the parser when the
    /// shared channel is full.
    pub fn submit(&self, table: String, batch: RecordBatch, seq: u64) -> Result<(), WriteError> {
        self.tx
            .blocking_send((table, batch, seq))
            .map_err(|_| WriteError::Write("code write aggregator drain closed".into(), None))
    }

    /// Watch receiver reporting the highest fully-durable project seq.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.flushed_rx.clone()
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

    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let col = Int64Array::from(vec![0i64; rows]);
        RecordBatch::try_new(schema, vec![Arc::new(col)]).unwrap()
    }

    /// `submit` uses `blocking_send` (it runs on the parse `spawn_blocking` thread in prod),
    /// so tests must submit off the async worker thread too.
    async fn submit(agg: &Arc<CodeWriteAggregator>, table: &str, batch: RecordBatch, seq: u64) {
        let agg = agg.clone();
        let table = table.to_string();
        tokio::task::spawn_blocking(move || agg.submit(table, batch, seq))
            .await
            .unwrap()
            .unwrap();
    }

    async fn wait_for_watermark(rx: &mut watch::Receiver<u64>, want: u64) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while *rx.borrow() < want {
                rx.changed().await.unwrap();
            }
        })
        .await
        .unwrap_or_else(|_| panic!("watermark never reached {want}, stuck at {}", *rx.borrow()));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watermark_advances_to_highest_seq_when_buffer_fully_flushed() {
        let writer = Arc::new(ClickHouseWriter::noop());
        let agg = CodeWriteAggregator::start(writer, 16, 1_000_000, Duration::from_millis(30));
        let mut wm = agg.subscribe();

        for _ in 0..3 {
            let seq = agg.begin_project();
            submit(&agg, "gl_edge", batch(10), seq).await;
        }

        wait_for_watermark(&mut wm, 3).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn watermark_holds_back_seq_whose_other_table_is_still_buffered() {
        let writer = Arc::new(ClickHouseWriter::noop());
        let agg = CodeWriteAggregator::start(writer, 16, 100, Duration::from_secs(3600));
        let wm = agg.subscribe();

        let seq = agg.begin_project();
        submit(&agg, "gl_code_edge", batch(10), seq).await;
        submit(&agg, "gl_edge", batch(100), seq).await;
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            *wm.borrow(),
            0,
            "seq must not be durable while its gl_code_edge rows are still buffered",
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn age_flush_fires_without_reaching_row_cap() {
        let writer = Arc::new(ClickHouseWriter::noop());
        let agg = CodeWriteAggregator::start(writer, 16, 1_000_000, Duration::from_millis(50));
        let mut wm = agg.subscribe();

        let seq = agg.begin_project();
        submit(&agg, "gl_edge", batch(5), seq).await;

        wait_for_watermark(&mut wm, seq).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn size_flush_coalesces_then_advances_watermark() {
        let writer = Arc::new(ClickHouseWriter::noop());
        let agg = CodeWriteAggregator::start(writer, 16, 50, Duration::from_secs(3600));
        let mut wm = agg.subscribe();

        let seq = agg.begin_project();
        submit(&agg, "gl_edge", batch(60), seq).await;

        wait_for_watermark(&mut wm, seq).await;
    }
}
