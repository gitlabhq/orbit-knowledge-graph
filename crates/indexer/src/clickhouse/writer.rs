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

/// Per-table accumulation buffer for the cross-project drain. Tracks the seq range of
/// buffered rows so the flush watermark can tell which projects are fully durable.
#[derive(Default)]
struct TableBuffer {
    batches: Vec<RecordBatch>,
    rows: usize,
    min_seq: Option<u64>,
    max_seq: u64,
}

impl TableBuffer {
    fn min_pending_seq(&self) -> u64 {
        self.min_seq.unwrap_or(self.max_seq)
    }
}

/// A channel of `(table, batch, seq)` items. `seq` is a per-project monotonic id assigned
/// by the producer so the drain can compute a durability watermark across projects.
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

    /// Cross-project drain. Coalesces `(table, batch, seq)` items from many projects into
    /// one ~`max_rows` part per table. A table also flushes when `max_buffer_age` elapses
    /// with rows pending. After each successful flush, the flush watermark advances to the
    /// largest `seq` for which no table still buffers any row at or below it; a handler
    /// whose project seq is at or below the watermark knows its rows are durable. A failed
    /// flush returns immediately without advancing the watermark, so the contributing
    /// projects never checkpoint.
    async fn drain_buffered(
        self: &Arc<Self>,
        mut rx: SeqReceiver,
        max_rows: usize,
        max_concurrent: usize,
        max_buffer_age: Option<std::time::Duration>,
        flushed_seq_tx: tokio::sync::watch::Sender<u64>,
    ) -> Result<Vec<WriteReport>, WriteError> {
        let max_rows = max_rows.max(1);
        let sem = Arc::new(tokio::sync::Semaphore::new(max_concurrent.max(1)));
        let mut pending: std::collections::HashMap<String, TableBuffer> =
            std::collections::HashMap::new();
        let mut reports = Vec::new();
        let mut max_accepted_seq = 0u64;

        let age = max_buffer_age.unwrap_or(std::time::Duration::from_secs(u64::MAX / 2));
        let mut ticker = tokio::time::interval(age);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        ticker.tick().await;

        loop {
            let item = tokio::select! {
                item = rx.recv() => item,
                _ = ticker.tick() => {
                    self.flush_all(&mut pending, &sem, &mut reports).await?;
                    self.publish_watermark(&pending, max_accepted_seq, &flushed_seq_tx);
                    continue;
                }
            };

            let Some((table, batch, seq)) = item else {
                break;
            };
            max_accepted_seq = max_accepted_seq.max(seq);
            let entry = pending.entry(table.clone()).or_default();
            entry.rows += batch.num_rows();
            entry.min_seq = Some(entry.min_seq.map_or(seq, |m| m.min(seq)));
            entry.max_seq = entry.max_seq.max(seq);
            entry.batches.push(batch);
            if entry.rows >= max_rows {
                let buf = pending.remove(&table).unwrap();
                self.flush_one(&table, buf.batches, &sem, &mut reports)
                    .await?;
                self.publish_watermark(&pending, max_accepted_seq, &flushed_seq_tx);
            }
        }

        self.flush_all(&mut pending, &sem, &mut reports).await?;
        self.publish_watermark(&pending, max_accepted_seq, &flushed_seq_tx);
        Ok(reports)
    }

    async fn flush_one(
        self: &Arc<Self>,
        table: &str,
        batches: Vec<RecordBatch>,
        sem: &Arc<tokio::sync::Semaphore>,
        reports: &mut Vec<WriteReport>,
    ) -> Result<(), WriteError> {
        let _permit = sem.acquire().await.expect("write semaphore closed");
        let report = self
            .write(table, batches, Some(WriteDurability::Durable))
            .await?;
        reports.push(report);
        Ok(())
    }

    async fn flush_all(
        self: &Arc<Self>,
        pending: &mut std::collections::HashMap<String, TableBuffer>,
        sem: &Arc<tokio::sync::Semaphore>,
        reports: &mut Vec<WriteReport>,
    ) -> Result<(), WriteError> {
        let mut set = tokio::task::JoinSet::new();
        for (table, buf) in pending.drain() {
            if buf.batches.is_empty() {
                continue;
            }
            let (w, p) = (self.clone(), sem.clone());
            set.spawn(async move {
                let _permit = p.acquire_owned().await.expect("write semaphore closed");
                w.write(&table, buf.batches, Some(WriteDurability::Durable))
                    .await
            });
        }
        while let Some(r) = set.join_next().await {
            reports.push(r.map_err(|e| WriteError::Write(format!("join: {e}"), None))??);
        }
        Ok(())
    }

    /// The watermark is the largest seq such that no still-buffered table holds a row at
    /// or below it. Because seqs are assigned per project and a project's batches are sent
    /// contiguously, this is the highest seq whose rows are fully durable: it is one below
    /// the minimum `max_seq` still buffered, or the highest accepted seq when nothing is
    /// pending.
    fn publish_watermark(
        &self,
        pending: &std::collections::HashMap<String, TableBuffer>,
        max_accepted_seq: u64,
        flushed_seq_tx: &tokio::sync::watch::Sender<u64>,
    ) {
        let watermark = pending
            .values()
            .filter(|b| !b.batches.is_empty())
            .map(|b| b.min_pending_seq())
            .min()
            .map(|min_pending| min_pending.saturating_sub(1))
            .unwrap_or(max_accepted_seq);
        flushed_seq_tx.send_if_modified(|cur| {
            if watermark > *cur {
                *cur = watermark;
                true
            } else {
                false
            }
        });
    }
}

/// Process-wide write coalescer shared by every code-indexing job.
///
/// Small repositories submit their edges into one shared per-table buffer
/// ([`submit_buffered`]) so the long tail of tiny projects flushes as a few
/// well-sized parts instead of one part each. Big repositories bypass the buffer
/// ([`write_solo`]) and write their own parts immediately, so their completion is
/// not gated behind small-repo flushes and they cannot bloat buffer latency.
///
/// Buffered durability is reported through a watch watermark: each project takes a
/// monotonic seq from [`begin_project`], tags all its batches with it, and waits
/// (via [`subscribe`]) until the watermark reaches that seq before checkpointing.
pub struct CodeWriteAggregator {
    writer: Arc<ClickHouseWriter>,
    tx: tokio::sync::mpsc::Sender<(String, RecordBatch, u64)>,
    flushed_rx: watch::Receiver<u64>,
    next_seq: AtomicU64,
    drain: tokio::task::JoinHandle<Result<Vec<WriteReport>, WriteError>>,
}

impl CodeWriteAggregator {
    pub fn start(
        writer: Arc<ClickHouseWriter>,
        channel_capacity: usize,
        max_rows: usize,
        max_concurrent: usize,
        max_buffer_age: Duration,
    ) -> Arc<Self> {
        let (tx, rx) = tokio::sync::mpsc::channel(channel_capacity.max(1));
        let (flushed_tx, flushed_rx) = watch::channel(0u64);
        let drain_writer = writer.clone();
        let drain = tokio::spawn(async move {
            drain_writer
                .drain_buffered(
                    SeqReceiver { rx },
                    max_rows,
                    max_concurrent,
                    Some(max_buffer_age),
                    flushed_tx,
                )
                .await
        });
        Arc::new(Self {
            writer,
            tx,
            flushed_rx,
            next_seq: AtomicU64::new(1),
            drain,
        })
    }

    /// Reserve a seq for one project. All of that project's batches must be submitted
    /// with this seq, and contiguously (before any later project's first batch), so the
    /// watermark cleanly separates durable projects from buffered ones.
    pub fn begin_project(&self) -> u64 {
        self.next_seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Buffer one batch for a small project. Backpressures when the shared channel is full.
    pub async fn submit_buffered(
        &self,
        table: String,
        batch: RecordBatch,
        seq: u64,
    ) -> Result<(), WriteError> {
        self.tx
            .send((table, batch, seq))
            .await
            .map_err(|_| WriteError::Write("code write aggregator drain closed".into(), None))
    }

    /// `submit_buffered` for use from the blocking parse thread. Backpressures the parser
    /// when the shared channel is full, exactly like the async variant.
    pub fn blocking_submit_buffered(
        &self,
        table: String,
        batch: RecordBatch,
        seq: u64,
    ) -> Result<(), WriteError> {
        self.tx
            .blocking_send((table, batch, seq))
            .map_err(|_| WriteError::Write("code write aggregator drain closed".into(), None))
    }

    /// Watch receiver that reports the highest fully-durable project seq.
    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.flushed_rx.clone()
    }

    /// Write a big repository's batches directly as their own parts, bypassing the buffer.
    /// Batches are coalesced per table so each table flushes one part. Returns once every
    /// table is durable.
    pub async fn write_solo(
        &self,
        batches: Vec<(String, RecordBatch)>,
    ) -> Result<Vec<WriteReport>, WriteError> {
        let mut by_table: std::collections::HashMap<String, Vec<RecordBatch>> =
            std::collections::HashMap::new();
        for (table, batch) in batches {
            by_table.entry(table).or_default().push(batch);
        }
        let mut reports = Vec::with_capacity(by_table.len());
        for (table, batches) in by_table {
            reports.push(
                self.writer
                    .write(&table, batches, Some(WriteDurability::Durable))
                    .await?,
            );
        }
        Ok(reports)
    }

    /// Close the channel and join the drain on shutdown, surfacing any final flush error.
    pub async fn shutdown(self: Arc<Self>) {
        let Ok(this) = Arc::try_unwrap(self) else {
            warn!("code write aggregator still has live handles at shutdown; skipping join");
            return;
        };
        drop(this.tx);
        match this.drain.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => warn!(error = %e, "code write aggregator drain ended with error"),
            Err(e) => warn!(error = %e, "code write aggregator drain task panicked"),
        }
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

    async fn wait_for_watermark(rx: &mut watch::Receiver<u64>, want: u64) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while *rx.borrow() < want {
                rx.changed().await.unwrap();
            }
        })
        .await
        .unwrap_or_else(|_| panic!("watermark never reached {want}, stuck at {}", *rx.borrow()));
    }

    #[tokio::test]
    async fn watermark_advances_to_highest_seq_when_buffer_fully_flushed() {
        let writer = Arc::new(ClickHouseWriter::noop());
        let agg = CodeWriteAggregator::start(writer, 16, 1_000_000, 4, Duration::from_millis(30));
        let mut wm = agg.subscribe();

        for _ in 0..3 {
            let seq = agg.begin_project();
            agg.submit_buffered("gl_edge".into(), batch(10), seq)
                .await
                .unwrap();
        }

        wait_for_watermark(&mut wm, 3).await;
        agg.shutdown().await;
    }

    #[tokio::test]
    async fn watermark_holds_back_seq_whose_other_table_is_still_buffered() {
        let writer = Arc::new(ClickHouseWriter::noop());
        let agg = CodeWriteAggregator::start(writer, 16, 100, 4, Duration::from_secs(3600));
        let wm = agg.subscribe();

        let seq = agg.begin_project();
        agg.submit_buffered("gl_code_edge".into(), batch(10), seq)
            .await
            .unwrap();
        agg.submit_buffered("gl_edge".into(), batch(100), seq)
            .await
            .unwrap();
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            *wm.borrow(),
            0,
            "seq must not be durable while its gl_code_edge rows are still buffered",
        );

        agg.shutdown().await;
    }

    #[tokio::test]
    async fn age_flush_fires_without_reaching_row_cap() {
        let writer = Arc::new(ClickHouseWriter::noop());
        let agg = CodeWriteAggregator::start(writer, 16, 1_000_000, 4, Duration::from_millis(50));
        let mut wm = agg.subscribe();

        let seq = agg.begin_project();
        agg.submit_buffered("gl_edge".into(), batch(5), seq)
            .await
            .unwrap();

        wait_for_watermark(&mut wm, seq).await;
        agg.shutdown().await;
    }

    #[tokio::test]
    async fn solo_write_coalesces_per_table() {
        let writer = Arc::new(ClickHouseWriter::noop());
        let agg = CodeWriteAggregator::start(writer, 16, 1_000_000, 4, Duration::from_secs(3600));

        let reports = agg
            .write_solo(vec![
                ("gl_edge".into(), batch(10)),
                ("gl_edge".into(), batch(20)),
                ("gl_code_edge".into(), batch(5)),
            ])
            .await
            .unwrap();

        assert_eq!(reports.len(), 2, "one part per distinct table");
        agg.shutdown().await;
    }
}
