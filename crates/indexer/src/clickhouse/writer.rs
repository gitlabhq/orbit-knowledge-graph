use std::collections::HashMap;
use std::error::Error as StdError;
use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use clickhouse_client::{ArrowClickHouseClient, ClickHouseConfigurationExt};
use gkg_server_config::ClickHouseConfiguration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
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

/// A per-submission completion hook. The buffered writer calls exactly one of these for every
/// batch once that batch's part lands (or fails). The producer uses it to learn durability
/// without the writer knowing anything about what the batch represents.
pub trait FlushToken: Send + Sync {
    /// This batch's part was written durably.
    fn on_flushed(self: Arc<Self>);
    /// This batch's part failed to write.
    fn on_failed(self: Arc<Self>);
}

type Token = Arc<dyn FlushToken>;

enum Msg {
    Submit(String, RecordBatch, Token),
    Flush(tokio::sync::oneshot::Sender<()>),
}

/// Coalesces tagged writes into well-sized parts. A table flushes at `max_rows` or after
/// `max_age`. Each batch carries a [`FlushToken`] the writer notifies once the batch's part is
/// durable or has failed, so a producer can finalize work (checkpointing) per part.
#[derive(Clone)]
pub struct BufferedWriter {
    tx: mpsc::Sender<Msg>,
}

impl BufferedWriter {
    pub fn spawn(
        writer: Arc<ClickHouseWriter>,
        channel_capacity: usize,
        max_rows: usize,
        max_age: Duration,
        max_concurrent: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel(channel_capacity.max(1));
        tokio::spawn(drain(
            writer,
            rx,
            max_rows.max(1),
            max_age,
            max_concurrent.max(1),
        ));
        Self { tx }
    }

    /// Buffer one batch with its completion `token`. Uses `blocking_send`, for the blocking
    /// parse thread.
    pub fn submit(
        &self,
        table: String,
        batch: RecordBatch,
        token: Token,
    ) -> Result<(), WriteError> {
        self.tx
            .blocking_send(Msg::Submit(table, batch, token))
            .map_err(|_| WriteError::Write("buffered writer drain closed".into(), None))
    }

    /// Flush all buffered rows now and wait until they are written. Used to make writes
    /// synchronously visible (tests, shutdown); the steady-state path relies on the size/age
    /// flush instead.
    pub async fn flush(&self) -> Result<(), WriteError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.tx
            .send(Msg::Flush(tx))
            .await
            .map_err(|_| WriteError::Write("buffered writer drain closed".into(), None))?;
        rx.await
            .map_err(|_| WriteError::Write("buffered writer drain closed".into(), None))
    }
}

#[derive(Default)]
struct TableBuffer {
    batches: Vec<RecordBatch>,
    tokens: Vec<Token>,
    rows: usize,
}

async fn drain(
    writer: Arc<ClickHouseWriter>,
    mut rx: mpsc::Receiver<Msg>,
    max_rows: usize,
    max_age: Duration,
    max_concurrent: usize,
) {
    let mut pending: HashMap<String, TableBuffer> = HashMap::new();
    let sem = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    // Parts write concurrently and may land out of order. That is safe: the edge tables are
    // ReplacingMergeTree keyed by row identity + `_version`, and stale cleanup keys on
    // `indexed_at`, so neither correctness nor dedup depends on insert order.
    let mut writes = JoinSet::new();

    let mut ticker = tokio::time::interval(max_age);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                None => break,
                Some(Msg::Flush(ack)) => {
                    for (table, buf) in std::mem::take(&mut pending) {
                        spawn_write(&mut writes, &sem, &writer, table, buf);
                    }
                    while writes.join_next().await.is_some() {}
                    let _ = ack.send(());
                }
                Some(Msg::Submit(table, batch, token)) => {
                    let buf = pending.entry(table.clone()).or_default();
                    buf.rows += batch.num_rows();
                    buf.batches.push(batch);
                    buf.tokens.push(token);
                    if buf.rows >= max_rows {
                        let buf = pending.remove(&table).unwrap();
                        spawn_write(&mut writes, &sem, &writer, table, buf);
                    }
                }
            },
            _ = ticker.tick() => {
                for (table, buf) in std::mem::take(&mut pending) {
                    spawn_write(&mut writes, &sem, &writer, table, buf);
                }
            }
            // Reap finished writes so the JoinSet does not grow unbounded between flushes.
            Some(_) = writes.join_next(), if !writes.is_empty() => {}
        }
    }

    for (table, buf) in std::mem::take(&mut pending) {
        spawn_write(&mut writes, &sem, &writer, table, buf);
    }
    while writes.join_next().await.is_some() {}
}

/// Spawn one coalesced part write, bounded by `sem` to `max_concurrent` in flight.
fn spawn_write(
    writes: &mut JoinSet<()>,
    sem: &Arc<tokio::sync::Semaphore>,
    writer: &Arc<ClickHouseWriter>,
    table: String,
    buf: TableBuffer,
) {
    let (writer, sem) = (writer.clone(), sem.clone());
    writes.spawn(async move {
        let _permit = sem
            .acquire_owned()
            .await
            .expect("write semaphore never closes");
        write_part(&writer, &table, buf).await;
    });
}

/// Notifies every held token of failure when dropped, unless `disarm`ed first. Guarantees a part
/// that unwinds (a panic in `write`) still releases its tokens, so a project can never be stranded
/// with an un-decremented commit (which would hang `flush()` on the inflight count forever).
struct NotifyOnDrop(Vec<Token>);

impl NotifyOnDrop {
    fn disarm(mut self) -> Vec<Token> {
        std::mem::take(&mut self.0)
    }
}

impl Drop for NotifyOnDrop {
    fn drop(&mut self) {
        std::mem::take(&mut self.0)
            .into_iter()
            .for_each(|t| t.on_failed());
    }
}

/// Backoff before each insert retry. A short, bounded ladder so a transient ClickHouse blip
/// does not cost the whole project a re-index next sweep tick, without stalling the writer
/// through a sustained outage.
const WRITE_RETRY_BACKOFF: &[Duration] = &[Duration::from_secs(2), Duration::from_secs(4)];

/// Write one coalesced part, then notify every batch's token of the outcome. A single part can
/// hold batches from many producers; each is told precisely whether its rows landed. Retries the
/// insert with bounded backoff so a transient failure does not strand the part for the sweep.
async fn write_part(writer: &ClickHouseWriter, table: &str, buf: TableBuffer) {
    let guard = NotifyOnDrop(buf.tokens);

    // Retry the insert with bounded backoff so a transient ClickHouse failure does not cost the
    // project a re-index next sweep tick. Arrow batches are Arc-backed, so the per-attempt clone
    // is a refcount bump, not a data copy.
    let mut tries = 0;
    let durable = loop {
        match writer
            .write(table, buf.batches.clone(), Some(WriteDurability::Durable))
            .await
        {
            Ok(report) => break Ok(report),
            Err(e) => match WRITE_RETRY_BACKOFF.get(tries) {
                Some(delay) => {
                    warn!(table, retry = tries + 1, error = %e, "buffered write failed; retrying after backoff");
                    tokio::time::sleep(*delay).await;
                    tries += 1;
                }
                None => break Err(e),
            },
        }
    };

    let tokens = guard.disarm();
    match durable {
        Ok(_) => tokens.into_iter().for_each(|t| t.on_flushed()),
        Err(e) => {
            warn!(table, error = %e, "buffered write flush failed after retries");
            tokens.into_iter().for_each(|t| t.on_failed());
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
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![0i64; rows]))]).unwrap()
    }

    #[derive(Default)]
    struct CountToken {
        flushed: AtomicUsize,
        failed: AtomicUsize,
    }

    impl FlushToken for CountToken {
        fn on_flushed(self: Arc<Self>) {
            self.flushed.fetch_add(1, Ordering::SeqCst);
        }
        fn on_failed(self: Arc<Self>) {
            self.failed.fetch_add(1, Ordering::SeqCst);
        }
    }

    async fn submit(w: &BufferedWriter, table: &str, batch: RecordBatch, token: Token) {
        let w = w.clone();
        let table = table.to_string();
        tokio::task::spawn_blocking(move || w.submit(table, batch, token))
            .await
            .unwrap()
            .unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn size_flush_notifies_each_batch_token_once_its_part_lands() {
        let w = BufferedWriter::spawn(
            Arc::new(ClickHouseWriter::noop()),
            16,
            100,
            Duration::from_secs(3600),
            1,
        );
        let token: Arc<CountToken> = Arc::new(CountToken::default());

        // One batch in each of two tables. gl_edge crosses max_rows and flushes immediately;
        // gl_code_edge stays buffered, so only one notification has fired.
        submit(&w, "gl_edge", batch(100), token.clone()).await;
        submit(&w, "gl_code_edge", batch(10), token.clone()).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(token.flushed.load(Ordering::SeqCst), 1);

        // An explicit flush drains gl_code_edge, notifying the second batch.
        w.flush().await.unwrap();
        assert_eq!(token.flushed.load(Ordering::SeqCst), 2);
        assert_eq!(token.failed.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn flush_waits_for_concurrent_in_flight_writes() {
        let w = BufferedWriter::spawn(
            Arc::new(ClickHouseWriter::noop()),
            64,
            100,
            Duration::from_secs(3600),
            4,
        );
        let token: Arc<CountToken> = Arc::new(CountToken::default());

        // Each batch crosses max_rows on its own table, so all four spawn concurrent writes.
        for table in ["a", "b", "c", "d"] {
            submit(&w, table, batch(100), token.clone()).await;
        }

        w.flush().await.unwrap();
        assert_eq!(token.flushed.load(Ordering::SeqCst), 4);
        assert_eq!(token.failed.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn notify_on_drop_fails_tokens_when_not_disarmed() {
        let token: Arc<CountToken> = Arc::new(CountToken::default());
        let guard = NotifyOnDrop(vec![token.clone()]);
        drop(guard);
        assert_eq!(token.failed.load(Ordering::SeqCst), 1);
        assert_eq!(token.flushed.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn notify_on_drop_is_silent_once_disarmed() {
        let token: Arc<CountToken> = Arc::new(CountToken::default());
        let guard = NotifyOnDrop(vec![token.clone()]);
        let tokens = guard.disarm();
        tokens.into_iter().for_each(|t| t.on_flushed());
        assert_eq!(token.flushed.load(Ordering::SeqCst), 1);
        assert_eq!(token.failed.load(Ordering::SeqCst), 0);
    }
}
