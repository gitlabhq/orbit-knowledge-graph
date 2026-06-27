use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use tracing::{info, warn};

use super::checkpoint::{CodeCheckpointStore, CodeIndexingCheckpoint};
use crate::clickhouse::{ClickHouseWriter, WriteError};
use crate::durability::WriteDurability;

/// A project's batches stream in under its `seq`; `Done` carries the checkpoint to write once
/// the project's rows are durable.
enum Msg {
    Batch(String, RecordBatch, u64),
    Done(u64, Box<CodeIndexingCheckpoint>),
}

/// Process-wide ClickHouse write sink shared by every code-indexing job. Coalesces the long
/// tail of tiny projects into well-sized parts instead of one small part each, and owns
/// checkpointing: a project is checkpointed only after the flush that makes its rows durable.
///
/// A handler takes a seq from [`next_seq`], streams its batches in with that seq, calls
/// [`finish`] with its checkpoint, and returns — it does not wait for the write. If the
/// process dies before the flush, the project is simply never checkpointed and the
/// once-a-minute backfill sweep re-dispatches it (re-indexing is idempotent). Big projects
/// cross the row cap and flush promptly; a failed flush is logged and the project's
/// checkpoint is never written, so it is re-indexed.
pub struct CodeWriteSink {
    tx: tokio::sync::mpsc::Sender<Msg>,
    seq: AtomicU64,
}

impl CodeWriteSink {
    pub fn new(
        writer: Arc<ClickHouseWriter>,
        checkpoint_store: Arc<dyn CodeCheckpointStore>,
        channel_capacity: usize,
        max_rows: usize,
        max_buffer_age: Duration,
    ) -> Arc<Self> {
        let (tx, rx) = tokio::sync::mpsc::channel(channel_capacity.max(1));
        tokio::spawn(drain(
            writer,
            checkpoint_store,
            rx,
            max_rows.max(1),
            max_buffer_age,
        ));
        Arc::new(Self {
            tx,
            seq: AtomicU64::new(1),
        })
    }

    /// Reserve a seq for one project. All of that project's batches must be submitted with
    /// this seq and contiguously (before [`finish`]), so the durability watermark cleanly
    /// separates flushed projects from buffered ones.
    pub fn next_seq(&self) -> u64 {
        self.seq.fetch_add(1, Ordering::Relaxed)
    }

    /// Buffer one batch from the blocking parse thread, backpressuring the parser when the
    /// shared channel is full.
    pub fn submit(&self, table: String, batch: RecordBatch, seq: u64) -> Result<(), WriteError> {
        self.tx
            .blocking_send(Msg::Batch(table, batch, seq))
            .map_err(|_| WriteError::Write("code write sink drain closed".into(), None))
    }

    /// Hand the sink the project's checkpoint. It is written once the project's rows are
    /// durable; the caller does not wait.
    pub async fn finish(
        &self,
        seq: u64,
        checkpoint: CodeIndexingCheckpoint,
    ) -> Result<(), WriteError> {
        self.tx
            .send(Msg::Done(seq, Box::new(checkpoint)))
            .await
            .map_err(|_| WriteError::Write("code write sink drain closed".into(), None))
    }
}

/// Per-table accumulator: buffered batches, row count, and the lowest seq still buffered.
#[derive(Default)]
struct Table {
    batches: Vec<RecordBatch>,
    rows: usize,
    min_seq: u64,
}

async fn drain(
    writer: Arc<ClickHouseWriter>,
    checkpoint_store: Arc<dyn CodeCheckpointStore>,
    mut rx: tokio::sync::mpsc::Receiver<Msg>,
    max_rows: usize,
    max_buffer_age: Duration,
) {
    let mut pending: HashMap<String, Table> = HashMap::new();
    let mut checkpoints: BTreeMap<u64, CodeIndexingCheckpoint> = BTreeMap::new();
    let mut max_seq = 0u64;
    // Lowest seq whose flush failed. Every project at or above it is unproven, so the
    // watermark never passes it and those projects re-index on the next sweep.
    let mut failed_floor: Option<u64> = None;

    let mut ticker = tokio::time::interval(max_buffer_age);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticker.tick().await;

    loop {
        let flushed = tokio::select! {
            msg = rx.recv() => match msg {
                None => break,
                Some(Msg::Done(seq, checkpoint)) => {
                    max_seq = max_seq.max(seq);
                    checkpoints.insert(seq, *checkpoint);
                    // A project that produced no batches (parsed clean but emitted nothing) is
                    // durable immediately; recheck the checkpoint set so it isn't stranded.
                    true
                }
                Some(Msg::Batch(table, batch, seq)) => {
                    max_seq = max_seq.max(seq);
                    let t = pending.entry(table.clone()).or_insert(Table { min_seq: seq, ..Default::default() });
                    t.rows += batch.num_rows();
                    t.min_seq = t.min_seq.min(seq);
                    t.batches.push(batch);
                    if t.rows >= max_rows {
                        let t = pending.remove(&table).unwrap();
                        flush(&writer, &table, t, &mut failed_floor).await;
                        true
                    } else {
                        false
                    }
                }
            },
            _ = ticker.tick() => {
                for (table, t) in std::mem::take(&mut pending) {
                    flush(&writer, &table, t, &mut failed_floor).await;
                }
                true
            }
        };
        if flushed {
            checkpoint_durable(
                &checkpoint_store,
                &pending,
                &mut checkpoints,
                max_seq,
                failed_floor,
            )
            .await;
        }
    }

    for (table, t) in std::mem::take(&mut pending) {
        flush(&writer, &table, t, &mut failed_floor).await;
    }
    checkpoint_durable(
        &checkpoint_store,
        &pending,
        &mut checkpoints,
        max_seq,
        failed_floor,
    )
    .await;
}

/// Write one table's coalesced batches as a durable part. On failure, poison every seq the
/// part carried so those projects never checkpoint and are re-indexed.
async fn flush(writer: &ClickHouseWriter, table: &str, t: Table, failed_floor: &mut Option<u64>) {
    if let Err(e) = writer
        .write(table, t.batches, Some(WriteDurability::Durable))
        .await
    {
        warn!(table, error = %e, "code write sink flush failed; affected projects will be re-indexed");
        *failed_floor = Some(failed_floor.map_or(t.min_seq, |f| f.min(t.min_seq)));
    }
}

/// Write the checkpoint of every project whose rows are now durable: all seqs at or below the
/// lowest still-buffered seq (or the highest accepted seq when nothing is buffered), but never
/// at or above a seq whose flush failed.
async fn checkpoint_durable(
    store: &Arc<dyn CodeCheckpointStore>,
    pending: &HashMap<String, Table>,
    checkpoints: &mut BTreeMap<u64, CodeIndexingCheckpoint>,
    max_seq: u64,
    failed_floor: Option<u64>,
) {
    let mut watermark = pending
        .values()
        .filter(|t| !t.batches.is_empty())
        .map(|t| t.min_seq)
        .min()
        .map_or(max_seq, |lowest| lowest.saturating_sub(1));
    if let Some(floor) = failed_floor {
        watermark = watermark.min(floor.saturating_sub(1));
    }

    while let Some((&seq, _)) = checkpoints.iter().next() {
        if seq > watermark {
            break;
        }
        let checkpoint = checkpoints.remove(&seq).unwrap();
        match store.set_checkpoint(&checkpoint).await {
            Ok(()) => info!(
                project_id = checkpoint.project_id,
                branch = %checkpoint.branch,
                task_id = checkpoint.last_task_id,
                "completed code indexing"
            ),
            Err(e) => warn!(
                project_id = checkpoint.project_id,
                error = %e,
                "failed to checkpoint code indexing; project will be re-indexed",
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clickhouse::ClickHouseWriter;
    use crate::modules::code::checkpoint::test_utils::MockCodeCheckpointStore;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use chrono::Utc;

    fn batch(rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![0i64; rows]))]).unwrap()
    }

    fn checkpoint(project_id: i64) -> CodeIndexingCheckpoint {
        CodeIndexingCheckpoint {
            traversal_path: format!("1/{project_id}/"),
            project_id,
            branch: "main".into(),
            last_task_id: project_id,
            last_commit: None,
            indexed_at: Utc::now(),
        }
    }

    async fn submit(sink: &Arc<CodeWriteSink>, table: &str, batch: RecordBatch, seq: u64) {
        let sink = sink.clone();
        let table = table.to_string();
        tokio::task::spawn_blocking(move || sink.submit(table, batch, seq))
            .await
            .unwrap()
            .unwrap();
    }

    async fn await_checkpoint(store: &Arc<MockCodeCheckpointStore>, project_id: i64) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while store
                .get_checkpoint(&format!("1/{project_id}/"), project_id, "main")
                .await
                .unwrap()
                .is_none()
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("project {project_id} never checkpointed"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoints_after_size_flush() {
        let store = Arc::new(MockCodeCheckpointStore::new());
        let sink = CodeWriteSink::new(
            Arc::new(ClickHouseWriter::noop()),
            store.clone(),
            16,
            50,
            Duration::from_secs(3600),
        );
        let seq = sink.next_seq();
        submit(&sink, "gl_edge", batch(60), seq).await;
        sink.finish(seq, checkpoint(7)).await.unwrap();
        await_checkpoint(&store, 7).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn checkpoints_after_age_flush() {
        let store = Arc::new(MockCodeCheckpointStore::new());
        let sink = CodeWriteSink::new(
            Arc::new(ClickHouseWriter::noop()),
            store.clone(),
            16,
            1_000_000,
            Duration::from_millis(40),
        );
        let seq = sink.next_seq();
        submit(&sink, "gl_edge", batch(5), seq).await;
        sink.finish(seq, checkpoint(9)).await.unwrap();
        await_checkpoint(&store, 9).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn holds_checkpoint_until_all_tables_flush() {
        let store = Arc::new(MockCodeCheckpointStore::new());
        let sink = CodeWriteSink::new(
            Arc::new(ClickHouseWriter::noop()),
            store.clone(),
            16,
            100,
            Duration::from_secs(3600),
        );
        let seq = sink.next_seq();
        // gl_edge crosses the cap and flushes, but gl_code_edge stays buffered, so the
        // project is not yet durable and must not checkpoint.
        submit(&sink, "gl_code_edge", batch(10), seq).await;
        submit(&sink, "gl_edge", batch(100), seq).await;
        sink.finish(seq, checkpoint(11)).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            store
                .get_checkpoint("1/11/", 11, "main")
                .await
                .unwrap()
                .is_none(),
            "must not checkpoint while gl_code_edge rows are still buffered",
        );
    }
}
