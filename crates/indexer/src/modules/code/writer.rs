//! Streaming sync-to-async bridge for code-graph writes.
//!
//! The code-graph pipeline calls [`BatchSink::write_batch`] from plain OS
//! threads. This module bridges those sync calls to the async
//! [`Destination`]/[`BatchWriter`] layer via a bounded mpsc channel.
//! Large batches are sliced pre-send; small per-language batches for the
//! same table are coalesced in the drain loop before writing.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use code_graph::v2::SinkError;
use parking_lot::Mutex;
use tokio::sync::mpsc;

use crate::destination::{BatchWriterOptions, Destination};
use crate::durability::WriteDurability;

type WriteOutcome = Result<Vec<TableWriteTotals>, SinkError>;
type WriterState = (mpsc::Sender<(String, RecordBatch)>, tokio::task::JoinHandle<WriteOutcome>);

pub struct StreamingClickHouseSink {
    state: Mutex<Option<WriterState>>,
    max_rows_per_send: usize,
}

impl StreamingClickHouseSink {
    pub fn new(
        destination: Arc<dyn Destination>,
        channel_capacity: usize,
        max_concurrent: usize,
        max_rows_per_insert: usize,
    ) -> Self {
        let max_rows = max_rows_per_insert.max(1);
        let (tx, rx) = mpsc::channel(channel_capacity.max(1));
        let task = tokio::runtime::Handle::current().spawn(
            drain_loop(destination, rx, max_concurrent.max(1), max_rows),
        );
        Self { state: Mutex::new(Some((tx, task))), max_rows_per_send: max_rows }
    }

    pub async fn finish(&self) -> WriteOutcome {
        let (tx, task) = self.state.lock().take().expect("finish called exactly once");
        drop(tx);
        task.await.map_err(|e| SinkError(format!("writer task join: {e}")))?
    }
}

impl code_graph::v2::BatchSink for StreamingClickHouseSink {
    fn write_batch(&self, table: &str, batch: &RecordBatch) -> Result<(), SinkError> {
        if batch.num_rows() == 0 { return Ok(()); }
        let tx = self.state.lock().as_ref()
            .map(|(tx, _)| tx.clone())
            .ok_or_else(|| SinkError("streaming sink already finished".into()))?;
        let table = table.to_string();
        let mut offset = 0;
        while offset < batch.num_rows() {
            let len = (batch.num_rows() - offset).min(self.max_rows_per_send);
            tx.blocking_send((table.clone(), batch.slice(offset, len)))
                .map_err(|_| SinkError("streaming sink writer stopped".into()))?;
            offset += len;
        }
        Ok(())
    }
}

/// Per-table buffer of batches waiting to fill an insert.
#[derive(Default)]
struct Pending {
    batches: Vec<RecordBatch>,
    rows: usize,
}

async fn drain_loop(
    destination: Arc<dyn Destination>,
    mut rx: mpsc::Receiver<(String, RecordBatch)>,
    max_concurrent: usize,
    max_rows: usize,
) -> WriteOutcome {
    let sem = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let mut set = tokio::task::JoinSet::new();
    let mut totals: HashMap<String, TableWriteTotals> = HashMap::new();
    let mut pending: HashMap<String, Pending> = HashMap::new();

    while let Some((table, batch)) = rx.recv().await {
        let p = pending.entry(table.clone()).or_default();
        p.rows += batch.num_rows();
        p.batches.push(batch);
        if p.rows >= max_rows {
            let flushed = pending.remove(&table).unwrap();
            spawn_write(&sem, &mut set, &destination, table, flushed.batches);
        }
    }

    for (table, p) in pending.drain() {
        if !p.batches.is_empty() {
            spawn_write(&sem, &mut set, &destination, table, p.batches);
        }
    }

    while let Some(res) = set.join_next().await {
        let (t, r, b) = res.map_err(|e| SinkError(format!("join: {e}")))??;
        let e = totals.entry(t.clone()).or_insert(TableWriteTotals { table: t, rows: 0, bytes: 0 });
        e.rows += r;
        e.bytes += b;
    }
    Ok(totals.into_values().collect())
}

fn spawn_write(
    sem: &Arc<tokio::sync::Semaphore>,
    set: &mut tokio::task::JoinSet<Result<(String, u64, u64), SinkError>>,
    destination: &Arc<dyn Destination>,
    table: String,
    batches: Vec<RecordBatch>,
) {
    let permit = Arc::clone(sem);
    let dest = destination.clone();
    set.spawn(async move {
        let permit = permit.acquire_owned().await
            .map_err(|e| SinkError(format!("semaphore: {e}")))?;
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let bytes: u64 = batches.iter().map(|b| b.get_array_memory_size() as u64).sum();
        let opts = BatchWriterOptions { durability: Some(WriteDurability::Durable) };
        let w = dest.new_batch_writer(&table, opts).await
            .map_err(|e| SinkError(format!("writer for {table}: {e}")))?;
        w.write_batch(&batches).await
            .map_err(|e| SinkError(format!("write to {table}: {e}")))?;
        drop(permit);
        Ok((table, rows, bytes))
    });
}

#[derive(Debug, Clone)]
pub struct TableWriteTotals {
    pub table: String,
    pub rows: u64,
    pub bytes: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::MockDestination;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use code_graph::v2::BatchSink;

    #[tokio::test]
    async fn finish_returns_per_table_totals() {
        let sink = Arc::new(StreamingClickHouseSink::new(Arc::new(MockDestination::new()), 8, 4, 500_000));
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        let writer = Arc::clone(&sink);
        tokio::task::spawn_blocking(move || {
            writer.write_batch("gl_file", &batch).unwrap();
            writer.write_batch("gl_definition", &batch).unwrap();
        }).await.unwrap();

        let per_table = sink.finish().await.expect("finish should succeed");
        let by_table: HashMap<&str, &TableWriteTotals> = per_table.iter().map(|t| (t.table.as_str(), t)).collect();
        assert_eq!(by_table.len(), 2);
        assert_eq!(by_table["gl_file"].rows, 3);
        assert_eq!(by_table["gl_definition"].rows, 3);
        assert!(by_table.values().all(|t| t.bytes > 0));
    }
}
