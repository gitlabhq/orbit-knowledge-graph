//! Sync-to-async streaming write bridge over [`TableWriter`].

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use code_graph::v2::SinkError;
use thiserror::Error;
use tokio::sync::mpsc;

use crate::destination::{TableWriter, Writable, WriteReport};
use crate::durability::WriteDurability;

#[derive(Debug, Error)]
#[error("{0}")]
pub struct StreamWriteError(pub String);

type Outcome = Result<Vec<WriteReport>, StreamWriteError>;

pub struct StreamWriter {
    tx: mpsc::Sender<(String, RecordBatch)>,
    max_rows_per_send: usize,
}

pub struct StreamHandle {
    task: tokio::task::JoinHandle<Outcome>,
}

impl StreamWriter {
    pub fn new<W: TableWriter + 'static>(
        writer: Arc<W>,
        channel_capacity: usize,
        max_concurrent: usize,
        max_rows_per_insert: usize,
    ) -> (Self, StreamHandle) {
        let max_rows = max_rows_per_insert.max(1);
        let (tx, rx) = mpsc::channel(channel_capacity.max(1));
        let task = tokio::runtime::Handle::current().spawn(drain_loop(
            writer,
            rx,
            max_concurrent.max(1),
            max_rows,
        ));
        (
            Self {
                tx,
                max_rows_per_send: max_rows,
            },
            StreamHandle { task },
        )
    }

    pub fn send(&self, table: &str, batch: &RecordBatch) -> Result<(), StreamWriteError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let table = table.to_string();
        let mut offset = 0;
        while offset < batch.num_rows() {
            let len = (batch.num_rows() - offset).min(self.max_rows_per_send);
            self.tx
                .blocking_send((table.clone(), batch.slice(offset, len)))
                .map_err(|_| StreamWriteError("stream writer stopped".into()))?;
            offset += len;
        }
        Ok(())
    }
}

impl StreamHandle {
    pub async fn finish(self) -> Outcome {
        self.task
            .await
            .map_err(|e| StreamWriteError(format!("join: {e}")))?
    }
}

impl code_graph::v2::BatchSink for StreamWriter {
    fn write_batch(&self, table: &str, batch: &RecordBatch) -> Result<(), SinkError> {
        self.send(table, batch)
            .map_err(|e| SinkError(e.to_string()))
    }
}

#[derive(Default)]
struct Pending {
    batches: Vec<RecordBatch>,
    rows: usize,
}

async fn drain_loop<W: TableWriter>(
    writer: Arc<W>,
    mut rx: mpsc::Receiver<(String, RecordBatch)>,
    max_concurrent: usize,
    max_rows: usize,
) -> Outcome {
    let sem = Arc::new(tokio::sync::Semaphore::new(max_concurrent));
    let mut set = tokio::task::JoinSet::new();
    let mut reports: Vec<WriteReport> = Vec::new();
    let mut pending: HashMap<String, Pending> = HashMap::new();

    while let Some((table, batch)) = rx.recv().await {
        let p = pending.entry(table.clone()).or_default();
        p.rows += batch.num_rows();
        p.batches.push(batch);
        if p.rows >= max_rows {
            let flushed = pending.remove(&table).unwrap();
            spawn_write(&sem, &mut set, &writer, table, flushed.batches);
        }
    }

    for (table, p) in pending.drain() {
        if !p.batches.is_empty() {
            spawn_write(&sem, &mut set, &writer, table, p.batches);
        }
    }

    while let Some(res) = set.join_next().await {
        reports.push(res.map_err(|e| StreamWriteError(format!("join: {e}")))?
            .map_err(|e| StreamWriteError(format!("write: {e}")))?);
    }
    Ok(reports)
}

fn spawn_write<W: TableWriter + 'static>(
    sem: &Arc<tokio::sync::Semaphore>,
    set: &mut tokio::task::JoinSet<Result<WriteReport, crate::destination::DestinationError>>,
    writer: &Arc<W>,
    table: String,
    batches: Vec<RecordBatch>,
) {
    let permit = Arc::clone(sem);
    let w = Arc::clone(writer);
    set.spawn(async move {
        let permit = permit
            .acquire_owned()
            .await
            .map_err(|e| crate::destination::DestinationError::Write(format!("semaphore: {e}"), None))?;
        let report = w.write(Writable::new(table, batches).durable()).await?;
        drop(permit);
        Ok(report)
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::MockTableWriter;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use code_graph::v2::BatchSink;

    #[tokio::test]
    async fn finish_returns_per_table_reports() {
        let (writer, handle) =
            StreamWriter::new(Arc::new(MockTableWriter::new()), 8, 4, 500_000);
        let writer = Arc::new(writer);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap();

        let w = Arc::clone(&writer);
        tokio::task::spawn_blocking(move || {
            w.write_batch("gl_file", &batch).unwrap();
            w.write_batch("gl_definition", &batch).unwrap();
        })
        .await
        .unwrap();

        drop(writer);
        let reports = handle.finish().await.expect("finish should succeed");
        assert_eq!(reports.len(), 2);
        assert!(reports.iter().all(|r| r.rows == 3));
        assert!(reports.iter().all(|r| r.bytes > 0));
    }
}
