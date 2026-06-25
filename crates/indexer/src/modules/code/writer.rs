//! Sync-to-async write bridge for the code-graph pipeline.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use code_graph::v2::SinkError;
use tokio::sync::mpsc;

use crate::destination::{TableWriter, WriteError, WriteReport, WriteStrategy};
use crate::durability::WriteDurability;

/// `BatchSink` that forwards to an mpsc channel for async draining.
pub struct ChannelSink(pub mpsc::Sender<(String, RecordBatch)>);

impl code_graph::v2::BatchSink for ChannelSink {
    fn write_batch(&self, table: &str, batch: &RecordBatch) -> Result<(), SinkError> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        self.0
            .blocking_send((table.to_string(), batch.clone()))
            .map_err(|_| SinkError("channel closed".into()))
    }
}

/// Drain batches from the channel, coalesce per table, and write concurrently.
pub async fn drain_writes<W: TableWriter + 'static>(
    writer: Arc<W>,
    mut rx: mpsc::Receiver<(String, RecordBatch)>,
    strategy: WriteStrategy,
) -> Result<Vec<WriteReport>, WriteError> {
    let max_rows = strategy.max_rows_per_insert.max(1);
    let sem = Arc::new(tokio::sync::Semaphore::new(strategy.max_concurrent.max(1)));
    let mut set = tokio::task::JoinSet::new();
    let mut pending: HashMap<String, (Vec<RecordBatch>, usize)> = HashMap::new();

    while let Some((table, batch)) = rx.recv().await {
        let entry = pending.entry(table.clone()).or_default();
        entry.1 += batch.num_rows();
        entry.0.push(batch);
        if entry.1 >= max_rows {
            let (batches, _) = pending.remove(&table).unwrap();
            let (w, p) = (writer.clone(), sem.clone());
            set.spawn(async move {
                let _permit = p.acquire_owned().await;
                w.write(&table, batches, Some(WriteDurability::Durable))
                    .await
            });
        }
    }

    for (table, (batches, _)) in pending {
        let (w, p) = (writer.clone(), sem.clone());
        set.spawn(async move {
            let _permit = p.acquire_owned().await;
            w.write(&table, batches, Some(WriteDurability::Durable))
                .await
        });
    }

    let mut reports = Vec::new();
    while let Some(r) = set.join_next().await {
        reports.push(r.map_err(|e| WriteError::Write(format!("join: {e}"), None))??);
    }
    Ok(reports)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testkit::MockTableWriter;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use code_graph::v2::BatchSink;

    #[tokio::test]
    async fn drain_writes_returns_per_table_reports() {
        let strategy = WriteStrategy {
            channel_capacity: 8,
            max_rows_per_insert: 500_000,
            max_concurrent: 4,
        };
        let (tx, rx) = mpsc::channel(strategy.channel_capacity);
        let sink = Arc::new(ChannelSink(tx));
        let drain = tokio::spawn(drain_writes(Arc::new(MockTableWriter::new()), rx, strategy));

        let s = Arc::clone(&sink);
        tokio::task::spawn_blocking(move || {
            s.write_batch("gl_file", &test_batch()).unwrap();
            s.write_batch("gl_definition", &test_batch()).unwrap();
        })
        .await
        .unwrap();

        drop(sink);
        let reports = drain.await.unwrap().expect("drain should succeed");
        assert_eq!(reports.len(), 2);
        assert!(reports.iter().all(|r| r.rows == 3));
        assert!(reports.iter().all(|r| r.bytes > 0));
    }

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![1, 2, 3]))]).unwrap()
    }
}
