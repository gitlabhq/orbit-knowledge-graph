//! Bridges the code-graph [`BatchSink`](code_graph::v2::BatchSink) trait
//! to the engine's [`StreamWriter`].

use arrow::record_batch::RecordBatch;
use code_graph::v2::SinkError;

pub use crate::engine::stream_writer::{StreamWriter, WriteTotals};

impl code_graph::v2::BatchSink for StreamWriter {
    fn write_batch(&self, table: &str, batch: &RecordBatch) -> Result<(), SinkError> {
        self.send(table, batch).map_err(|e| SinkError(e.to_string()))
    }
}
