use arrow::record_batch::RecordBatch;

use super::linker::CodeGraph;

/// Converts a `CodeGraph` to Arrow RecordBatches. Downstream crates
/// implement this with their own envelope columns, ontology specs,
/// and ID assignment logic.
///
/// Called on the CPU thread after all phases complete. Takes ownership
/// of the graph — convert everything, then let it drop.
pub trait GraphConverter: Send + Sync {
    fn convert(&self, graph: CodeGraph) -> Result<Vec<(String, RecordBatch)>, SinkError>;
}

/// Receives named Arrow RecordBatches for writing to a destination
/// (ClickHouse, DuckDB, lance, etc.).
///
/// Multiple per-language writer threads call concurrently — must be
/// thread-safe. Implementations handle their own serialization if
/// the destination requires it (e.g. DuckDB single-writer).
pub trait BatchSink: Send + Sync {
    fn write_batch(&self, table: &str, batch: &RecordBatch) -> Result<(), SinkError>;
}

#[derive(Debug)]
pub struct SinkError(pub String);

impl std::fmt::Display for SinkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sink error: {}", self.0)
    }
}

impl std::error::Error for SinkError {}

impl From<String> for SinkError {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<arrow::error::ArrowError> for SinkError {
    fn from(e: arrow::error::ArrowError) -> Self {
        Self(e.to_string())
    }
}

/// A sink that collects all batches in memory. Useful for tests
/// that need to inspect the output without writing to a database.
pub struct CollectSink {
    batches: std::sync::Mutex<Vec<(String, RecordBatch)>>,
}

impl Default for CollectSink {
    fn default() -> Self {
        Self::new()
    }
}

impl CollectSink {
    pub fn new() -> Self {
        Self {
            batches: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub fn take(&self) -> Vec<(String, RecordBatch)> {
        std::mem::take(&mut *self.batches.lock().unwrap())
    }
}

impl BatchSink for CollectSink {
    fn write_batch(&self, table: &str, batch: &RecordBatch) -> Result<(), SinkError> {
        self.batches
            .lock()
            .unwrap()
            .push((table.to_string(), batch.clone()));
        Ok(())
    }
}

/// A no-op sink that discards all batches.
pub struct NullSink;

impl BatchSink for NullSink {
    fn write_batch(&self, _table: &str, _batch: &RecordBatch) -> Result<(), SinkError> {
        Ok(())
    }
}
