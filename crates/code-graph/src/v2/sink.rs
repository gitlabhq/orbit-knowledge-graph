use arrow::record_batch::RecordBatch;

use super::linker::CodeGraph;

pub trait GraphConverter: Send + Sync {
    fn convert(&self, graph: CodeGraph) -> Result<Vec<(String, RecordBatch)>, SinkError>;
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

/// Callback type for receiving converted batches from the pipeline.
pub type OnBatch = dyn Fn(&str, RecordBatch) -> Result<(), SinkError> + Send + Sync;
