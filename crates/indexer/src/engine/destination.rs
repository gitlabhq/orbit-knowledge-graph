//! Where processed data goes. Implement [`Destination`] to write to your storage.

use std::error::Error as StdError;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use thiserror::Error;

/// Underlying error from implementations.
pub type UnderlyingError = Box<dyn StdError + Send + Sync>;

/// Errors that can occur during destination operations.
#[derive(Debug, Error)]
pub enum DestinationError {
    #[error("failed to write batch: {0}")]
    Write(String, #[source] Option<UnderlyingError>),

    #[error("connection error: {0}")]
    Connection(String, #[source] Option<UnderlyingError>),

    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
}

/// Writes record batches to a destination.
#[async_trait]
pub trait BatchWriter: Send + Sync {
    async fn write_batch(&self, batch: &[RecordBatch]) -> Result<(), DestinationError>;
}

/// An insert kept open across `RecordBatch` writes; all batches land as one
/// logical write on [`finish`](Self::finish), so the caller never holds the
/// whole insert at once.
#[async_trait]
pub trait StreamingWriter: Send {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DestinationError>;
    async fn finish(self: Box<Self>) -> Result<(), DestinationError>;
}

/// Creates writers for a storage backend.
#[async_trait]
pub trait Destination: Send + Sync {
    async fn new_batch_writer(&self, table: &str)
    -> Result<Box<dyn BatchWriter>, DestinationError>;

    /// Opens a [`StreamingWriter`]. The default buffers and writes on `finish`,
    /// so only backends that gain from true streaming (ClickHouse) override it.
    async fn open_streaming_writer(
        &self,
        table: &str,
    ) -> Result<Box<dyn StreamingWriter>, DestinationError> {
        Ok(Box::new(BufferingStreamingWriter {
            writer: self.new_batch_writer(table).await?,
            batches: Vec::new(),
        }))
    }
}

struct BufferingStreamingWriter {
    writer: Box<dyn BatchWriter>,
    batches: Vec<RecordBatch>,
}

#[async_trait]
impl StreamingWriter for BufferingStreamingWriter {
    async fn write_batch(&mut self, batch: &RecordBatch) -> Result<(), DestinationError> {
        self.batches.push(batch.clone());
        Ok(())
    }

    async fn finish(self: Box<Self>) -> Result<(), DestinationError> {
        if self.batches.is_empty() {
            return Ok(());
        }
        self.writer.write_batch(&self.batches).await
    }
}
