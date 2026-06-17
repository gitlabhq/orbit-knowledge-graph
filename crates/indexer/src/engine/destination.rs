//! Where processed data goes. Implement [`Destination`] to write to your storage.

use std::error::Error as StdError;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use thiserror::Error;

use crate::durability::WriteDurability;

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

/// Creates writers for a storage backend.
#[async_trait]
pub trait Destination: Send + Sync {
    /// Writes with the backend's configured durability. Use
    /// [`new_batch_writer_with_durability`](Self::new_batch_writer_with_durability) where a write
    /// must pin its own (SDLC).
    async fn new_batch_writer(
        &self,
        table: &str,
    ) -> Result<Box<dyn BatchWriter>, DestinationError> {
        self.new_batch_writer_with_durability(table, WriteDurability::FireAndForget)
            .await
    }

    async fn new_batch_writer_with_durability(
        &self,
        table: &str,
        durability: WriteDurability,
    ) -> Result<Box<dyn BatchWriter>, DestinationError>;
}
