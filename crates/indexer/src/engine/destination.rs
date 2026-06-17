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

/// `durability: None` inherits the backend's configured settings.
#[derive(Clone, Copy, Debug, Default)]
pub struct BatchWriterOptions {
    pub durability: Option<WriteDurability>,
}

/// Creates writers for a storage backend.
#[async_trait]
pub trait Destination: Send + Sync {
    async fn new_batch_writer(
        &self,
        table: &str,
        options: BatchWriterOptions,
    ) -> Result<Box<dyn BatchWriter>, DestinationError>;
}
