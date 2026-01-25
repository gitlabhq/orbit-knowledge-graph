//! Where processed data goes. Implement [`Destination`] to write to your storage.
//!
//! Two writer types: [`BatchWriter`] for one-shot writes, [`StreamWriter`] for
//! buffered writes with explicit flush/close.

use crate::entities::Entity;
use arrow::record_batch::RecordBatch;
use thiserror::Error;

/// Errors that can occur during destination operations.
#[derive(Debug, Error)]
pub enum DestinationError {
    /// Failed to write a batch of records.
    #[error("failed to write batch: {0}")]
    Write(String),

    /// Failed to flush buffered data.
    #[error("failed to flush: {0}")]
    Flush(String),

    /// Failed to close the writer.
    #[error("failed to close writer: {0}")]
    Close(String),

    /// Failed to establish a connection.
    #[error("connection error: {0}")]
    Connection(String),
}

/// A writer for streaming data with explicit lifecycle control.
///
/// Stream writers maintain an open connection and buffer data for efficiency.
/// Use [`StreamWriter::flush`] to ensure data is persisted and
/// [`StreamWriter::close`] when done.
///
/// # Example
///
/// ```ignore
/// let writer = destination.new_stream_writer(entity);
///
/// for batch in batches {
///     writer.write(&[batch])?;
/// }
///
/// writer.flush()?;
/// writer.close()?;
/// ```
pub trait StreamWriter: Send + Sync {
    /// Writes record batches to the destination.
    ///
    /// Data may be buffered internally. Call [`StreamWriter::flush`]
    /// to ensure persistence.
    fn write(&self, batch: &[RecordBatch]) -> Result<(), DestinationError>;

    /// Flushes any buffered data to the destination.
    fn flush(&self) -> Result<(), DestinationError>;

    /// Closes the writer and releases resources.
    ///
    /// This implicitly flushes any remaining buffered data.
    fn close(&self) -> Result<(), DestinationError>;
}

/// A writer for one-shot batch operations.
///
/// Batch writers are simpler than stream writers and handle a complete
/// write operation in a single call.
///
/// # Example
///
/// ```ignore
/// let writer = destination.new_batch_writer(entity);
/// writer.write_batch(&batches)?;
/// ```
pub trait BatchWriter: Send + Sync {
    /// Writes record batches to the destination in a single operation.
    fn write_batch(&self, batch: &[RecordBatch]) -> Result<(), DestinationError>;
}

/// A factory for creating writers to output destinations.
///
/// Destinations represent storage backends like databases or data lakes.
/// They create writers configured for specific entities (tables/collections).
///
/// # Example
///
/// ```ignore
/// use etl_engine::destination::Destination;
/// use etl_engine::entities::Entity;
///
/// fn process_data(destination: &dyn Destination, entity: &Entity, batches: Vec<RecordBatch>) {
///     let writer = destination.new_batch_writer(entity);
///     writer.write_batch(&batches).unwrap();
/// }
/// ```
pub trait Destination: Send + Sync {
    /// Creates a new batch writer for the given entity.
    fn new_batch_writer(&self, entity: &Entity) -> Box<dyn BatchWriter>;

    /// Creates a new stream writer for the given entity.
    fn new_stream_writer(&self, entity: &Entity) -> Box<dyn StreamWriter>;
}
