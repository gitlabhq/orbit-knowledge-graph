//! Write abstraction for the indexer ETL pipeline.

use std::error::Error as StdError;

use arrow::record_batch::RecordBatch;
use thiserror::Error;

use crate::durability::WriteDurability;

#[derive(Debug, Error)]
pub enum DestinationError {
    #[error("failed to write: {0}")]
    Write(String, #[source] Option<Box<dyn StdError + Send + Sync>>),

    #[error("connection error: {0}")]
    Connection(String, #[source] Option<Box<dyn StdError + Send + Sync>>),

    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
}

#[derive(Debug, Clone)]
pub struct DestinationReport {
    pub table: String,
    pub rows: u64,
    pub bytes: u64,
}

pub trait Destination: Send + Sync {
    fn write(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
        durability: Option<WriteDurability>,
    ) -> impl std::future::Future<Output = Result<DestinationReport, DestinationError>> + Send;
}
