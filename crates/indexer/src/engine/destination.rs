//! Write abstraction for the indexer ETL pipeline.

use std::error::Error as StdError;

use arrow::record_batch::RecordBatch;
use thiserror::Error;

use crate::durability::WriteDurability;

pub type UnderlyingError = Box<dyn StdError + Send + Sync>;

#[derive(Debug, Error)]
pub enum WriteError {
    #[error("failed to write: {0}")]
    Write(String, #[source] Option<UnderlyingError>),

    #[error("connection error: {0}")]
    Connection(String, #[source] Option<UnderlyingError>),

    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
}

#[derive(Debug, Clone)]
pub struct WriteReport {
    pub table: String,
    pub rows: u64,
    pub bytes: u64,
}

pub trait TableWriter: Send + Sync {
    fn write(
        &self,
        table: &str,
        batches: Vec<RecordBatch>,
        durability: Option<WriteDurability>,
    ) -> impl std::future::Future<Output = Result<WriteReport, WriteError>> + Send;
}

#[derive(Debug, Clone)]
pub struct WriteStrategy {
    pub channel_capacity: usize,
    pub max_rows_per_insert: usize,
    pub max_concurrent: usize,
}
