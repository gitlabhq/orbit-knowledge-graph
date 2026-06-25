//! Write abstraction for the indexer ETL pipeline.

use std::error::Error as StdError;

use thiserror::Error;

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
