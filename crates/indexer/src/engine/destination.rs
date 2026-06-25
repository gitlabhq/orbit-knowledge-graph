//! Write abstraction for the indexer ETL pipeline.

use std::error::Error as StdError;

use arrow::record_batch::RecordBatch;
use thiserror::Error;

use crate::durability::WriteDurability;

pub type UnderlyingError = Box<dyn StdError + Send + Sync>;

#[derive(Debug, Error)]
pub enum DestinationError {
    #[error("failed to write batch: {0}")]
    Write(String, #[source] Option<UnderlyingError>),

    #[error("connection error: {0}")]
    Connection(String, #[source] Option<UnderlyingError>),

    #[error("invalid configuration: {0}")]
    InvalidConfiguration(String),
}

// ---------------------------------------------------------------------------
// Writable: data + table + durability
// ---------------------------------------------------------------------------

pub trait IntoRecordBatches: Send {
    fn into_batches(self) -> Vec<RecordBatch>;
}

impl IntoRecordBatches for RecordBatch {
    fn into_batches(self) -> Vec<RecordBatch> {
        vec![self]
    }
}

impl IntoRecordBatches for Vec<RecordBatch> {
    fn into_batches(self) -> Vec<RecordBatch> {
        self
    }
}

pub struct Writable {
    pub table: String,
    pub batches: Vec<RecordBatch>,
    pub durability: Option<WriteDurability>,
}

impl Writable {
    pub fn new(table: impl Into<String>, data: impl IntoRecordBatches) -> Self {
        Self {
            table: table.into(),
            batches: data.into_batches(),
            durability: None,
        }
    }

    pub fn durable(mut self) -> Self {
        self.durability = Some(WriteDurability::Durable);
        self
    }

    pub fn fire_and_forget(mut self) -> Self {
        self.durability = Some(WriteDurability::FireAndForget);
        self
    }

    pub fn with_durability(mut self, durability: Option<WriteDurability>) -> Self {
        self.durability = durability;
        self
    }
}

// ---------------------------------------------------------------------------
// TableWriter: the single write trait
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
pub struct WriteReport {
    pub table: String,
    pub rows: u64,
    pub bytes: u64,
}

pub trait TableWriter: Send + Sync {
    fn write(
        &self,
        writable: Writable,
    ) -> impl std::future::Future<Output = Result<WriteReport, DestinationError>> + Send;
}

#[derive(Debug, Clone)]
pub struct WriteStrategy {
    pub channel_capacity: usize,
    pub max_rows_per_insert: usize,
    pub max_concurrent: usize,
}
