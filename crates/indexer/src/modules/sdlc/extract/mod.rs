mod clickhouse;

#[cfg(test)]
mod memory;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::durability::WriteDurability;
use crate::handler::HandlerError;
use crate::observer::IndexingMode;

pub(in crate::modules::sdlc) use clickhouse::ClickHouseExtractor;
#[cfg(test)]
pub(in crate::modules::sdlc) use memory::MemoryExtractor;

#[derive(Debug, Clone)]
pub(in crate::modules::sdlc) struct ExtractRunContext {
    pub position_key: String,
    pub requested_watermark: DateTime<Utc>,
    pub traversal_path: Option<String>,
}

pub(in crate::modules::sdlc) struct ExtractRun {
    pub indexing_mode: IndexingMode,
    pub sessions: Vec<Box<dyn ExtractSession>>,
    pub completion: Box<dyn ExtractRunCompletion>,
}

#[derive(Clone)]
pub(in crate::modules::sdlc) struct ExtractPage {
    pub batches: Vec<RecordBatch>,
    pub row_count: u64,
    pub resume: ExtractResume,
    pub stats: ExtractPageStats,
    pub has_more: bool,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(in crate::modules::sdlc) struct ExtractPageStats {
    pub scanned_rows: u64,
    pub scanned_bytes: u64,
    pub elapsed_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(in crate::modules::sdlc) struct ExtractResume {
    #[serde(rename = "s")]
    source: String,
    #[serde(rename = "v")]
    version: u16,
    #[serde(rename = "p")]
    payload: Value,
}

#[async_trait]
pub(in crate::modules::sdlc) trait Extractor: Send + Sync {
    async fn start_extraction(
        &self,
        context: ExtractRunContext,
    ) -> Result<ExtractRun, HandlerError>;
}

#[async_trait]
pub(in crate::modules::sdlc) trait ExtractSession: Send {
    async fn get_next_page(&mut self) -> Result<Option<ExtractPage>, HandlerError>;

    async fn save_page_resume(&self, resume: &ExtractResume) -> Result<(), HandlerError>;

    async fn save_completed(&self, durability: WriteDurability) -> Result<(), HandlerError>;
}

#[async_trait]
pub(in crate::modules::sdlc) trait ExtractRunCompletion: Send {
    async fn finish_extraction(self: Box<Self>) -> Result<(), HandlerError>;
}

impl ExtractPage {
    pub fn rows(&self) -> u64 {
        self.row_count
    }

    pub fn bytes(&self) -> u64 {
        self.batches
            .iter()
            .map(|batch| batch.get_array_memory_size() as u64)
            .sum()
    }
}

impl ExtractResume {
    pub fn from_source_payload<T: Serialize>(
        source: &str,
        version: u16,
        payload: &T,
    ) -> Result<Self, HandlerError> {
        let payload = serde_json::to_value(payload).map_err(|error| {
            HandlerError::Processing(format!("failed to encode {source} extract resume: {error}"))
        })?;
        Ok(Self {
            source: source.to_string(),
            version,
            payload,
        })
    }

    pub fn get_source_payload<T: DeserializeOwned>(
        &self,
        expected_source: &str,
        expected_version: u16,
    ) -> Result<T, HandlerError> {
        if self.source != expected_source || self.version != expected_version {
            return Err(HandlerError::Processing(format!(
                "extract resume expected {expected_source} version {expected_version}, found {} version {}",
                self.source, self.version
            )));
        }
        serde_json::from_value(self.payload.clone()).map_err(|error| {
            HandlerError::Processing(format!(
                "failed to decode {expected_source} extract resume: {error}"
            ))
        })
    }

    pub fn to_checkpoint_value(&self) -> Result<String, HandlerError> {
        serde_json::to_string(self).map_err(|error| {
            HandlerError::Processing(format!("failed to serialize extract resume: {error}"))
        })
    }

    pub fn from_checkpoint_value(value: &str) -> Result<Self, HandlerError> {
        serde_json::from_str(value).map_err(|error| {
            HandlerError::Processing(format!("failed to deserialize extract resume: {error}"))
        })
    }
}
