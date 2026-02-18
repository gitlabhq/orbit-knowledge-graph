//! Watermark storage for tracking code indexing state.

use std::sync::Arc;

use crate::clickhouse::ArrowClickHouseClient;
use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use thiserror::Error;

const TIMESTAMP_FORMAT: &str = "%Y-%m-%d %H:%M:%S%.6f";

#[derive(Debug, Error)]
pub enum WatermarkError {
    #[error("query failed: {0}")]
    Query(String),

    #[error("invalid data type")]
    InvalidType,

    #[error("invalid timestamp value")]
    InvalidTimestamp,
}

#[derive(Debug, Clone)]
pub struct CodeIndexingWatermark {
    pub project_id: i64,
    pub branch: String,
    pub last_event_id: i64,
    pub last_commit: String,
    pub indexed_at: DateTime<Utc>,
}

#[async_trait]
pub trait CodeWatermarkStore: Send + Sync {
    async fn get_watermark(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<CodeIndexingWatermark>, WatermarkError>;

    async fn set_watermark(&self, watermark: &CodeIndexingWatermark) -> Result<(), WatermarkError>;
}

pub(crate) type WatermarkClient = Arc<ArrowClickHouseClient>;

pub struct ClickHouseCodeWatermarkStore {
    client: WatermarkClient,
}

impl ClickHouseCodeWatermarkStore {
    pub fn new(client: WatermarkClient) -> Self {
        Self { client }
    }

    fn extract_watermark(
        batches: Vec<RecordBatch>,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<CodeIndexingWatermark>, WatermarkError> {
        let batch = match batches.into_iter().next() {
            Some(b) if b.num_rows() > 0 => b,
            _ => return Ok(None),
        };

        let last_event_id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(WatermarkError::InvalidType)?;

        let last_commit_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(WatermarkError::InvalidType)?;

        let indexed_at_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or(WatermarkError::InvalidType)?;

        if last_event_id_col.is_null(0) {
            return Ok(None);
        }

        let last_event_id = last_event_id_col.value(0);
        let last_commit = last_commit_col.value(0).to_string();
        let indexed_at_micros = indexed_at_col.value(0);
        let indexed_at = Utc
            .timestamp_micros(indexed_at_micros)
            .single()
            .ok_or(WatermarkError::InvalidTimestamp)?;

        Ok(Some(CodeIndexingWatermark {
            project_id,
            branch: branch.to_string(),
            last_event_id,
            last_commit,
            indexed_at,
        }))
    }
}

#[async_trait]
impl CodeWatermarkStore for ClickHouseCodeWatermarkStore {
    async fn get_watermark(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<CodeIndexingWatermark>, WatermarkError> {
        let query = r#"
            SELECT
                argMax(last_event_id, _version) as last_event_id,
                argMax(last_commit, _version) as last_commit,
                argMax(indexed_at, _version) as indexed_at
            FROM project_code_indexing_watermark
            WHERE project_id = {project_id:Int64}
              AND branch = {branch:String}
        "#;

        let batches = self
            .client
            .query(query)
            .param("project_id", project_id)
            .param("branch", branch)
            .fetch_arrow()
            .await
            .map_err(|e| WatermarkError::Query(e.to_string()))?;

        Self::extract_watermark(batches, project_id, branch)
    }

    async fn set_watermark(&self, watermark: &CodeIndexingWatermark) -> Result<(), WatermarkError> {
        let formatted_timestamp = watermark.indexed_at.format(TIMESTAMP_FORMAT).to_string();

        self.client
            .query(
                r#"
                INSERT INTO project_code_indexing_watermark
                (project_id, branch, last_event_id, last_commit, indexed_at)
                VALUES ({project_id:Int64}, {branch:String}, {last_event_id:Int64}, {last_commit:String}, {indexed_at:String})
            "#,
            )
            .param("project_id", watermark.project_id)
            .param("branch", &watermark.branch)
            .param("last_event_id", watermark.last_event_id)
            .param("last_commit", &watermark.last_commit)
            .param("indexed_at", formatted_timestamp)
            .execute()
            .await
            .map_err(|e| WatermarkError::Query(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    #[allow(dead_code)]
    pub struct MockCodeWatermarkStore {
        watermarks: Mutex<HashMap<(i64, String), CodeIndexingWatermark>>,
    }

    #[allow(dead_code)]
    impl MockCodeWatermarkStore {
        pub fn new() -> Self {
            Self {
                watermarks: Mutex::new(HashMap::new()),
            }
        }
    }

    impl Default for MockCodeWatermarkStore {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl CodeWatermarkStore for MockCodeWatermarkStore {
        async fn get_watermark(
            &self,
            project_id: i64,
            branch: &str,
        ) -> Result<Option<CodeIndexingWatermark>, WatermarkError> {
            let watermarks = self.watermarks.lock();
            Ok(watermarks.get(&(project_id, branch.to_string())).cloned())
        }

        async fn set_watermark(
            &self,
            watermark: &CodeIndexingWatermark,
        ) -> Result<(), WatermarkError> {
            let mut watermarks = self.watermarks.lock();
            watermarks.insert(
                (watermark.project_id, watermark.branch.clone()),
                watermark.clone(),
            );
            Ok(())
        }
    }
}
