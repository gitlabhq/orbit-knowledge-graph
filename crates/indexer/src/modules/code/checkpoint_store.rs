//! Checkpoint storage for tracking code indexing state.

use std::sync::Arc;

use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use gkg_utils::arrow::ArrowUtils;
use thiserror::Error;

const CODE_INDEXING_CHECKPOINT_TABLE: &str = "code_indexing_checkpoint";

#[derive(Debug, Error)]
pub enum CheckpointError {
    #[error("query failed: {0}")]
    Query(String),

    #[error("invalid data type")]
    InvalidType,

    #[error("invalid timestamp value")]
    InvalidTimestamp,
}

#[derive(Debug, Clone)]
pub struct CodeIndexingCheckpoint {
    pub traversal_path: String,
    pub project_id: i64,
    pub branch: String,
    pub last_task_id: i64,
    pub last_commit: Option<String>,
    pub indexed_at: DateTime<Utc>,
}

#[async_trait]
pub trait CodeCheckpointStore: Send + Sync {
    async fn get_checkpoint(
        &self,
        traversal_path: &str,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<CodeIndexingCheckpoint>, CheckpointError>;

    async fn set_checkpoint(
        &self,
        checkpoint: &CodeIndexingCheckpoint,
    ) -> Result<(), CheckpointError>;
}

pub(crate) type CheckpointClient = Arc<ArrowClickHouseClient>;

pub struct ClickHouseCodeCheckpointStore {
    client: CheckpointClient,
}

impl ClickHouseCodeCheckpointStore {
    pub fn new(client: CheckpointClient) -> Self {
        Self { client }
    }

    fn extract_checkpoint(
        batches: Vec<RecordBatch>,
        traversal_path: &str,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<CodeIndexingCheckpoint>, CheckpointError> {
        let batch = match batches.into_iter().next() {
            Some(b) if b.num_rows() > 0 => b,
            _ => return Ok(None),
        };

        let last_task_id_col: &Int64Array =
            ArrowUtils::get_column_by_index(&batch, 0).ok_or(CheckpointError::InvalidType)?;

        let last_commit_col: &StringArray =
            ArrowUtils::get_column_by_index(&batch, 1).ok_or(CheckpointError::InvalidType)?;

        let indexed_at_col: &TimestampMicrosecondArray =
            ArrowUtils::get_column_by_index(&batch, 2).ok_or(CheckpointError::InvalidType)?;

        if last_task_id_col.is_null(0) {
            return Ok(None);
        }

        let last_task_id = last_task_id_col.value(0);
        let last_commit = if last_commit_col.is_null(0) {
            None
        } else {
            Some(last_commit_col.value(0).to_string())
        };
        let indexed_at_micros = indexed_at_col.value(0);
        let indexed_at = Utc
            .timestamp_micros(indexed_at_micros)
            .single()
            .ok_or(CheckpointError::InvalidTimestamp)?;

        Ok(Some(CodeIndexingCheckpoint {
            traversal_path: traversal_path.to_string(),
            project_id,
            branch: branch.to_string(),
            last_task_id,
            last_commit,
            indexed_at,
        }))
    }
}

#[async_trait]
impl CodeCheckpointStore for ClickHouseCodeCheckpointStore {
    async fn get_checkpoint(
        &self,
        traversal_path: &str,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<CodeIndexingCheckpoint>, CheckpointError> {
        let table = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let query = format!(
            r#"
            SELECT
                argMax(last_task_id, _version) as last_task_id,
                argMax(last_commit, _version) as last_commit,
                argMax(indexed_at, _version) as indexed_at
            FROM {table}
            WHERE traversal_path = {{traversal_path:String}}
              AND project_id = {{project_id:Int64}}
              AND branch = {{branch:String}}
            HAVING count() > 0
        "#
        );

        let batches = self
            .client
            .query(&query)
            .param("traversal_path", traversal_path)
            .param("project_id", project_id)
            .param("branch", branch)
            .fetch_arrow()
            .await
            .map_err(|e| CheckpointError::Query(e.to_string()))?;

        Self::extract_checkpoint(batches, traversal_path, project_id, branch)
    }

    async fn set_checkpoint(
        &self,
        checkpoint: &CodeIndexingCheckpoint,
    ) -> Result<(), CheckpointError> {
        let table = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let formatted_timestamp = checkpoint.indexed_at.format(TIMESTAMP_FORMAT).to_string();

        self.client
            .query(&format!(
                r#"
                INSERT INTO {table}
                (traversal_path, project_id, branch, last_task_id, last_commit, indexed_at)
                VALUES ({{traversal_path:String}}, {{project_id:Int64}}, {{branch:String}}, {{last_task_id:Int64}}, {{last_commit:Nullable(String)}}, {{indexed_at:String}})
            "#
            ))
            .param("traversal_path", &checkpoint.traversal_path)
            .param("project_id", checkpoint.project_id)
            .param("branch", &checkpoint.branch)
            .param("last_task_id", checkpoint.last_task_id)
            .param("last_commit", &checkpoint.last_commit)
            .param("indexed_at", formatted_timestamp)
            .execute()
            .await
            .map_err(|e| CheckpointError::Query(e.to_string()))?;

        Ok(())
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    #[allow(dead_code)]
    pub struct MockCodeCheckpointStore {
        checkpoints: Mutex<HashMap<(String, i64, String), CodeIndexingCheckpoint>>,
    }

    #[allow(dead_code)]
    impl MockCodeCheckpointStore {
        pub fn new() -> Self {
            Self {
                checkpoints: Mutex::new(HashMap::new()),
            }
        }
    }

    impl Default for MockCodeCheckpointStore {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl CodeCheckpointStore for MockCodeCheckpointStore {
        async fn get_checkpoint(
            &self,
            traversal_path: &str,
            project_id: i64,
            branch: &str,
        ) -> Result<Option<CodeIndexingCheckpoint>, CheckpointError> {
            let checkpoints = self.checkpoints.lock();
            Ok(checkpoints
                .get(&(traversal_path.to_string(), project_id, branch.to_string()))
                .cloned())
        }

        async fn set_checkpoint(
            &self,
            checkpoint: &CodeIndexingCheckpoint,
        ) -> Result<(), CheckpointError> {
            let mut checkpoints = self.checkpoints.lock();
            checkpoints.insert(
                (
                    checkpoint.traversal_path.clone(),
                    checkpoint.project_id,
                    checkpoint.branch.clone(),
                ),
                checkpoint.clone(),
            );
            Ok(())
        }
    }
}
