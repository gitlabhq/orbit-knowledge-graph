use std::sync::Arc;

use crate::clickhouse::{ArrowClickHouseClient, TIMESTAMP_FORMAT};
use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, TimeZone, Utc};
use orbit_migrations::version::{SCHEMA_VERSION, prefixed_table_name};
use orbit_utils::arrow::ArrowUtils;
use orbit_utils::traversal_path::TraversalPath;
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
pub struct CodeCheckpoint {
    pub traversal_path: TraversalPath,
    pub project_id: i64,
    pub branch: String,
    pub last_task_id: i64,
    pub last_commit: Option<String>,
    pub indexed_at: DateTime<Utc>,
}

#[async_trait]
pub trait CodeCheckpointStore: Send + Sync {
    async fn load(
        &self,
        traversal_path: &TraversalPath,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<CodeCheckpoint>, CheckpointError>;

    async fn save_started(
        &self,
        traversal_path: &TraversalPath,
        project_id: i64,
        branch: &str,
    ) -> Result<(), CheckpointError>;

    async fn save_completed(&self, checkpoint: &CodeCheckpoint) -> Result<(), CheckpointError>;
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
        traversal_path: &TraversalPath,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<CodeCheckpoint>, CheckpointError> {
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
            let v = last_commit_col.value(0).to_string();
            if v.is_empty() { None } else { Some(v) }
        };
        let indexed_at_micros = indexed_at_col.value(0);
        let indexed_at = Utc
            .timestamp_micros(indexed_at_micros)
            .single()
            .ok_or(CheckpointError::InvalidTimestamp)?;

        Ok(Some(CodeCheckpoint {
            traversal_path: traversal_path.clone(),
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
    async fn load(
        &self,
        traversal_path: &TraversalPath,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<CodeCheckpoint>, CheckpointError> {
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
            HAVING indexed_at IS NOT NULL
        "#
        );

        let batches = self
            .client
            .query(&query)
            .param("traversal_path", traversal_path.as_str())
            .param("project_id", project_id)
            .param("branch", branch)
            .fetch_arrow()
            .await
            .map_err(|e| CheckpointError::Query(e.to_string()))?;

        Self::extract_checkpoint(batches, traversal_path, project_id, branch)
    }

    async fn save_started(
        &self,
        traversal_path: &TraversalPath,
        project_id: i64,
        branch: &str,
    ) -> Result<(), CheckpointError> {
        let table = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, *SCHEMA_VERSION);

        self.client
            .insert_query(&format!(
                r#"
                INSERT INTO {table}
                (traversal_path, project_id, branch, last_task_id, indexed_at, attempts, is_default_branch, _version)
                SELECT {{traversal_path:String}}, {{project_id:Int64}}, {{branch:String}}, 0, NULL,
                       argMax(attempts, _version) + 1, true, {{version:UInt64}}
                FROM {table}
                WHERE traversal_path = {{traversal_path:String}}
                  AND project_id = {{project_id:Int64}}
                  AND branch = {{branch:String}}
                HAVING argMax(indexed_at, _version) IS NULL
            "#
            ))
            .param("traversal_path", traversal_path.as_str())
            .param("project_id", project_id)
            .param("branch", branch)
            .param("version", write_version())
            .execute()
            .await
            .map_err(|e| CheckpointError::Query(e.to_string()))
    }

    async fn save_completed(&self, checkpoint: &CodeCheckpoint) -> Result<(), CheckpointError> {
        let table = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let formatted_timestamp = checkpoint.indexed_at.format(TIMESTAMP_FORMAT).to_string();

        self.client
            .insert_query(&format!(
                r#"
                INSERT INTO {table}
                (traversal_path, project_id, branch, last_task_id, last_commit, indexed_at, attempts, is_default_branch, _version)
                SELECT {{traversal_path:String}}, {{project_id:Int64}}, {{branch:String}}, {{last_task_id:Int64}},
                       {{last_commit:String}}, {{indexed_at:String}}, argMax(attempts, _version), true, {{version:UInt64}}
                FROM {table}
                WHERE traversal_path = {{traversal_path:String}}
                  AND project_id = {{project_id:Int64}}
                  AND branch = {{branch:String}}
            "#
            ))
            .param("traversal_path", checkpoint.traversal_path.as_str())
            .param("project_id", checkpoint.project_id)
            .param("branch", &checkpoint.branch)
            .param("last_task_id", checkpoint.last_task_id)
            .param("last_commit", checkpoint.last_commit.as_deref().unwrap_or_default())
            .param("indexed_at", formatted_timestamp)
            .param("version", write_version())
            .execute()
            .await
            .map_err(|e| CheckpointError::Query(e.to_string()))?;

        Ok(())
    }
}

fn write_version() -> u64 {
    Utc::now().timestamp_micros().unsigned_abs()
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    type CheckpointKey = (TraversalPath, i64, String);

    fn checkpoint_key(
        traversal_path: &TraversalPath,
        project_id: i64,
        branch: &str,
    ) -> CheckpointKey {
        (traversal_path.clone(), project_id, branch.to_string())
    }

    pub struct MockCodeCheckpointStore {
        checkpoints: Mutex<HashMap<CheckpointKey, CodeCheckpoint>>,
        attempts: Mutex<HashMap<CheckpointKey, i64>>,
    }

    impl MockCodeCheckpointStore {
        pub fn new() -> Self {
            Self {
                checkpoints: Mutex::new(HashMap::new()),
                attempts: Mutex::new(HashMap::new()),
            }
        }

        pub fn attempts(
            &self,
            traversal_path: &TraversalPath,
            project_id: i64,
            branch: &str,
        ) -> i64 {
            self.attempts
                .lock()
                .get(&checkpoint_key(traversal_path, project_id, branch))
                .copied()
                .unwrap_or_default()
        }
    }

    impl Default for MockCodeCheckpointStore {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl CodeCheckpointStore for MockCodeCheckpointStore {
        async fn load(
            &self,
            traversal_path: &TraversalPath,
            project_id: i64,
            branch: &str,
        ) -> Result<Option<CodeCheckpoint>, CheckpointError> {
            Ok(self
                .checkpoints
                .lock()
                .get(&checkpoint_key(traversal_path, project_id, branch))
                .cloned())
        }

        async fn save_started(
            &self,
            traversal_path: &TraversalPath,
            project_id: i64,
            branch: &str,
        ) -> Result<(), CheckpointError> {
            let key = checkpoint_key(traversal_path, project_id, branch);
            if !self.checkpoints.lock().contains_key(&key) {
                *self.attempts.lock().entry(key).or_default() += 1;
            }
            Ok(())
        }

        async fn save_completed(&self, checkpoint: &CodeCheckpoint) -> Result<(), CheckpointError> {
            self.checkpoints.lock().insert(
                checkpoint_key(
                    &checkpoint.traversal_path,
                    checkpoint.project_id,
                    &checkpoint.branch,
                ),
                checkpoint.clone(),
            );
            Ok(())
        }
    }
}
