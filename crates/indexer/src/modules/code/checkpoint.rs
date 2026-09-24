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
    pub indexed_at: Option<DateTime<Utc>>,
    pub attempts: i64,
}

impl CodeCheckpoint {
    pub fn new(traversal_path: TraversalPath, project_id: i64, branch: &str) -> Self {
        Self {
            traversal_path,
            project_id,
            branch: branch.to_string(),
            last_task_id: 0,
            last_commit: None,
            indexed_at: None,
            attempts: 0,
        }
    }

    pub fn is_indexed(&self) -> bool {
        self.indexed_at.is_some()
    }

    pub fn start_attempt(&mut self) {
        self.attempts += 1;
    }

    pub fn complete(&mut self, task_id: i64, commit: Option<String>, indexed_at: DateTime<Utc>) {
        self.last_task_id = task_id;
        self.last_commit = commit;
        self.indexed_at = Some(indexed_at);
    }
}

#[async_trait]
pub trait CodeCheckpointStore: Send + Sync {
    async fn load(
        &self,
        traversal_path: &TraversalPath,
        project_id: i64,
        branch: &str,
    ) -> Result<Option<CodeCheckpoint>, CheckpointError>;

    async fn save(&self, checkpoint: &CodeCheckpoint) -> Result<(), CheckpointError>;

    async fn save_started(&self, checkpoint: &mut CodeCheckpoint) -> Result<(), CheckpointError> {
        if checkpoint.is_indexed() {
            return Ok(());
        }
        checkpoint.start_attempt();
        self.save(checkpoint).await
    }
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

        let attempts_col: &Int64Array =
            ArrowUtils::get_column_by_index(&batch, 3).ok_or(CheckpointError::InvalidType)?;

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
        let indexed_at = if indexed_at_col.is_null(0) {
            None
        } else {
            let indexed_at = Utc
                .timestamp_micros(indexed_at_col.value(0))
                .single()
                .ok_or(CheckpointError::InvalidTimestamp)?;
            Some(indexed_at)
        };

        Ok(Some(CodeCheckpoint {
            traversal_path: traversal_path.clone(),
            project_id,
            branch: branch.to_string(),
            last_task_id,
            last_commit,
            indexed_at,
            attempts: attempts_col.value(0),
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
                argMax(indexed_at, _version) as indexed_at,
                argMax(attempts, _version) as attempts
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
            .param("traversal_path", traversal_path.as_str())
            .param("project_id", project_id)
            .param("branch", branch)
            .fetch_arrow()
            .await
            .map_err(|e| CheckpointError::Query(e.to_string()))?;

        Self::extract_checkpoint(batches, traversal_path, project_id, branch)
    }

    async fn save(&self, checkpoint: &CodeCheckpoint) -> Result<(), CheckpointError> {
        let table = prefixed_table_name(CODE_INDEXING_CHECKPOINT_TABLE, *SCHEMA_VERSION);
        let formatted_timestamp = checkpoint
            .indexed_at
            .map(|indexed_at| indexed_at.format(TIMESTAMP_FORMAT).to_string());

        self.client
            .insert_query(&format!(
                r#"
                INSERT INTO {table}
                (traversal_path, project_id, branch, last_task_id, last_commit, indexed_at, attempts, is_default_branch, _version)
                VALUES ({{traversal_path:String}}, {{project_id:Int64}}, {{branch:String}}, {{last_task_id:Int64}}, {{last_commit:String}}, {{indexed_at:Nullable(String)}}, {{attempts:Int64}}, true, {{version:UInt64}})
            "#
            ))
            .param("traversal_path", checkpoint.traversal_path.as_str())
            .param("project_id", checkpoint.project_id)
            .param("branch", &checkpoint.branch)
            .param("last_task_id", checkpoint.last_task_id)
            .param("last_commit", checkpoint.last_commit.as_deref().unwrap_or_default())
            .param("indexed_at", formatted_timestamp)
            .param("attempts", checkpoint.attempts)
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

    pub struct MockCodeCheckpointStore {
        checkpoints: Mutex<HashMap<(TraversalPath, i64, String), CodeCheckpoint>>,
    }

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
        async fn load(
            &self,
            traversal_path: &TraversalPath,
            project_id: i64,
            branch: &str,
        ) -> Result<Option<CodeCheckpoint>, CheckpointError> {
            let checkpoints = self.checkpoints.lock();
            Ok(checkpoints
                .get(&(traversal_path.clone(), project_id, branch.to_string()))
                .cloned())
        }

        async fn save(&self, checkpoint: &CodeCheckpoint) -> Result<(), CheckpointError> {
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
