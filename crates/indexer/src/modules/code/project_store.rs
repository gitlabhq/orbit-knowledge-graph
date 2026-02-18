//! Project store for validating project existence and retrieving metadata.

use std::sync::Arc;

use crate::clickhouse::ArrowClickHouseClient;
use arrow::array::{Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ProjectStoreError {
    #[error("query failed: {0}")]
    Query(String),

    #[error("invalid data type")]
    InvalidType,
}

/// Information about a project from the knowledge graph.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ProjectInfo {
    pub project_id: i64,
    pub traversal_path: String,
    pub full_path: String,
}

/// Trait for querying project information.
#[async_trait]
pub trait ProjectStore: Send + Sync {
    /// Get project information by ID.
    /// Returns None if the project doesn't exist.
    async fn get_project(&self, project_id: i64) -> Result<Option<ProjectInfo>, ProjectStoreError>;
}

pub(crate) type ProjectClient = Arc<ArrowClickHouseClient>;

pub struct ClickHouseProjectStore {
    client: ProjectClient,
}

impl ClickHouseProjectStore {
    pub fn new(client: ProjectClient) -> Self {
        Self { client }
    }

    fn extract_project(
        batches: Vec<RecordBatch>,
    ) -> Result<Option<ProjectInfo>, ProjectStoreError> {
        let batch = match batches.into_iter().next() {
            Some(b) if b.num_rows() > 0 => b,
            _ => return Ok(None),
        };

        let project_id_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(ProjectStoreError::InvalidType)?;

        let traversal_path_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(ProjectStoreError::InvalidType)?;

        let full_path_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(ProjectStoreError::InvalidType)?;

        if project_id_col.is_null(0) {
            return Ok(None);
        }

        Ok(Some(ProjectInfo {
            project_id: project_id_col.value(0),
            traversal_path: traversal_path_col.value(0).to_string(),
            full_path: full_path_col.value(0).to_string(),
        }))
    }
}

#[async_trait]
impl ProjectStore for ClickHouseProjectStore {
    async fn get_project(&self, project_id: i64) -> Result<Option<ProjectInfo>, ProjectStoreError> {
        let query = r#"
            SELECT
                id,
                argMax(traversal_path, _version) as traversal_path,
                argMax(full_path, _version) as full_path
            FROM gl_project
            WHERE id = {project_id:Int64}
            GROUP BY id
        "#;

        let batches = self
            .client
            .query(query)
            .param("project_id", project_id)
            .fetch_arrow()
            .await
            .map_err(|e| ProjectStoreError::Query(e.to_string()))?;

        Self::extract_project(batches)
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;

    #[allow(dead_code)]
    pub struct MockProjectStore {
        pub projects: Mutex<HashMap<i64, ProjectInfo>>,
    }

    #[allow(dead_code)]
    impl MockProjectStore {
        pub fn new() -> Self {
            Self {
                projects: Mutex::new(HashMap::new()),
            }
        }
    }

    impl Default for MockProjectStore {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl ProjectStore for MockProjectStore {
        async fn get_project(
            &self,
            project_id: i64,
        ) -> Result<Option<ProjectInfo>, ProjectStoreError> {
            let projects = self.projects.lock();
            Ok(projects.get(&project_id).cloned())
        }
    }
}
