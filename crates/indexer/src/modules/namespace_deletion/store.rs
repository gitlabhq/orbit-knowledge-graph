use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use thiserror::Error;

use arrow::record_batch::RecordBatch;

use crate::clickhouse::ArrowClickHouseClient;
use clickhouse_client::FromArrowColumn;

use super::lower::{self, DeletionStatement};
use crate::checkpoint::namespace_position_key;

const IS_NAMESPACE_STILL_DELETED: &str = r#"
SELECT argMax(_siphon_deleted, _siphon_replicated_at) AS is_deleted
FROM siphon_knowledge_graph_enabled_namespaces
WHERE root_namespace_id = {namespace_id:Int64}
"#;

const MARK_DELETION_COMPLETE: &str = r#"
INSERT INTO namespace_deletion_schedule (namespace_id, traversal_path, scheduled_deletion_date, _deleted)
SELECT
    namespace_id,
    traversal_path,
    argMax(scheduled_deletion_date, _version) AS scheduled_deletion_date,
    true
FROM namespace_deletion_schedule
WHERE namespace_id = {namespace_id:Int64}
  AND traversal_path = {traversal_path:String}
GROUP BY namespace_id, traversal_path
HAVING argMax(_deleted, _version) = false
"#;

const DELETED_NAMESPACES_QUERY: &str = r#"
SELECT
    enabled.root_namespace_id AS namespace_id,
    CONCAT(toString(namespaces.organization_id), '/', toString(enabled.root_namespace_id), '/') AS traversal_path
FROM siphon_knowledge_graph_enabled_namespaces AS enabled
INNER JOIN siphon_namespaces AS namespaces
    ON enabled.root_namespace_id = namespaces.id
WHERE enabled._siphon_deleted = true
  AND enabled._siphon_replicated_at > {last_watermark:String}
  AND enabled._siphon_replicated_at <= {watermark:String}
"#;

const SCHEDULE_DELETION_INSERT: &str = r#"
INSERT INTO namespace_deletion_schedule (namespace_id, traversal_path, scheduled_deletion_date)
VALUES ({namespace_id:Int64}, {traversal_path:String}, {scheduled_deletion_date:String})
"#;

const DELETE_SDLC_CHECKPOINTS: &str = r#"
INSERT INTO checkpoint (key, watermark, cursor_values, _deleted)
SELECT key, argMax(watermark, _version), argMax(cursor_values, _version), true
FROM checkpoint
WHERE startsWith(key, {key_prefix:String})
GROUP BY key
HAVING argMax(_deleted, _version) = false
"#;

const DELETE_CODE_CHECKPOINTS: &str = r#"
INSERT INTO code_indexing_checkpoint (traversal_path, project_id, branch, last_event_id, last_commit, indexed_at, _deleted)
SELECT
    traversal_path,
    project_id,
    branch,
    argMax(last_event_id, _version),
    argMax(last_commit, _version),
    argMax(indexed_at, _version),
    true
FROM code_indexing_checkpoint
WHERE startsWith(traversal_path, {traversal_path:String})
GROUP BY traversal_path, project_id, branch
HAVING argMax(_deleted, _version) = false
"#;

const DUE_NAMESPACES_QUERY: &str = r#"
SELECT namespace_id, traversal_path
FROM namespace_deletion_schedule
GROUP BY namespace_id, traversal_path
HAVING argMax(_deleted, _version) = false
  AND argMax(scheduled_deletion_date, _version) <= now()
"#;

#[derive(Clone)]
pub struct TableDeletionOutcome {
    pub table: String,
    pub duration_seconds: f64,
    pub error: Option<String>,
}

#[derive(Clone)]
pub struct NamespaceScheduleEntry {
    pub namespace_id: i64,
    pub traversal_path: String,
}

#[derive(Debug, Error)]
pub enum NamespaceDeletionStoreError {
    #[error("mark complete for namespace {namespace_id}: {reason}")]
    MarkComplete { namespace_id: i64, reason: String },
    #[error("query failed: {0}")]
    Query(String),
    #[error("insert failed for namespace {namespace_id}: {reason}")]
    ScheduleInsert { namespace_id: i64, reason: String },
}

#[async_trait]
pub trait NamespaceDeletionStore: Send + Sync {
    async fn is_namespace_still_deleted(
        &self,
        namespace_id: i64,
    ) -> Result<bool, NamespaceDeletionStoreError>;

    async fn delete_namespace_data(&self, traversal_path: &str) -> Vec<TableDeletionOutcome>;

    async fn delete_namespace_checkpoints(
        &self,
        traversal_path: &str,
        namespace_id: i64,
    ) -> Result<(), NamespaceDeletionStoreError>;

    async fn mark_deletion_complete(
        &self,
        namespace_id: i64,
        traversal_path: &str,
    ) -> Result<(), NamespaceDeletionStoreError>;

    async fn find_newly_deleted_namespaces(
        &self,
        last_watermark: &str,
        watermark: &str,
    ) -> Result<Vec<NamespaceScheduleEntry>, NamespaceDeletionStoreError>;

    async fn schedule_deletion(
        &self,
        namespace_id: i64,
        traversal_path: &str,
        scheduled_deletion_date: &str,
    ) -> Result<(), NamespaceDeletionStoreError>;

    async fn find_due_deletions(
        &self,
    ) -> Result<Vec<NamespaceScheduleEntry>, NamespaceDeletionStoreError>;
}

pub struct ClickHouseNamespaceDeletionStore {
    datalake: Arc<ArrowClickHouseClient>,
    graph: Arc<ArrowClickHouseClient>,
    deletion_statements: Vec<DeletionStatement>,
}

impl ClickHouseNamespaceDeletionStore {
    pub fn new(
        datalake: Arc<ArrowClickHouseClient>,
        graph: Arc<ArrowClickHouseClient>,
        ontology: &ontology::Ontology,
    ) -> Self {
        let deletion_statements = lower::build_deletion_statements(ontology);
        Self {
            datalake,
            graph,
            deletion_statements,
        }
    }
}

#[async_trait]
impl NamespaceDeletionStore for ClickHouseNamespaceDeletionStore {
    async fn is_namespace_still_deleted(
        &self,
        namespace_id: i64,
    ) -> Result<bool, NamespaceDeletionStoreError> {
        let batches = self
            .datalake
            .query(IS_NAMESPACE_STILL_DELETED)
            .param("namespace_id", namespace_id)
            .fetch_arrow()
            .await
            .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;

        let deleted_flags = bool::extract_column(&batches, 0)
            .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;

        Ok(deleted_flags.first().copied().unwrap_or(true))
    }

    async fn delete_namespace_checkpoints(
        &self,
        traversal_path: &str,
        namespace_id: i64,
    ) -> Result<(), NamespaceDeletionStoreError> {
        let key_prefix = format!("{}.", namespace_position_key(namespace_id));

        self.graph
            .query(DELETE_SDLC_CHECKPOINTS)
            .param("key_prefix", key_prefix)
            .execute()
            .await
            .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;

        self.graph
            .query(DELETE_CODE_CHECKPOINTS)
            .param("traversal_path", traversal_path)
            .execute()
            .await
            .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))
    }

    async fn delete_namespace_data(&self, traversal_path: &str) -> Vec<TableDeletionOutcome> {
        let mut outcomes = Vec::with_capacity(self.deletion_statements.len());

        for statement in &self.deletion_statements {
            let started_at = Instant::now();

            let error = self
                .graph
                .query(&statement.sql)
                .param("traversal_path", traversal_path)
                .execute()
                .await
                .err()
                .map(|e| e.to_string());

            outcomes.push(TableDeletionOutcome {
                table: statement.table.clone(),
                duration_seconds: started_at.elapsed().as_secs_f64(),
                error,
            });
        }

        outcomes
    }

    async fn mark_deletion_complete(
        &self,
        namespace_id: i64,
        traversal_path: &str,
    ) -> Result<(), NamespaceDeletionStoreError> {
        self.graph
            .query(MARK_DELETION_COMPLETE)
            .param("namespace_id", namespace_id)
            .param("traversal_path", traversal_path)
            .execute()
            .await
            .map_err(|error| NamespaceDeletionStoreError::MarkComplete {
                namespace_id,
                reason: error.to_string(),
            })
    }

    async fn find_newly_deleted_namespaces(
        &self,
        last_watermark: &str,
        watermark: &str,
    ) -> Result<Vec<NamespaceScheduleEntry>, NamespaceDeletionStoreError> {
        let batches = self
            .datalake
            .query(DELETED_NAMESPACES_QUERY)
            .param("last_watermark", last_watermark)
            .param("watermark", watermark)
            .fetch_arrow()
            .await
            .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;

        extract_schedule_entries(&batches)
    }

    async fn schedule_deletion(
        &self,
        namespace_id: i64,
        traversal_path: &str,
        scheduled_deletion_date: &str,
    ) -> Result<(), NamespaceDeletionStoreError> {
        self.graph
            .query(SCHEDULE_DELETION_INSERT)
            .param("namespace_id", namespace_id)
            .param("traversal_path", traversal_path)
            .param("scheduled_deletion_date", scheduled_deletion_date)
            .execute()
            .await
            .map_err(|error| NamespaceDeletionStoreError::ScheduleInsert {
                namespace_id,
                reason: error.to_string(),
            })
    }

    async fn find_due_deletions(
        &self,
    ) -> Result<Vec<NamespaceScheduleEntry>, NamespaceDeletionStoreError> {
        let batches = self
            .graph
            .query(DUE_NAMESPACES_QUERY)
            .fetch_arrow()
            .await
            .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;

        extract_schedule_entries(&batches)
    }
}

fn extract_schedule_entries(
    batches: &[RecordBatch],
) -> Result<Vec<NamespaceScheduleEntry>, NamespaceDeletionStoreError> {
    let namespace_ids = i64::extract_column(batches, 0)
        .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;
    let traversal_paths = String::extract_column(batches, 1)
        .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;

    Ok(namespace_ids
        .into_iter()
        .zip(traversal_paths)
        .map(|(namespace_id, traversal_path)| NamespaceScheduleEntry {
            namespace_id,
            traversal_path,
        })
        .collect())
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use parking_lot::Mutex;

    pub struct MockNamespaceDeletionStore {
        delete_calls: Mutex<Vec<String>>,
        delete_checkpoint_calls: Mutex<Vec<i64>>,
        mark_complete_calls: Mutex<Vec<(i64, String)>>,
        schedule_calls: Mutex<Vec<(i64, String, String)>>,
        deletion_outcomes: Vec<TableDeletionOutcome>,
        newly_deleted: Vec<NamespaceScheduleEntry>,
        due_deletions: Vec<NamespaceScheduleEntry>,
        namespace_still_deleted: bool,
        fail_mark_complete: bool,
        fail_schedule: bool,
    }

    pub fn ok_outcome(table: &str) -> TableDeletionOutcome {
        TableDeletionOutcome {
            table: table.to_string(),
            duration_seconds: 0.001,
            error: None,
        }
    }

    pub fn failed_outcome(table: &str) -> TableDeletionOutcome {
        TableDeletionOutcome {
            table: table.to_string(),
            duration_seconds: 0.001,
            error: Some("simulated failure".to_string()),
        }
    }

    impl MockNamespaceDeletionStore {
        pub fn new() -> Self {
            Self {
                delete_calls: Mutex::new(Vec::new()),
                delete_checkpoint_calls: Mutex::new(Vec::new()),
                mark_complete_calls: Mutex::new(Vec::new()),
                schedule_calls: Mutex::new(Vec::new()),
                deletion_outcomes: vec![ok_outcome("gl_project")],
                newly_deleted: Vec::new(),
                due_deletions: Vec::new(),
                namespace_still_deleted: true,
                fail_mark_complete: false,
                fail_schedule: false,
            }
        }

        pub fn with_deletion_outcomes(mut self, outcomes: Vec<TableDeletionOutcome>) -> Self {
            self.deletion_outcomes = outcomes;
            self
        }

        pub fn namespace_re_enabled(mut self) -> Self {
            self.namespace_still_deleted = false;
            self
        }

        pub fn failing_mark_complete(mut self) -> Self {
            self.fail_mark_complete = true;
            self
        }

        pub fn failing_schedule(mut self) -> Self {
            self.fail_schedule = true;
            self
        }

        pub fn with_newly_deleted(mut self, entries: Vec<NamespaceScheduleEntry>) -> Self {
            self.newly_deleted = entries;
            self
        }

        pub fn with_due_deletions(mut self, entries: Vec<NamespaceScheduleEntry>) -> Self {
            self.due_deletions = entries;
            self
        }

        pub fn delete_calls(&self) -> Vec<String> {
            self.delete_calls.lock().clone()
        }

        pub fn mark_complete_calls(&self) -> Vec<(i64, String)> {
            self.mark_complete_calls.lock().clone()
        }

        pub fn schedule_calls(&self) -> Vec<(i64, String, String)> {
            self.schedule_calls.lock().clone()
        }

        pub fn delete_checkpoint_calls(&self) -> Vec<i64> {
            self.delete_checkpoint_calls.lock().clone()
        }
    }

    #[async_trait]
    impl NamespaceDeletionStore for MockNamespaceDeletionStore {
        async fn is_namespace_still_deleted(
            &self,
            _namespace_id: i64,
        ) -> Result<bool, NamespaceDeletionStoreError> {
            Ok(self.namespace_still_deleted)
        }

        async fn delete_namespace_data(&self, traversal_path: &str) -> Vec<TableDeletionOutcome> {
            self.delete_calls.lock().push(traversal_path.to_string());
            self.deletion_outcomes.clone()
        }

        async fn delete_namespace_checkpoints(
            &self,
            _traversal_path: &str,
            namespace_id: i64,
        ) -> Result<(), NamespaceDeletionStoreError> {
            self.delete_checkpoint_calls.lock().push(namespace_id);
            Ok(())
        }

        async fn mark_deletion_complete(
            &self,
            namespace_id: i64,
            traversal_path: &str,
        ) -> Result<(), NamespaceDeletionStoreError> {
            self.mark_complete_calls
                .lock()
                .push((namespace_id, traversal_path.to_string()));

            if self.fail_mark_complete {
                return Err(NamespaceDeletionStoreError::MarkComplete {
                    namespace_id,
                    reason: "simulated failure".to_string(),
                });
            }

            Ok(())
        }

        async fn find_newly_deleted_namespaces(
            &self,
            _last_watermark: &str,
            _watermark: &str,
        ) -> Result<Vec<NamespaceScheduleEntry>, NamespaceDeletionStoreError> {
            Ok(self.newly_deleted.clone())
        }

        async fn schedule_deletion(
            &self,
            namespace_id: i64,
            traversal_path: &str,
            scheduled_deletion_date: &str,
        ) -> Result<(), NamespaceDeletionStoreError> {
            self.schedule_calls.lock().push((
                namespace_id,
                traversal_path.to_string(),
                scheduled_deletion_date.to_string(),
            ));

            if self.fail_schedule {
                return Err(NamespaceDeletionStoreError::ScheduleInsert {
                    namespace_id,
                    reason: "simulated failure".to_string(),
                });
            }

            Ok(())
        }

        async fn find_due_deletions(
            &self,
        ) -> Result<Vec<NamespaceScheduleEntry>, NamespaceDeletionStoreError> {
            Ok(self.due_deletions.clone())
        }
    }
}
