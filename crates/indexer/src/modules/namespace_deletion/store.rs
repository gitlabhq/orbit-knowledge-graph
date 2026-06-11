use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use thiserror::Error;

use arrow::record_batch::RecordBatch;

use crate::clickhouse::ArrowClickHouseClient;
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use clickhouse_client::FromArrowColumn;

use const_format::concatcp;

use super::lower::{self, DeletionStatement};
use crate::checkpoint::namespace_position_key;

const WM: &str = ontology::constants::SIPHON_WATERMARK_COLUMN;

// Datalake tables (siphon_*) are never prefixed — only graph tables are.
const IS_NAMESPACE_STILL_DELETED: &str = concatcp!(
    r#"
SELECT argMax(_siphon_deleted, "#,
    WM,
    r#") AS is_deleted
FROM siphon_knowledge_graph_enabled_namespaces
WHERE root_namespace_id = {namespace_id:Int64}
"#
);

const ENABLED_NAMESPACE_ROOTS_QUERY: &str = r#"
SELECT traversal_path
FROM siphon_knowledge_graph_enabled_namespaces
WHERE _siphon_deleted = false
  AND traversal_path != ''
"#;

const CURRENT_ROUTES_UNDER_ROOT: &str = r#"
SELECT DISTINCT traversal_path FROM project_namespace_traversal_paths FINAL
WHERE deleted = false AND startsWith(traversal_path, {traversal_path:String})
UNION DISTINCT
SELECT DISTINCT traversal_path FROM namespace_traversal_paths FINAL
WHERE deleted = false AND startsWith(traversal_path, {traversal_path:String})
"#;

// Reads `traversal_path` directly from the enabled-namespaces table
// (gitlab-org/gitlab!232941) instead of joining `siphon_namespaces` and
// reconstructing the path with CONCAT.
const DELETED_NAMESPACES_QUERY: &str = concatcp!(
    r#"
SELECT
    root_namespace_id AS namespace_id,
    traversal_path,
    toString("#,
    WM,
    r#") AS deleted_at
FROM siphon_knowledge_graph_enabled_namespaces
WHERE _siphon_deleted = true
  AND traversal_path != ''
  AND "#,
    WM,
    r#" > {last_watermark:String}
  AND "#,
    WM,
    r#" <= {watermark:String}
"#
);

fn mark_deletion_complete_sql() -> String {
    let table = prefixed_table_name("namespace_deletion_schedule", *SCHEMA_VERSION);
    format!(
        r#"
INSERT INTO {table} (namespace_id, traversal_path, scheduled_deletion_date, _deleted)
SELECT
    namespace_id,
    traversal_path,
    argMax(scheduled_deletion_date, _version) AS scheduled_deletion_date,
    true
FROM {table}
WHERE namespace_id = {{namespace_id:Int64}}
  AND traversal_path = {{traversal_path:String}}
GROUP BY namespace_id, traversal_path
HAVING argMax(_deleted, _version) = false
"#
    )
}

fn schedule_deletion_insert_sql() -> String {
    let table = prefixed_table_name("namespace_deletion_schedule", *SCHEMA_VERSION);
    format!(
        r#"
INSERT INTO {table} (namespace_id, traversal_path, scheduled_deletion_date)
VALUES ({{namespace_id:Int64}}, {{traversal_path:String}}, {{scheduled_deletion_date:String}})
"#
    )
}

fn delete_sdlc_checkpoints_sql() -> String {
    let table = prefixed_table_name("checkpoint", *SCHEMA_VERSION);
    format!(
        r#"
INSERT INTO {table} (key, watermark, cursor_values, _deleted)
SELECT key, argMax(watermark, _version), argMax(cursor_values, _version), true
FROM {table}
WHERE startsWith(key, {{key_prefix:String}})
GROUP BY key
HAVING argMax(_deleted, _version) = false
"#
    )
}

fn delete_code_checkpoints_sql() -> String {
    let table = prefixed_table_name("code_indexing_checkpoint", *SCHEMA_VERSION);
    format!(
        r#"
INSERT INTO {table} (traversal_path, project_id, branch, last_task_id, last_commit, indexed_at, _deleted)
SELECT
    traversal_path,
    project_id,
    branch,
    argMax(last_task_id, _version),
    argMax(last_commit, _version),
    argMax(indexed_at, _version),
    true
FROM {table}
WHERE startsWith(traversal_path, {{traversal_path:String}})
GROUP BY traversal_path, project_id, branch
HAVING argMax(_deleted, _version) = false
"#
    )
}

fn due_namespaces_query_sql() -> String {
    let table = prefixed_table_name("namespace_deletion_schedule", *SCHEMA_VERSION);
    format!(
        r#"
SELECT namespace_id, traversal_path
FROM {table}
GROUP BY namespace_id, traversal_path
HAVING argMax(_deleted, _version) = false
  AND argMax(scheduled_deletion_date, _version) <= now()
"#
    )
}

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

#[derive(Clone)]
pub struct DeletedNamespaceEntry {
    pub namespace_id: i64,
    pub traversal_path: String,
    pub deleted_at: String,
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

    async fn enabled_namespace_roots(&self) -> Result<Vec<String>, NamespaceDeletionStoreError>;

    async fn reconcile_moved_entities(
        &self,
        root_traversal_path: &str,
    ) -> Vec<TableDeletionOutcome>;

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
    ) -> Result<Vec<DeletedNamespaceEntry>, NamespaceDeletionStoreError>;

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
    reconcile_statements: Vec<DeletionStatement>,
}

impl ClickHouseNamespaceDeletionStore {
    pub fn new(
        datalake: Arc<ArrowClickHouseClient>,
        graph: Arc<ArrowClickHouseClient>,
        ontology: &ontology::Ontology,
    ) -> Self {
        Self {
            datalake,
            graph,
            deletion_statements: lower::build_deletion_statements(ontology),
            reconcile_statements: lower::build_reconcile_statements(ontology),
        }
    }

    async fn tombstone(
        &self,
        statements: &[DeletionStatement],
        traversal_path: &str,
        current_paths: Option<&[String]>,
    ) -> Vec<TableDeletionOutcome> {
        let mut outcomes = Vec::with_capacity(statements.len());

        for statement in statements {
            let started_at = Instant::now();

            let mut query = self
                .graph
                .insert_query(&statement.sql)
                .param("traversal_path", traversal_path);
            if let Some(current_paths) = current_paths {
                query = query.param("current_paths", current_paths);
            }

            let error = query.execute().await.err().map(|e| e.to_string());

            outcomes.push(TableDeletionOutcome {
                table: statement.table.clone(),
                duration_seconds: started_at.elapsed().as_secs_f64(),
                error,
            });
        }

        outcomes
    }

    async fn current_routes_under_root(
        &self,
        root_traversal_path: &str,
    ) -> Result<Vec<String>, NamespaceDeletionStoreError> {
        let batches = self
            .datalake
            .query(CURRENT_ROUTES_UNDER_ROOT)
            .param("traversal_path", root_traversal_path)
            .fetch_arrow()
            .await
            .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;

        String::extract_column(&batches, 0)
            .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))
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
            .insert_query(&delete_sdlc_checkpoints_sql())
            .param("key_prefix", key_prefix)
            .execute()
            .await
            .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;

        self.graph
            .insert_query(&delete_code_checkpoints_sql())
            .param("traversal_path", traversal_path)
            .execute()
            .await
            .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))
    }

    async fn delete_namespace_data(&self, traversal_path: &str) -> Vec<TableDeletionOutcome> {
        self.tombstone(&self.deletion_statements, traversal_path, None)
            .await
    }

    async fn enabled_namespace_roots(&self) -> Result<Vec<String>, NamespaceDeletionStoreError> {
        let batches = self
            .datalake
            .query(ENABLED_NAMESPACE_ROOTS_QUERY)
            .fetch_arrow()
            .await
            .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;

        String::extract_column(&batches, 0)
            .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))
    }

    async fn reconcile_moved_entities(
        &self,
        root_traversal_path: &str,
    ) -> Vec<TableDeletionOutcome> {
        let current_paths = match self.current_routes_under_root(root_traversal_path).await {
            Ok(paths) => paths,
            Err(error) => {
                return vec![TableDeletionOutcome {
                    table: "current_routes".to_string(),
                    duration_seconds: 0.0,
                    error: Some(error.to_string()),
                }];
            }
        };

        self.tombstone(
            &self.reconcile_statements,
            root_traversal_path,
            Some(&current_paths),
        )
        .await
    }

    async fn mark_deletion_complete(
        &self,
        namespace_id: i64,
        traversal_path: &str,
    ) -> Result<(), NamespaceDeletionStoreError> {
        self.graph
            .insert_query(&mark_deletion_complete_sql())
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
    ) -> Result<Vec<DeletedNamespaceEntry>, NamespaceDeletionStoreError> {
        let batches = self
            .datalake
            .query(DELETED_NAMESPACES_QUERY)
            .param("last_watermark", last_watermark)
            .param("watermark", watermark)
            .fetch_arrow()
            .await
            .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;

        extract_deleted_namespace_entries(&batches)
    }

    async fn schedule_deletion(
        &self,
        namespace_id: i64,
        traversal_path: &str,
        scheduled_deletion_date: &str,
    ) -> Result<(), NamespaceDeletionStoreError> {
        self.graph
            .insert_query(&schedule_deletion_insert_sql())
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
            .query(&due_namespaces_query_sql())
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

fn extract_deleted_namespace_entries(
    batches: &[RecordBatch],
) -> Result<Vec<DeletedNamespaceEntry>, NamespaceDeletionStoreError> {
    let namespace_ids = i64::extract_column(batches, 0)
        .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;
    let traversal_paths = String::extract_column(batches, 1)
        .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;
    let deleted_ats = String::extract_column(batches, 2)
        .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;

    Ok(namespace_ids
        .into_iter()
        .zip(traversal_paths)
        .zip(deleted_ats)
        .map(
            |((namespace_id, traversal_path), deleted_at)| DeletedNamespaceEntry {
                namespace_id,
                traversal_path,
                deleted_at,
            },
        )
        .collect())
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use parking_lot::Mutex;

    pub struct MockNamespaceDeletionStore {
        delete_calls: Mutex<Vec<String>>,
        reconcile_calls: Mutex<Vec<String>>,
        delete_checkpoint_calls: Mutex<Vec<i64>>,
        mark_complete_calls: Mutex<Vec<(i64, String)>>,
        schedule_calls: Mutex<Vec<(i64, String, String)>>,
        deletion_outcomes: Vec<TableDeletionOutcome>,
        newly_deleted: Vec<DeletedNamespaceEntry>,
        due_deletions: Vec<NamespaceScheduleEntry>,
        enabled_roots: Vec<String>,
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
                reconcile_calls: Mutex::new(Vec::new()),
                delete_checkpoint_calls: Mutex::new(Vec::new()),
                mark_complete_calls: Mutex::new(Vec::new()),
                schedule_calls: Mutex::new(Vec::new()),
                deletion_outcomes: vec![ok_outcome("gl_project")],
                newly_deleted: Vec::<DeletedNamespaceEntry>::new(),
                due_deletions: Vec::new(),
                enabled_roots: Vec::new(),
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

        pub fn with_newly_deleted(mut self, entries: Vec<DeletedNamespaceEntry>) -> Self {
            self.newly_deleted = entries;
            self
        }

        pub fn with_due_deletions(mut self, entries: Vec<NamespaceScheduleEntry>) -> Self {
            self.due_deletions = entries;
            self
        }

        pub fn with_enabled_roots(mut self, roots: Vec<String>) -> Self {
            self.enabled_roots = roots;
            self
        }

        pub fn delete_calls(&self) -> Vec<String> {
            self.delete_calls.lock().clone()
        }

        pub fn reconcile_calls(&self) -> Vec<String> {
            self.reconcile_calls.lock().clone()
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

        async fn enabled_namespace_roots(
            &self,
        ) -> Result<Vec<String>, NamespaceDeletionStoreError> {
            Ok(self.enabled_roots.clone())
        }

        async fn reconcile_moved_entities(
            &self,
            root_traversal_path: &str,
        ) -> Vec<TableDeletionOutcome> {
            self.reconcile_calls
                .lock()
                .push(root_traversal_path.to_string());
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
        ) -> Result<Vec<DeletedNamespaceEntry>, NamespaceDeletionStoreError> {
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
