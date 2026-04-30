use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use thiserror::Error;

use arrow::record_batch::RecordBatch;

use crate::clickhouse::ArrowClickHouseClient;
use crate::schema::version::{SCHEMA_VERSION, prefixed_table_name};
use clickhouse_client::FromArrowColumn;

use super::lower::{self, DeletionStatement};
use crate::checkpoint::namespace_position_key;

// Datalake tables (siphon_*) are never prefixed — only graph tables are.
const IS_NAMESPACE_STILL_DELETED: &str = r#"
SELECT argMax(_siphon_deleted, _siphon_replicated_at) AS is_deleted
FROM siphon_knowledge_graph_enabled_namespaces
WHERE root_namespace_id = {namespace_id:Int64}
"#;

// Reads `traversal_path` directly from the enabled-namespaces table
// (gitlab-org/gitlab!232941) instead of joining `siphon_namespaces` and
// reconstructing the path with CONCAT.
const DELETED_NAMESPACES_QUERY: &str = r#"
SELECT
    root_namespace_id AS namespace_id,
    traversal_path
FROM siphon_knowledge_graph_enabled_namespaces
WHERE _siphon_deleted = true
  AND traversal_path != ''
  AND _siphon_replicated_at > {last_watermark:String}
  AND _siphon_replicated_at <= {watermark:String}
"#;

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
    datalake_breaker: Option<circuit_breaker::CircuitBreaker>,
    graph_breaker: Option<circuit_breaker::CircuitBreaker>,
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
            datalake_breaker: None,
            graph_breaker: None,
        }
    }

    pub fn with_circuit_breakers(
        mut self,
        datalake_breaker: circuit_breaker::CircuitBreaker,
        graph_breaker: circuit_breaker::CircuitBreaker,
    ) -> Self {
        self.datalake_breaker = Some(datalake_breaker);
        self.graph_breaker = Some(graph_breaker);
        self
    }

    async fn guarded_datalake<F, Fut, T>(&self, f: F) -> Result<T, NamespaceDeletionStoreError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, NamespaceDeletionStoreError>>,
    {
        let Some(breaker) = &self.datalake_breaker else {
            return f().await;
        };
        breaker.call(f).await.map_err(|e| match e {
            circuit_breaker::CircuitBreakerError::Open { service } => {
                NamespaceDeletionStoreError::Query(format!("circuit breaker open for {service}"))
            }
            circuit_breaker::CircuitBreakerError::Inner(inner) => inner,
        })
    }

    async fn guarded_graph<F, Fut, T>(&self, f: F) -> Result<T, NamespaceDeletionStoreError>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<T, NamespaceDeletionStoreError>>,
    {
        let Some(breaker) = &self.graph_breaker else {
            return f().await;
        };
        breaker.call(f).await.map_err(|e| match e {
            circuit_breaker::CircuitBreakerError::Open { service } => {
                NamespaceDeletionStoreError::Query(format!("circuit breaker open for {service}"))
            }
            circuit_breaker::CircuitBreakerError::Inner(inner) => inner,
        })
    }
}

#[async_trait]
impl NamespaceDeletionStore for ClickHouseNamespaceDeletionStore {
    async fn is_namespace_still_deleted(
        &self,
        namespace_id: i64,
    ) -> Result<bool, NamespaceDeletionStoreError> {
        self.guarded_datalake(|| async {
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
        })
        .await
    }

    async fn delete_namespace_checkpoints(
        &self,
        traversal_path: &str,
        namespace_id: i64,
    ) -> Result<(), NamespaceDeletionStoreError> {
        self.guarded_graph(|| async {
            let key_prefix = format!("{}.", namespace_position_key(namespace_id));

            self.graph
                .query(&delete_sdlc_checkpoints_sql())
                .param("key_prefix", key_prefix)
                .execute()
                .await
                .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;

            self.graph
                .query(&delete_code_checkpoints_sql())
                .param("traversal_path", traversal_path)
                .execute()
                .await
                .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))
        })
        .await
    }

    async fn delete_namespace_data(&self, traversal_path: &str) -> Vec<TableDeletionOutcome> {
        let mut outcomes = Vec::with_capacity(self.deletion_statements.len());

        for statement in &self.deletion_statements {
            let started_at = Instant::now();

            let error = if self
                .graph_breaker
                .as_ref()
                .is_some_and(|b| !b.is_available())
            {
                Some("circuit breaker open for clickhouse_graph".to_string())
            } else {
                self.graph
                    .query(&statement.sql)
                    .param("traversal_path", traversal_path)
                    .execute()
                    .await
                    .err()
                    .map(|e| e.to_string())
            };

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
        self.guarded_graph(|| async {
            self.graph
                .query(&mark_deletion_complete_sql())
                .param("namespace_id", namespace_id)
                .param("traversal_path", traversal_path)
                .execute()
                .await
                .map_err(|error| NamespaceDeletionStoreError::MarkComplete {
                    namespace_id,
                    reason: error.to_string(),
                })
        })
        .await
    }

    async fn find_newly_deleted_namespaces(
        &self,
        last_watermark: &str,
        watermark: &str,
    ) -> Result<Vec<NamespaceScheduleEntry>, NamespaceDeletionStoreError> {
        self.guarded_datalake(|| async {
            let batches = self
                .datalake
                .query(DELETED_NAMESPACES_QUERY)
                .param("last_watermark", last_watermark)
                .param("watermark", watermark)
                .fetch_arrow()
                .await
                .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;

            extract_schedule_entries(&batches)
        })
        .await
    }

    async fn schedule_deletion(
        &self,
        namespace_id: i64,
        traversal_path: &str,
        scheduled_deletion_date: &str,
    ) -> Result<(), NamespaceDeletionStoreError> {
        self.guarded_graph(|| async {
            self.graph
                .query(&schedule_deletion_insert_sql())
                .param("namespace_id", namespace_id)
                .param("traversal_path", traversal_path)
                .param("scheduled_deletion_date", scheduled_deletion_date)
                .execute()
                .await
                .map_err(|error| NamespaceDeletionStoreError::ScheduleInsert {
                    namespace_id,
                    reason: error.to_string(),
                })
        })
        .await
    }

    async fn find_due_deletions(
        &self,
    ) -> Result<Vec<NamespaceScheduleEntry>, NamespaceDeletionStoreError> {
        self.guarded_graph(|| async {
            let batches = self
                .graph
                .query(&due_namespaces_query_sql())
                .fetch_arrow()
                .await
                .map_err(|e| NamespaceDeletionStoreError::Query(e.to_string()))?;

            extract_schedule_entries(&batches)
        })
        .await
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
