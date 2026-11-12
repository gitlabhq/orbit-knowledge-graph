//! Executors run the requests of compiled stages, one per connection. Production registers
//! ClickHouse, the local CLI DuckDB; code and live connections run only behind test fakes.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use compiler::dag::Request;
use compiler::orbit_authorization::Key;
use compiler::remote::RowCheck;

use crate::types::QueryExecution;

/// The rows one request returned, and the executions a log records for it.
#[derive(Debug, Default)]
pub struct Executed {
    pub batches: Vec<RecordBatch>,
    pub log: Vec<QueryExecution>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ExecutorError(pub String);

#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(&self, request: &Request) -> Result<Executed, ExecutorError>;
}

/// The executor registered for each connection a deployment serves.
#[derive(Clone, Default)]
pub struct Executors {
    by_connection: HashMap<String, Arc<dyn Executor>>,
}

impl Executors {
    #[must_use]
    pub fn with(mut self, connection: impl Into<String>, executor: Arc<dyn Executor>) -> Self {
        self.by_connection.insert(connection.into(), executor);
        self
    }

    pub fn get(&self, connection: &str) -> Result<&Arc<dyn Executor>, ExecutorError> {
        self.by_connection
            .get(connection)
            .ok_or_else(|| ExecutorError(format!("no executor serves connection {connection}")))
    }
}

/// Answers the row checks a remote stage's rows go through, before later stages read them.
#[async_trait]
pub trait RowChecks: Send + Sync {
    async fn allowed(
        &self,
        check: &RowCheck,
        keys: Vec<Key>,
    ) -> Result<BTreeSet<Key>, ExecutorError>;
}

/// A deployment that serves no remote connection runs no remote row check.
pub struct NoRemoteChecks;

#[async_trait]
impl RowChecks for NoRemoteChecks {
    async fn allowed(&self, check: &RowCheck, _: Vec<Key>) -> Result<BTreeSet<Key>, ExecutorError> {
        Err(ExecutorError(format!(
            "no provider answers {} checks in this deployment",
            check.permission
        )))
    }
}
