//! Executors run the requests of compiled stages, one per connection. Production registers
//! ClickHouse, the local CLI DuckDB; code and live connections run only behind test fakes.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use compiler::dag::{Contract, Request};
use query_data_model::ConnectionId;

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

/// Runs a request. Given a contract, the executor returns its rows in contract order with
/// contract types; without one, as its backend names them.
#[async_trait]
pub trait Executor: Send + Sync {
    async fn execute(
        &self,
        request: &Request,
        contract: Option<&Contract>,
    ) -> Result<Executed, ExecutorError>;
}

/// The executor registered for each connection a deployment serves.
#[derive(Clone, Default)]
pub struct Executors {
    by_connection: HashMap<ConnectionId, Arc<dyn Executor>>,
}

impl Executors {
    #[must_use]
    pub fn with(mut self, connection: ConnectionId, executor: Arc<dyn Executor>) -> Self {
        self.by_connection.insert(connection, executor);
        self
    }

    pub fn get(&self, connection: ConnectionId) -> Result<&Arc<dyn Executor>, ExecutorError> {
        self.by_connection.get(&connection).ok_or_else(|| {
            ExecutorError(format!(
                "no executor serves connection {}",
                connection.index()
            ))
        })
    }
}
