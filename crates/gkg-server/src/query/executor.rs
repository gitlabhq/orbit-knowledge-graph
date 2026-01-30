use serde_json::Value;
use thiserror::Error;

use crate::auth::Claims;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("Invalid query: {0}")]
    InvalidQuery(String),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Execution failed: {0}")]
    ExecutionFailed(String),
}

impl QueryError {
    pub fn code(&self) -> String {
        match self {
            Self::InvalidQuery(_) => "invalid_query".to_string(),
            Self::ParseError(_) => "parse_error".to_string(),
            Self::ExecutionFailed(_) => "execution_error".to_string(),
        }
    }
}

#[derive(Debug)]
pub struct QueryResult {
    pub result: Value,
    pub generated_sql: String,
}

#[derive(Debug, Clone, Default)]
pub struct QueryExecutor;

impl QueryExecutor {
    pub fn new() -> Self {
        Self
    }

    /// Execute a raw JSON DSL query.
    /// Returns the query result and the generated SQL for debugging/audit.
    pub fn execute(&self, query_json: &str, _claims: &Claims) -> Result<QueryResult, QueryError> {
        let query: Value =
            serde_json::from_str(query_json).map_err(|e| QueryError::ParseError(e.to_string()))?;

        let operation = query
            .get("operation")
            .and_then(|v| v.as_str())
            .ok_or_else(|| QueryError::InvalidQuery("operation is required".to_string()))?;

        Err(QueryError::InvalidQuery(format!(
            "Unknown operation: {}",
            operation
        )))
    }
}
