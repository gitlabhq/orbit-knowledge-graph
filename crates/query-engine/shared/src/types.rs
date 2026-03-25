use arrow::record_batch::RecordBatch;
use compiler::{CompiledQueryContext, ResultContext};
use serde::Serialize;
use std::sync::Arc;
use types::{QueryResult, ResourceAuthorization};

pub struct ExecutionOutput {
    pub batches: Vec<RecordBatch>,
    pub result_context: ResultContext,
}

pub struct ExtractionOutput {
    pub query_result: QueryResult,
}

pub struct AuthorizationOutput {
    pub query_result: QueryResult,
    pub authorizations: Vec<ResourceAuthorization>,
}

pub struct RedactionOutput {
    pub query_result: QueryResult,
    pub redacted_count: usize,
}

/// A compiled query with both parameterized template and rendered SQL.
#[derive(Debug, Clone, Serialize)]
pub struct DebugQuery {
    pub sql: String,
    pub rendered: String,
}

pub struct HydrationOutput {
    pub query_result: QueryResult,
    pub result_context: ResultContext,
    pub redacted_count: usize,
    pub hydration_queries: Vec<DebugQuery>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct QueryExecutionStats {
    pub read_rows: u64,
    pub read_bytes: u64,
    pub result_rows: u64,
    pub result_bytes: u64,
    pub elapsed_ns: u64,
    pub memory_usage: i64,
}

#[derive(Debug, Clone, Serialize)]
pub struct QueryExecution {
    pub label: String,
    pub rendered_sql: String,
    pub query_id: String,
    pub elapsed_ms: f64,
    pub stats: QueryExecutionStats,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain_plan: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub explain_pipeline: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_log: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processors: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Default)]
pub struct QueryExecutionLog(pub Vec<QueryExecution>);

#[derive(Clone)]
pub struct PipelineOutput {
    pub query_result: QueryResult,
    pub result_context: ResultContext,
    pub compiled: Arc<CompiledQueryContext>,
    pub query_type: String,
    pub raw_query_strings: Vec<String>,
    pub row_count: usize,
    pub redacted_count: usize,
    pub execution_log: Vec<QueryExecution>,
    /// Pagination metadata, present when the query included a cursor.
    pub pagination: Option<PaginationMeta>,
}

/// Pagination metadata returned when the query includes a cursor.
#[derive(Clone)]
pub struct PaginationMeta {
    /// Whether more authorized rows exist beyond the current page.
    pub has_more: bool,
    /// Total authorized rows before cursor slicing.
    pub total_rows: usize,
}
