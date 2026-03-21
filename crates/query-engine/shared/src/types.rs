use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use compiler::{CompiledQueryContext, ResultContext};
use serde::Serialize;
use types::QueryResult;

#[derive(Debug, Clone, Default)]
pub struct ClickHouseStats {
    pub read_rows: u64,
    pub read_bytes: u64,
    pub elapsed_ns: u64,
    pub result_rows: u64,
}

pub struct ExecutionOutput {
    pub batches: Vec<RecordBatch>,
    pub result_context: ResultContext,
    pub stats: Option<ClickHouseStats>,
}

pub struct ExtractionOutput {
    pub query_result: QueryResult,
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

pub struct PipelineOutput {
    pub query_result: QueryResult,
    pub result_context: ResultContext,
    pub compiled: Arc<CompiledQueryContext>,
    pub query_type: String,
    pub raw_query_strings: Vec<String>,
    pub row_count: usize,
    pub redacted_count: usize,
    pub stats: Option<ClickHouseStats>,
}
