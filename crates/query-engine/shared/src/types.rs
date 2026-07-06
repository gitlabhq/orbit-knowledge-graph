use arrow::record_batch::RecordBatch;
use compiler::{CompiledQueryContext, ResultContext};
use gkg_utils::arrow::ColumnValue;
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

pub struct PipelineOutput {
    pub query_result: QueryResult,
    pub result_context: ResultContext,
    pub compiled: Arc<CompiledQueryContext>,
    pub query_type: String,
    pub raw_query_strings: Vec<String>,
    pub row_count: usize,
    pub redacted_count: usize,
    pub execution_log: Vec<QueryExecution>,
    /// Pagination metadata; always present (`Option` only spares test fixtures).
    pub pagination: Option<PaginationMeta>,
}

pub struct PaginationMeta {
    /// Whether the fetched window overflowed: more matching rows exist in the dataset.
    pub has_more: bool,
    /// Same signal as `has_more`, kept explicit so clients can detect incomplete results.
    pub truncated: bool,
    /// Opaque keyset token for the next page; None when the dataset is exhausted.
    pub next_cursor: Option<String>,
}

/// Trims the overfetched probe row and derives honest pagination metadata.
/// The next-page token anchors on the last SCANNED row (authorized or not),
/// so redaction shortens a page but never stalls pagination.
pub fn paginate(query_result: &mut QueryResult, input: &compiler::Input) -> PaginationMeta {
    let window = input.cursor.as_ref().map_or(input.limit, |c| c.page_size) as usize;
    let has_more = query_result.len() > window;
    if has_more {
        query_result.truncate(window);
    }
    let key_count = input.compiler.cursor_key_count;
    let next_cursor = input
        .cursor
        .as_ref()
        .filter(|_| has_more && key_count > 0)
        .and_then(|_| {
            let last = query_result.rows().last()?;
            (0..key_count)
                .map(|i| {
                    // A present-but-NULL readback is a real NULL sort key and
                    // stays paginable; an absent column means the readback was
                    // lost upstream, so withhold the token rather than seek on
                    // a wrong boundary.
                    match last.column(&compiler::passes::cursor::cursor_column(i)) {
                        Some(ColumnValue::Null) => Some(None),
                        Some(v) => v.as_string().cloned().map(Some),
                        None => None,
                    }
                })
                .collect::<Option<Vec<Option<String>>>>()
        })
        .map(|keys| compiler::passes::cursor::encode(input.compiler.query_hash, &keys));
    PaginationMeta {
        has_more,
        truncated: has_more,
        next_cursor,
    }
}
