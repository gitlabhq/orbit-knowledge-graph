use arrow::record_batch::RecordBatch;
use query_engine::{CompiledQuery, ResultContext};
use serde_json::Value;

use crate::redaction::{QueryResult, ResourceAuthorization};

pub struct ExecutionOutput {
    pub batches: Vec<RecordBatch>,
    pub result_context: ResultContext,
}

pub struct CompilationOutput {
    pub compiled_query: CompiledQuery,
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

pub struct HydrationOutput {
    pub query_result: QueryResult,
    pub result_context: ResultContext,
    pub redacted_count: usize,
}

pub struct PipelineOutput {
    pub formatted_result: Value,
    pub query_type: String,
    pub raw_query_strings: Vec<String>,
    pub row_count: usize,
}
