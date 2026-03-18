use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use ontology::Ontology;
use query_engine::{CompiledQueryContext, ResultContext, SecurityContext};
use querying_types::{QueryResult, ResourceAuthorization};
use serde_json::Value;

use crate::error::{PipelineError, SecurityError};

pub struct PipelineRequest {
    pub query_json: String,
    pub security_context: SecurityContext,
}

pub struct QueryPipelineContext {
    pub compiled: Option<Arc<CompiledQueryContext>>,
    pub ontology: Arc<Ontology>,
    pub security_context: Option<SecurityContext>,
}

impl QueryPipelineContext {
    pub fn compiled(&self) -> Result<&Arc<CompiledQueryContext>, PipelineError> {
        self.compiled.as_ref().ok_or_else(|| {
            PipelineError::Compile("compiled query context not yet available".into())
        })
    }

    pub fn security_context(&self) -> Result<&SecurityContext, PipelineError> {
        self.security_context.as_ref().ok_or_else(|| {
            PipelineError::Security(
                SecurityError("security context not yet available".into()).to_string(),
            )
        })
    }
}

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
    pub redacted_count: usize,
}
