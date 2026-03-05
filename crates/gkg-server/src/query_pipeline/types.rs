use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use clickhouse_client::ArrowClickHouseClient;
use ontology::Ontology;
use query_engine::{CompiledQueryContext, ResultContext, SecurityContext};
use serde_json::Value;

use tokio::sync::mpsc;
use tonic::{Status, Streaming};

use crate::auth::Claims;
use crate::redaction::{QueryResult, RedactionMessage, ResourceAuthorization};

use super::error::PipelineError;

pub struct PipelineRequest<'a, M: RedactionMessage> {
    pub claims: &'a Claims,
    pub query_json: &'a str,
    pub tx: Option<&'a mpsc::Sender<Result<M, Status>>>,
    pub stream: Option<&'a mut Streaming<M>>,
}

pub struct QueryPipelineContext {
    pub compiled: Option<Arc<CompiledQueryContext>>,
    pub ontology: Arc<Ontology>,
    pub client: Arc<ArrowClickHouseClient>,
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
            super::stages::SecurityError("security context not yet available".into()).into()
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
