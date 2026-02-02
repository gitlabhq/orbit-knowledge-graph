use std::collections::HashMap;

use arrow::record_batch::RecordBatch;
use query_engine::ResultContext;
use serde_json::Value;

use crate::redaction::{QueryResult, ResourceAuthorization, ResourceCheck};

pub struct ExecutionOutput {
    pub batches: Vec<RecordBatch>,
    pub result_context: ResultContext,
    pub generated_sql: String,
}

pub struct RedactionPlan {
    pub resources_to_check: Vec<ResourceCheck>,
    pub entity_to_resource_map: HashMap<String, String>,
    pub entity_to_id_column_map: HashMap<String, String>,
}

pub struct ExtractionOutput {
    pub query_result: QueryResult,
    pub result_context: ResultContext,
    pub redaction_plan: RedactionPlan,
    pub generated_sql: String,
}

pub struct AuthorizationOutput {
    pub query_result: QueryResult,
    pub result_context: ResultContext,
    pub authorizations: Vec<ResourceAuthorization>,
    pub entity_to_resource_map: HashMap<String, String>,
    pub entity_to_id_column_map: HashMap<String, String>,
    pub generated_sql: String,
}

pub struct RedactionOutput {
    pub query_result: QueryResult,
    pub result_context: ResultContext,
    pub redacted_count: usize,
    pub generated_sql: String,
}

pub struct PipelineOutput {
    pub formatted_result: Value,
    pub generated_sql: Option<String>,
    pub row_count: usize,
    pub redacted_count: usize,
}
