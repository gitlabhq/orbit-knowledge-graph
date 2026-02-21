use serde_json::Value;

use crate::redaction::{QueryResult, ResourceAuthorization};

pub struct AuthorizationOutput {
    pub query_result: QueryResult,
    pub authorizations: Vec<ResourceAuthorization>,
}

pub struct PipelineOutput {
    pub formatted_result: Value,
    pub generated_sql: String,
    pub row_count: usize,
    pub redacted_count: usize,
}
