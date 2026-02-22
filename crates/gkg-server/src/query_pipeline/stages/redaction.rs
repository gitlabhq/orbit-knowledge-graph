use crate::redaction::QueryResult;

use super::super::types::AuthorizationOutput;

pub struct RedactionStage;

impl RedactionStage {
    /// Apply authorization results, returning the query result and count of redacted rows.
    pub fn execute(mut input: AuthorizationOutput) -> (QueryResult, usize) {
        let redacted_count = input
            .query_result
            .apply_authorizations(&input.authorizations);
        (input.query_result, redacted_count)
    }
}
