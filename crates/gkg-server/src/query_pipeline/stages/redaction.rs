use super::super::types::{AuthorizationOutput, RedactionOutput};

pub struct RedactionStage;

impl RedactionStage {
    pub fn execute(mut input: AuthorizationOutput) -> RedactionOutput {
        let redacted_count = input
            .query_result
            .apply_authorizations(&input.authorizations);

        RedactionOutput {
            query_result: input.query_result,
            redacted_count,
        }
    }
}
