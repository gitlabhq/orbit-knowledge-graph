use std::collections::HashMap;

use super::super::types::{AuthorizationOutput, RedactionOutput};

pub struct RedactionStage;

impl RedactionStage {
    pub fn execute(mut input: AuthorizationOutput) -> RedactionOutput {
        let entity_map: HashMap<&str, &str> = input
            .entity_to_resource_map
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();

        let redacted_count = input
            .query_result
            .apply_authorizations(&input.authorizations, &entity_map);

        RedactionOutput {
            query_result: input.query_result,
            result_context: input.result_context,
            redacted_count,
            generated_sql: input.generated_sql,
        }
    }
}
