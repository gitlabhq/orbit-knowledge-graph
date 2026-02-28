use super::super::metrics::PipelineObserver;
use super::super::types::{AuthorizationOutput, RedactionOutput};

pub struct RedactionStage;

impl RedactionStage {
    pub fn execute(mut input: AuthorizationOutput, _obs: &PipelineObserver) -> RedactionOutput {
        let redacted_count = input
            .query_result
            .apply_authorizations(&input.authorizations);

        RedactionOutput {
            query_result: input.query_result,
            redacted_count,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use query_engine::{EntityAuthConfig, ResultContext};
    use std::sync::Arc;

    use crate::redaction::{QueryResult, ResourceAuthorization};

    fn make_input(authorizations: Vec<ResourceAuthorization>) -> AuthorizationOutput {
        let schema = Arc::new(Schema::new(vec![
            Field::new("_gkg_p_id", DataType::Int64, false),
            Field::new("_gkg_p_type", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec!["Project", "Project", "Project"])),
            ],
        )
        .unwrap();

        let mut ctx = ResultContext::new();
        ctx.add_node("p", "Project");
        ctx.add_entity_auth(
            "Project",
            EntityAuthConfig {
                resource_type: "project".to_string(),
                ability: "read".to_string(),
                auth_id_column: "id".to_string(),
                owner_entity: None,
            },
        );

        AuthorizationOutput {
            query_result: QueryResult::from_batches(&[batch], &ctx),
            authorizations,
        }
    }

    #[test]
    fn denied_rows_are_redacted() {
        let auth = vec![ResourceAuthorization {
            resource_type: "project".to_string(),
            authorized: [(10, true), (20, false), (30, true)].into_iter().collect(),
        }];

        let output = RedactionStage::execute(make_input(auth), &PipelineObserver::start());

        assert_eq!(output.redacted_count, 1);
        assert_eq!(output.query_result.authorized_count(), 2);
    }

    #[test]
    fn no_authorizations_redacts_all() {
        let output = RedactionStage::execute(make_input(vec![]), &PipelineObserver::start());

        assert_eq!(output.redacted_count, 3);
        assert_eq!(output.query_result.authorized_count(), 0);
    }
}
