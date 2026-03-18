use querying_pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};

use crate::types::{AuthorizationOutput, RedactionOutput};

#[derive(Clone)]
pub struct RedactionStage;

impl PipelineStage for RedactionStage {
    type Input = AuthorizationOutput;
    type Output = RedactionOutput;

    async fn execute(
        &self,
        mut input: Self::Input,
        _ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let redacted_count = input
            .query_result
            .apply_authorizations(&input.authorizations);

        Ok(RedactionOutput {
            query_result: input.query_result,
            redacted_count,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use ontology::Ontology;
    use query_engine::{EntityAuthConfig, ResultContext};
    use querying_pipeline::NoOpObserver;
    use querying_types::{QueryResult, ResourceAuthorization};
    use std::sync::Arc;

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

    #[tokio::test]
    async fn denied_rows_are_redacted() {
        let auth = vec![ResourceAuthorization {
            resource_type: "project".to_string(),
            authorized: [(10, true), (20, false), (30, true)].into_iter().collect(),
        }];

        let mut ctx = QueryPipelineContext {
            query_json: String::new(),
            compiled: None,
            ontology: Arc::new(Ontology::new()),
            security_context: None,
            extensions: Default::default(),
        };
        let mut obs = NoOpObserver;

        let output = RedactionStage
            .execute(make_input(auth), &mut ctx, &mut obs)
            .await
            .unwrap();

        assert_eq!(output.redacted_count, 1);
        assert_eq!(output.query_result.authorized_count(), 2);
    }

    #[tokio::test]
    async fn no_authorizations_redacts_all() {
        let mut ctx = QueryPipelineContext {
            query_json: String::new(),
            compiled: None,
            ontology: Arc::new(Ontology::new()),
            security_context: None,
            extensions: Default::default(),
        };
        let mut obs = NoOpObserver;

        let output = RedactionStage
            .execute(make_input(vec![]), &mut ctx, &mut obs)
            .await
            .unwrap();

        assert_eq!(output.redacted_count, 3);
        assert_eq!(output.query_result.authorized_count(), 0);
    }
}
