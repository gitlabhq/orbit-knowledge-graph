use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};

use query_engine::shared::{AuthorizationOutput, RedactionOutput};

#[derive(Clone)]
pub struct RedactionStage;

impl PipelineStage for RedactionStage {
    type Input = AuthorizationOutput;
    type Output = RedactionOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let input = ctx.phases.get::<AuthorizationOutput>().ok_or_else(|| {
            PipelineError::Authorization("AuthorizationOutput not found in phases".into())
        })?;

        let mut query_result = input.query_result.clone();
        let redacted_count = query_result.apply_authorizations(&input.authorizations);

        Ok(RedactionOutput {
            query_result,
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
    use query_engine::compiler::{EntityAuthConfig, ResultContext};
    use query_engine::pipeline::NoOpObserver;
    use query_engine::types::{QueryResult, ResourceAuthorization};
    use std::sync::Arc;

    fn seed_ctx(authorizations: Vec<ResourceAuthorization>) -> QueryPipelineContext {
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

        let mut result_ctx = ResultContext::new();
        result_ctx.add_node("p", "Project");
        result_ctx.add_entity_auth(
            "Project",
            EntityAuthConfig {
                resource_type: "project".to_string(),
                ability: "read".to_string(),
                auth_id_column: "id".to_string(),
                owner_entity: None,
                required_access_level: 20,
            },
        );

        let mut ctx = QueryPipelineContext {
            query_json: String::new(),
            compiled: None,
            ontology: Arc::new(Ontology::new()),
            security_context: None,
            server_extensions: Default::default(),
            phases: Default::default(),
        };
        ctx.phases.insert(AuthorizationOutput {
            query_result: QueryResult::from_batches(&[batch], &result_ctx),
            authorizations,
        });
        ctx
    }

    #[tokio::test]
    async fn denied_rows_are_redacted() {
        let auth = vec![ResourceAuthorization {
            resource_type: "project".to_string(),
            authorized: [(10, true), (20, false), (30, true)].into_iter().collect(),
        }];
        let mut ctx = seed_ctx(auth);
        let mut obs = NoOpObserver;

        let output = RedactionStage.execute(&mut ctx, &mut obs).await.unwrap();
        assert_eq!(output.redacted_count, 1);
        assert_eq!(output.query_result.authorized_count(), 2);
    }

    #[tokio::test]
    async fn no_authorizations_redacts_all() {
        let mut ctx = seed_ctx(vec![]);
        let mut obs = NoOpObserver;

        let output = RedactionStage.execute(&mut ctx, &mut obs).await.unwrap();
        assert_eq!(output.redacted_count, 3);
        assert_eq!(output.query_result.authorized_count(), 0);
    }
}
