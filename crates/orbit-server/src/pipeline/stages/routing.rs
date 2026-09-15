use ontology::introspection::SchemaResponse;
use query_engine::compiler::{Frontend, gql::RoutedStatement};
use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};

pub enum RoutingOutput {
    Query,
    Schema(SchemaResponse),
}

#[derive(Clone)]
pub struct RoutingStage;

impl PipelineStage for RoutingStage {
    type Input = ();
    type Output = RoutingOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        match ctx.frontend {
            Frontend::JsonDsl => Ok(RoutingOutput::Query),
            Frontend::Gql => match query_engine::compiler::gql::route(
                &ctx.query_json,
                &ctx.ontology,
                ontology::introspection::IntrospectionScope::All,
            )
            .map_err(|error| PipelineError::Compile {
                client_safe: error.is_client_safe(),
                message: error.to_string(),
            })
            .inspect_err(|error| obs.record_error(error))?
            {
                RoutedStatement::Query(input) => {
                    ctx.phases.insert(*input);
                    Ok(RoutingOutput::Query)
                }
                RoutedStatement::Schema(response) => Ok(RoutingOutput::Schema(response)),
            },
        }
    }
}
