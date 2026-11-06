use std::sync::Arc;

use ontology::introspection::SchemaResponse;
use query_data_model::ClickHouseDataModel;

use crate::auth::Claims;
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

impl RoutingStage {
    fn route(ctx: &QueryPipelineContext) -> Result<RoutedStatement, PipelineError> {
        let data_model = ctx
            .server_extensions
            .get::<Arc<ClickHouseDataModel>>()
            .ok_or_else(|| PipelineError::custom("query data model missing"))?;
        let claims = ctx.server_extensions.get::<Claims>().ok_or_else(|| {
            PipelineError::Security("Claims not found in server_extensions".into())
        })?;
        Ok(query_engine::compiler::gql::route(
            &ctx.query_json,
            &ctx.ontology,
            data_model,
            claims.admin,
            ontology::introspection::IntrospectionScope::All,
        )?)
    }
}

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
            Frontend::Gql => match Self::route(ctx).inspect_err(|error| obs.record_error(error))? {
                RoutedStatement::Query(query) => {
                    ctx.phases.insert(*query);
                    Ok(RoutingOutput::Query)
                }
                RoutedStatement::Schema(response) => Ok(RoutingOutput::Schema(response)),
            },
        }
    }
}
