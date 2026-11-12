use std::sync::Arc;

use query_data_model::introspection::{IntrospectionScope, SchemaResponse};
use query_data_model::{Catalog, ClickHouse};

use crate::auth::Claims;
use query_engine::compiler::dag::Remotes;
use query_engine::compiler::gql::RoutedStatement;
use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};

pub enum RoutingOutput {
    Query,
    Schema(SchemaResponse),
}

/// Parses against the relational connection and the code and live connections the deployment
/// serves.
#[derive(Clone)]
pub struct RoutingStage {
    pub remotes: Remotes,
}

impl RoutingStage {
    fn route(&self, ctx: &QueryPipelineContext) -> Result<RoutedStatement, PipelineError> {
        let data_model = ctx
            .server_extensions
            .get::<Arc<Catalog<ClickHouse>>>()
            .ok_or_else(|| PipelineError::custom("query data model missing"))?;
        let claims = ctx.server_extensions.get::<Claims>().ok_or_else(|| {
            PipelineError::Security("Claims not found in server_extensions".into())
        })?;
        Ok(query_engine::compiler::gql::route(
            &ctx.query_json,
            data_model,
            &self.remotes,
            claims.admin,
            IntrospectionScope::All,
            ctx.limits()?,
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
        match self
            .route(ctx)
            .inspect_err(|error| obs.record_error(error))?
        {
            RoutedStatement::Query(query) => {
                ctx.phases.insert(*query);
                Ok(RoutingOutput::Query)
            }
            RoutedStatement::Schema(response) => Ok(RoutingOutput::Schema(response)),
        }
    }
}
