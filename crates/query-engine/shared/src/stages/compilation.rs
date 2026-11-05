use std::sync::Arc;
use std::time::Instant;

use pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};

#[derive(Clone)]
pub struct CompilationStage;

impl PipelineStage for CompilationStage {
    type Input = ();
    type Output = ();

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let t = Instant::now();
        let security_context = ctx
            .security_context()
            .inspect_err(|e| obs.record_error(e))?;

        let compiled = ctx
            .server_extensions
            .get::<Arc<query_data_model::ClickHouseDataModel>>()
            .ok_or_else(|| {
                compiler::QueryError::PipelineInvariant("no data model in the pipeline".into())
            })
            .and_then(
                |data_model| match ctx.phases.get::<compiler::gql::ParsedQuery>() {
                    Some(query) => {
                        compiler::gql::compile_query(query.clone(), data_model, security_context)
                    }
                    None => compiler::compile(
                        &ctx.query_json,
                        ctx.frontend,
                        data_model,
                        security_context,
                    ),
                },
            )
            .map_err(PipelineError::from)
            .inspect_err(|e| obs.record_error(e))?;

        let query_type: &str = compiled.query_type.into();
        obs.set_query_type(query_type);
        obs.set_compiled(&compiled);
        obs.compiled(t.elapsed());

        ctx.compiled = Some(Arc::new(compiled));
        Ok(())
    }
}
