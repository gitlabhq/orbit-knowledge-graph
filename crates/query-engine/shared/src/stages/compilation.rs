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
        let parsed = ctx.phases.remove::<compiler::gql::ParsedQuery>();
        let grants = ctx.grants().inspect_err(|e| obs.record_error(e))?;

        let compiled = ctx
            .server_extensions
            .get::<Arc<query_data_model::Catalog<query_data_model::ClickHouse>>>()
            .ok_or_else(|| {
                compiler::QueryError::PipelineInvariant("no data model in the pipeline".into())
            })
            .and_then(|data_model| match parsed {
                Some(query) => compiler::gql::compile_query(query, data_model, grants),
                None => compiler::compile(&ctx.query_json, data_model, grants),
            })
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
