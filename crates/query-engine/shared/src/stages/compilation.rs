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
        let ontology = &ctx.ontology;
        let security_context = ctx
            .security_context()
            .inspect_err(|e| obs.record_error(e))?;

        let data_model = ctx
            .server_extensions
            .get::<Arc<query_data_model::ClickHouseDataModel>>();
        let compiled = match ctx.phases.get::<compiler::Input>() {
            Some(input) => match data_model {
                Some(data_model) => {
                    compiler::gql::compile_query_model(input.clone(), data_model, security_context)
                }
                None => compiler::gql::compile_query(input.clone(), ontology, security_context),
            },
            None => match data_model {
                Some(data_model) => compiler::compile_model(
                    &ctx.query_json,
                    ctx.frontend,
                    data_model,
                    security_context,
                ),
                None => {
                    compiler::compile(&ctx.query_json, ctx.frontend, ontology, security_context)
                }
            },
        }
        .map_err(|e| PipelineError::Compile {
            client_safe: e.is_client_safe(),
            message: e.to_string(),
        })
        .inspect_err(|e| obs.record_error(e))?;

        let query_type: &str = compiled.query_type.into();
        obs.set_query_type(query_type);
        obs.set_compiled(&compiled);
        obs.compiled(t.elapsed());

        ctx.compiled = Some(Arc::new(compiled));
        Ok(())
    }
}
