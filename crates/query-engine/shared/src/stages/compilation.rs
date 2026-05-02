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
        let security_context = ctx.security_context()?;

        // Route to v2 skeleton-first pipeline when the query opts in.
        // The option is parsed from the JSON before compilation, so we
        // peek at it here. Falls back to v1 if parsing fails.
        let use_v2 = serde_json::from_str::<serde_json::Value>(&ctx.query_json)
            .ok()
            .and_then(|v| v.get("options")?.get("use_v2")?.as_bool())
            .unwrap_or(false);

        let compiled = if use_v2 {
            compiler::compile_v2(&ctx.query_json, ontology, security_context)
        } else {
            compiler::compile(&ctx.query_json, ontology, security_context)
        }
        .map_err(|e| PipelineError::Compile {
            client_safe: e.is_client_safe(),
            message: e.to_string(),
        })
        .inspect_err(|e| obs.record_error(e))?;

        let query_type: &str = compiled.query_type.into();
        obs.set_query_type(query_type);
        obs.compiled(t.elapsed());

        ctx.compiled = Some(Arc::new(compiled));
        Ok(())
    }
}
