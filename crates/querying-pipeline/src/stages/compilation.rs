use std::sync::Arc;
use std::time::Instant;

use query_engine::compile;

use crate::error::PipelineError;
use crate::observer::PipelineObserver;
use crate::types::QueryPipelineContext;

pub struct CompilationStage;

impl CompilationStage {
    pub fn execute(
        &self,
        query_json: &str,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<(), PipelineError> {
        let t = Instant::now();
        let ontology = &ctx.ontology;
        let security_context = ctx.security_context()?;

        let compiled = compile(query_json, ontology, security_context)
            .map_err(|e| PipelineError::Compile(e.to_string()))
            .inspect_err(|e| obs.record_error(e))?;

        let query_type: &str = compiled.query_type.into();
        obs.set_query_type(query_type);
        obs.compiled(t.elapsed());

        ctx.compiled = Some(Arc::new(compiled));
        Ok(())
    }
}
