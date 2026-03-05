use std::sync::Arc;
use std::time::Instant;

use query_engine::compile;

use crate::redaction::RedactionMessage;

use super::super::error::PipelineError;
use super::super::metrics::PipelineObserver;
use super::super::types::{PipelineRequest, QueryPipelineContext};
use super::PipelineStage;

#[derive(Clone)]
pub struct CompilationStage;

impl<M: RedactionMessage> PipelineStage<M> for CompilationStage {
    type Input = ();
    type Output = ();

    async fn execute(
        &self,
        _input: Self::Input,
        ctx: &mut QueryPipelineContext,
        req: &mut PipelineRequest<'_, M>,
        obs: &mut PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let t = Instant::now();
        let ontology = &ctx.ontology;
        let security_context = ctx.security_context()?;

        let compiled = obs.check(
            compile(req.query_json, ontology, security_context)
                .map_err(|e| PipelineError::Compile(e.to_string())),
        )?;

        let query_type: &str = compiled.query_type.into();
        obs.set_query_type(query_type);
        obs.compiled(t.elapsed());

        ctx.compiled = Some(Arc::new(compiled));
        Ok(())
    }
}
