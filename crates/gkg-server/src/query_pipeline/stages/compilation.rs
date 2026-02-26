use std::sync::Arc;
use std::time::Instant;

use ontology::Ontology;
use query_engine::{SecurityContext, compile};

use super::super::error::PipelineError;
use super::super::metrics::PipelineObserver;
use super::super::types::CompilationOutput;

pub struct CompilationStage;

impl CompilationStage {
    pub fn execute(
        query_json: &str,
        ontology: &Arc<Ontology>,
        security_context: &SecurityContext,
        obs: &mut PipelineObserver,
    ) -> Result<CompilationOutput, PipelineError> {
        let t = Instant::now();

        let compiled = obs.check(
            compile(query_json, ontology, security_context)
                .map_err(|e| PipelineError::Compile(e.to_string())),
        )?;

        let query_type: &'static str = compiled
            .result_context
            .query_type
            .ok_or_else(|| PipelineError::Compile("query_type not set by enforce_return".into()))
            .map(Into::into)?;

        obs.set_query_type(query_type);
        obs.compiled(t.elapsed());

        Ok(CompilationOutput {
            compiled_query: compiled,
        })
    }
}
