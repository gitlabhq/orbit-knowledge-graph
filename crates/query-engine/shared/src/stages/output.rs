use std::sync::Arc;

use pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};
use serde_json::json;

use crate::types::{HydrationOutput, PipelineOutput};

#[derive(Clone)]
pub struct OutputStage;

impl PipelineStage for OutputStage {
    type Input = HydrationOutput;
    type Output = PipelineOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let input = ctx.phases.get::<HydrationOutput>().ok_or_else(|| {
            PipelineError::Execution("HydrationOutput not found in phases".into())
        })?;

        let compiled = ctx.compiled()?;

        let debug_json = json!({
            "base": compiled.base.sql,
            "base_rendered": compiled.base.render(),
            "hydration": input.hydration_queries,
        });

        let stats = ctx
            .phases
            .get::<crate::ExecutionOutput>()
            .and_then(|e| e.stats.clone());

        Ok(PipelineOutput {
            row_count: input.query_result.authorized_count(),
            redacted_count: input.redacted_count,
            query_type: compiled.query_type.to_string(),
            raw_query_strings: vec![debug_json.to_string()],
            compiled: Arc::clone(compiled),
            query_result: input.query_result.clone(),
            result_context: input.result_context.clone(),
            stats,
        })
    }
}
