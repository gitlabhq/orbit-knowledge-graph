use std::sync::Arc;

use querying_pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};

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

        Ok(PipelineOutput {
            row_count: input.query_result.authorized_count(),
            redacted_count: input.redacted_count,
            query_type: compiled.query_type.to_string(),
            raw_query_strings: vec![compiled.base.sql.clone()],
            compiled: Arc::clone(compiled),
            query_result: input.query_result.clone(),
            result_context: input.result_context.clone(),
        })
    }
}
