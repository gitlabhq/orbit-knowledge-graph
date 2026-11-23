use arrow::record_batch::RecordBatch;
use compiler::ParameterizedQuery;
use pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};

use crate::types::ExecutionOutput;

pub trait QueryExecutor: Send + Sync {
    fn run(
        &self,
        query: &ParameterizedQuery,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> impl Future<Output = Result<Vec<RecordBatch>, PipelineError>> + Send;
}

pub struct ExecutionStage<X>(pub X);

impl<X: QueryExecutor> PipelineStage for ExecutionStage<X> {
    type Input = ();
    type Output = ExecutionOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let compiled = ctx.compiled().inspect_err(|e| obs.record_error(e))?.clone();
        let batches = self.0.run(&compiled.base, ctx, obs).await?;
        Ok(ExecutionOutput {
            batches,
            result_context: compiled.base.result_context.clone(),
        })
    }
}
