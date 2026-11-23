use pipeline::{
    PipelineError, PipelineObserver, PipelineRunner, PipelineStage, QueryPipelineContext,
};
use query_data_model::{ClickHouseDataModel, DuckDbDataModel};

use crate::stages::{
    CompilationStage, ExecutionStage, ExtractionStage, OutputStage, QueryExecutor,
};
use crate::types::{
    AuthorizationOutput, ExtractionOutput, HydrationOutput, PipelineOutput, RedactionOutput,
};

pub struct RemoteStages<'a, Sec, X, Auth, Red, Hyd> {
    pub security: &'a Sec,
    pub executor: X,
    pub authorization: &'a Auth,
    pub redaction: &'a Red,
    pub hydration: &'a Hyd,
}

pub async fn run_remote<Sec, X, Auth, Red, Hyd>(
    ctx: &mut QueryPipelineContext,
    obs: &mut dyn PipelineObserver,
    stages: RemoteStages<'_, Sec, X, Auth, Red, Hyd>,
) -> Result<PipelineOutput, PipelineError>
where
    Sec: PipelineStage<Input = (), Output = ()>,
    X: QueryExecutor,
    Auth: PipelineStage<Input = ExtractionOutput, Output = AuthorizationOutput>,
    Red: PipelineStage<Input = AuthorizationOutput, Output = RedactionOutput>,
    Hyd: PipelineStage<Input = RedactionOutput, Output = HydrationOutput>,
{
    let output = PipelineRunner::start(ctx, obs)
        .then(stages.security)
        .await?
        .then(&CompilationStage::<ClickHouseDataModel>::new())
        .await?
        .then(&ExecutionStage(stages.executor))
        .await?
        .then(&ExtractionStage)
        .await?
        .then(stages.authorization)
        .await?
        .then(stages.redaction)
        .await?
        .then(stages.hydration)
        .await?
        .then(&OutputStage)
        .await?
        .finish();
    output
        .ok_or_else(|| PipelineError::custom("OutputStage did not produce PipelineOutput"))
        .inspect_err(|error| obs.record_error(error))
}

pub struct NoHydrationStage;

impl PipelineStage for NoHydrationStage {
    type Input = ExtractionOutput;
    type Output = HydrationOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let input = ctx.phases.remove::<ExtractionOutput>().ok_or_else(|| {
            PipelineError::Execution("ExtractionOutput not found in phases".into())
        })?;
        Ok(HydrationOutput {
            query_result: input.query_result,
            result_context: ctx.compiled()?.base.result_context.clone(),
            redacted_count: 0,
            hydration_queries: Vec::new(),
        })
    }
}

pub async fn run_local<X: QueryExecutor>(
    ctx: &mut QueryPipelineContext,
    obs: &mut dyn PipelineObserver,
    executor: X,
) -> Result<PipelineOutput, PipelineError> {
    PipelineRunner::start(ctx, obs)
        .then(&CompilationStage::<DuckDbDataModel>::new())
        .await?
        .then(&ExecutionStage(executor))
        .await?
        .then(&ExtractionStage)
        .await?
        .then(&NoHydrationStage)
        .await?
        .then(&OutputStage)
        .await?
        .finish()
        .ok_or_else(|| PipelineError::custom("OutputStage did not produce PipelineOutput"))
}
