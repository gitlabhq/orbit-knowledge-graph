use std::sync::{Arc, Mutex};

use duckdb_client::DuckDbClient;
use orbit_server::pipeline::HydrationStage;
use query_engine::compiler::compile_local;
use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};
use query_engine::shared::{
    AuthorizationOutput, ExecutionOutput, ExtractionOutput, HydrationOutput,
};

use super::coverage::Coverage;
use crate::mock_redaction::MockRedactionService;

pub(super) struct MockAuthorizationStage<'a>(pub(super) &'a MockRedactionService);

impl PipelineStage for MockAuthorizationStage<'_> {
    type Input = ExtractionOutput;
    type Output = AuthorizationOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let mut input = ctx.phases.remove::<ExtractionOutput>().ok_or_else(|| {
            PipelineError::Authorization("ExtractionOutput not found in phases".into())
        })?;
        HydrationStage::resolve_owners(ctx, &mut input.query_result).await?;
        let authorizations = self.0.check(&input.query_result.resource_checks());
        Ok(AuthorizationOutput {
            query_result: input.query_result,
            authorizations,
        })
    }
}

pub(super) struct LocalCompilationStage;

impl PipelineStage for LocalCompilationStage {
    type Input = ();
    type Output = ();

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let compiled = compile_local(&ctx.query_json, ctx.frontend, &ctx.ontology)?;
        ctx.compiled = Some(Arc::new(compiled));
        Ok(())
    }
}

pub(super) struct LocalExecutionStage<'a> {
    pub(super) database: &'a Mutex<DuckDbClient>,
    pub(super) coverage: &'a Mutex<Coverage>,
}

impl PipelineStage for LocalExecutionStage<'_> {
    type Input = ();
    type Output = ExecutionOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let compiled = ctx.compiled()?;
        self.coverage.lock().unwrap().record(&compiled.ast);
        let sql = compiled.base.render();
        let batches = self
            .database
            .lock()
            .unwrap()
            .query_arrow(&sql)
            .map_err(|error| PipelineError::Execution(format!("{error}\n{sql}")))?;
        Ok(ExecutionOutput {
            batches,
            result_context: compiled.base.result_context.clone(),
        })
    }
}

pub(super) struct LocalFinishStage;

impl PipelineStage for LocalFinishStage {
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
