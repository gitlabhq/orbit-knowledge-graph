use std::sync::Mutex;

use arrow::record_batch::RecordBatch;
use duckdb_client::DuckDbClient;
use orbit_server::pipeline::HydrationStage;
use query_engine::compiler::ParameterizedQuery;
use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};
use query_engine::shared::{AuthorizationOutput, ExtractionOutput, QueryExecutor};

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

pub(super) struct LocalExecutor<'a> {
    pub(super) database: &'a Mutex<DuckDbClient>,
    pub(super) coverage: &'a Mutex<Coverage>,
}

impl QueryExecutor for LocalExecutor<'_> {
    async fn run(
        &self,
        query: &ParameterizedQuery,
        ctx: &mut QueryPipelineContext,
        _obs: &mut dyn PipelineObserver,
    ) -> Result<Vec<RecordBatch>, PipelineError> {
        self.coverage.lock().unwrap().record(&ctx.compiled()?.ast);
        let sql = query.render();
        self.database
            .lock()
            .unwrap()
            .query_arrow(&sql)
            .map_err(|error| PipelineError::Execution(format!("{error}\n{sql}")))
    }
}
