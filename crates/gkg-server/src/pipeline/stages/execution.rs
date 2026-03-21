use std::sync::Arc;
use std::time::Instant;

use clickhouse_client::ArrowClickHouseClient;

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};
use query_engine::shared::{ClickHouseStats, ExecutionOutput};

#[derive(Clone)]
pub struct ClickHouseExecutor;

impl PipelineStage for ClickHouseExecutor {
    type Input = ();
    type Output = ExecutionOutput;

    async fn execute(
        &self,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let t = Instant::now();
        let client = ctx
            .server_extensions
            .get::<Arc<ArrowClickHouseClient>>()
            .ok_or_else(|| PipelineError::Execution("ClickHouse client not available".into()))?;
        let compiled = ctx.compiled()?;
        let rendered_sql = compiled.base.render();

        let (batches, query_stats) = client
            .fetch_arrow_with_stats(&rendered_sql)
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))
            .inspect_err(|e| obs.record_error(e))?;

        obs.executed(t.elapsed(), batches.len());
        Ok(ExecutionOutput {
            batches,
            result_context: compiled.base.result_context.clone(),
            stats: Some(ClickHouseStats {
                read_rows: query_stats.read_rows,
                read_bytes: query_stats.read_bytes,
                elapsed_ns: query_stats.elapsed_ns,
                result_rows: query_stats.result_rows,
            }),
        })
    }
}
