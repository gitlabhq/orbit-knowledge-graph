use std::sync::Arc;
use std::time::Instant;

use clickhouse_client::ArrowClickHouseClient;

use querying_pipeline::{PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext};
use querying_shared_stages::ExecutionOutput;

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
        let sql = &compiled.base.sql;
        let params = &compiled.base.params;

        let mut query = client.query(sql);
        for (key, param) in params.iter() {
            query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.ch_type);
        }
        let batches = query
            .fetch_arrow()
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))
            .inspect_err(|e| obs.record_error(e))?;

        obs.executed(t.elapsed(), batches.len());
        Ok(ExecutionOutput {
            batches,
            result_context: compiled.base.result_context.clone(),
        })
    }
}
