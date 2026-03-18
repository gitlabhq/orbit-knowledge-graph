use std::sync::Arc;
use std::time::Instant;

use clickhouse_client::ArrowClickHouseClient;

use querying_pipeline::{
    ExecutionOutput, PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};

#[derive(Clone)]
pub struct ClickHouseExecutor {
    client: Arc<ArrowClickHouseClient>,
}

impl ClickHouseExecutor {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }
}

impl PipelineStage for ClickHouseExecutor {
    type Input = ();
    type Output = ExecutionOutput;

    async fn execute(
        &self,
        _input: Self::Input,
        ctx: &mut QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let t = Instant::now();
        let compiled = ctx.compiled()?;
        let sql = &compiled.base.sql;
        let params = &compiled.base.params;

        let mut query = self.client.query(sql);
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
