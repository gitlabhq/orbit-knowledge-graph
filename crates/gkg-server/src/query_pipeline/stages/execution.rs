use std::sync::Arc;
use std::time::Instant;

use crate::query_pipeline::types::ExecutionOutput;
use clickhouse_client::ArrowClickHouseClient;

use super::super::error::PipelineError;
use super::super::metrics::PipelineObserver;
use super::super::types::CompilationOutput;
pub struct ExecutionStage {
    client: Arc<ArrowClickHouseClient>,
}

impl ExecutionStage {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }

    pub async fn execute(
        &self,
        compiled: &CompilationOutput,
        obs: &mut PipelineObserver,
    ) -> Result<ExecutionOutput, PipelineError> {
        let t = Instant::now();
        let sql = &compiled.compiled_query.sql;
        let params = &compiled.compiled_query.params;

        let mut query = self.client.query(sql);
        for (key, value) in params.iter() {
            query = ArrowClickHouseClient::bind_param(query, key, value);
        }
        let result = obs.check(
            query
                .fetch_arrow()
                .await
                .map_err(|e| PipelineError::Execution(e.to_string())),
        );
        if let Ok(ref batches) = result {
            obs.executed(t.elapsed(), batches.len());
            return Ok(ExecutionOutput {
                batches: batches.clone(),
                result_context: compiled.compiled_query.result_context.clone(),
            });
        }
        Err(PipelineError::Execution("No batches returned".into()))
    }
}
