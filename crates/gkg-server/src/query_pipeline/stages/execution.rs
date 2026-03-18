use std::sync::Arc;
use std::time::Instant;

use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use clickhouse_client::ArrowClickHouseClient;

use querying_pipeline::{PipelineError, PipelineObserver, QueryExecutor, QueryPipelineContext};

#[derive(Clone)]
pub struct ClickHouseExecutor {
    client: Arc<ArrowClickHouseClient>,
}

impl ClickHouseExecutor {
    pub fn new(client: Arc<ArrowClickHouseClient>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl QueryExecutor for ClickHouseExecutor {
    async fn execute(
        &self,
        ctx: &QueryPipelineContext,
        obs: &mut dyn PipelineObserver,
    ) -> Result<Vec<RecordBatch>, PipelineError> {
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
        Ok(batches)
    }
}
