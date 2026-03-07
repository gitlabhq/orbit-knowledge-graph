use std::time::Instant;

use crate::query_pipeline::types::ExecutionOutput;
use crate::redaction::RedactionMessage;
use clickhouse_client::ArrowClickHouseClient;

use super::super::error::PipelineError;
use super::super::metrics::PipelineObserver;
use super::super::types::{PipelineRequest, QueryPipelineContext};
use super::PipelineStage;

#[derive(Clone)]
pub struct ExecutionStage;

impl<M: RedactionMessage> PipelineStage<M> for ExecutionStage {
    type Input = ();
    type Output = ExecutionOutput;

    async fn execute(
        &self,
        _input: Self::Input,
        ctx: &mut QueryPipelineContext,
        _req: &mut PipelineRequest<'_, M>,
        obs: &mut PipelineObserver,
    ) -> Result<Self::Output, PipelineError> {
        let t = Instant::now();
        let compiled = ctx.compiled()?;
        let sql = &compiled.base.sql;
        let params = &compiled.base.params;

        let mut query = ctx.client.query(sql);
        for (key, param) in params.iter() {
            query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.ch_type);
        }
        let batches = obs.check(
            query
                .fetch_arrow()
                .await
                .map_err(|e| PipelineError::Execution(e.to_string())),
        )?;
        obs.executed(t.elapsed(), batches.len());
        Ok(ExecutionOutput {
            batches,
            result_context: compiled.base.result_context.clone(),
        })
    }
}
