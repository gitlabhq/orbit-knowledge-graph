use std::sync::Arc;
use std::time::Instant;

use clickhouse_client::ArrowClickHouseClient;

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};
use query_engine::shared::{
    ExecutionOutput, QueryExecution, QueryExecutionLog, QueryExecutionStats,
};

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

        let (sql, params, result_context, rendered_sql) = {
            let compiled = ctx.compiled()?;
            (
                compiled.base.sql.clone(),
                compiled.base.params.clone(),
                compiled.base.result_context.clone(),
                compiled.base.render(),
            )
        };

        let mut query = client.query(&sql);
        for (key, param) in params.iter() {
            query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.ch_type);
        }
        let batches = query
            .fetch_arrow()
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))
            .inspect_err(|e| obs.record_error(e))?;

        let elapsed = t.elapsed();
        obs.executed(elapsed, batches.len());

        // TODO: capture read_rows/read_bytes/memory_usage stats here.
        // The clickhouse-rs crate discards X-ClickHouse-Summary headers so we
        // need a different approach. See https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/640
        let result_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64;
        ctx.phases
            .get_or_insert_default::<QueryExecutionLog>()
            .0
            .push(QueryExecution {
                label: "base".into(),
                rendered_sql,
                query_id: String::new(),
                elapsed_ms: elapsed.as_secs_f64() * 1000.0,
                stats: QueryExecutionStats {
                    result_rows,
                    elapsed_ns: elapsed.as_nanos() as u64,
                    ..Default::default()
                },
                explain_plan: None,
                explain_pipeline: None,
                query_log: None,
                processors: None,
            });

        Ok(ExecutionOutput {
            batches,
            result_context,
        })
    }
}
