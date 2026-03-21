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
        let compiled = ctx.compiled()?;
        let rendered_sql = compiled.base.render();
        let result_context = compiled.base.result_context.clone();

        let (batches, query_stats) = client
            .profiler()
            .execute_with_stats(&rendered_sql, &[])
            .await
            .map_err(|e| PipelineError::Execution(e.to_string()))
            .inspect_err(|e| obs.record_error(e))?;

        let elapsed = t.elapsed();
        obs.executed(elapsed, batches.len());
        obs.query_executed(
            "base",
            query_stats.read_rows,
            query_stats.read_bytes,
            query_stats.memory_usage,
        );

        let execution = QueryExecution {
            label: "base".into(),
            rendered_sql,
            query_id: query_stats.query_id.clone(),
            elapsed_ms: elapsed.as_secs_f64() * 1000.0,
            stats: QueryExecutionStats {
                read_rows: query_stats.read_rows,
                read_bytes: query_stats.read_bytes,
                result_rows: query_stats.result_rows,
                result_bytes: query_stats.result_bytes,
                elapsed_ns: query_stats.elapsed_ns,
                memory_usage: query_stats.memory_usage,
            },
            explain_plan: None,
            explain_pipeline: None,
            query_log: None,
            processors: None,
        };

        ctx.phases
            .get_or_insert_default::<QueryExecutionLog>()
            .0
            .push(execution);

        Ok(ExecutionOutput {
            batches,
            result_context,
        })
    }
}
