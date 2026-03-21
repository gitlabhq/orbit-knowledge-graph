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
        let sql = compiled.base.sql.clone();
        let params = compiled.base.params.clone();
        let result_context = compiled.base.result_context.clone();
        let rendered_sql = compiled.base.render();

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
        let result_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64;
        obs.executed(elapsed, batches.len());
        obs.query_executed("base", result_rows, 0, 0);

        let execution = QueryExecution {
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
