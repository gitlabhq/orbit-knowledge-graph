use std::sync::Arc;
use std::time::Instant;

use clickhouse_client::{ArrowClickHouseClient, ProfilingConfig};

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
        let profiling = ctx
            .server_extensions
            .get::<ProfilingConfig>()
            .cloned()
            .unwrap_or_default();

        let (sql, params, result_context, rendered_sql) = {
            let compiled = ctx.compiled()?;
            (
                compiled.base.sql.clone(),
                compiled.base.params.clone(),
                compiled.base.result_context.clone(),
                compiled.base.render(),
            )
        };

        let (batches, execution) = if profiling.enabled {
            execute_profiled(client, &sql, &params, &rendered_sql, &profiling, t).await?
        } else {
            execute_standard(client, &sql, &params, &rendered_sql, t).await?
        };

        let elapsed = t.elapsed();
        obs.executed(elapsed, batches.len());
        obs.query_executed(
            "base",
            execution.stats.read_rows,
            execution.stats.read_bytes,
            execution.stats.memory_usage,
        );

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

async fn execute_standard(
    client: &ArrowClickHouseClient,
    sql: &str,
    params: &std::collections::HashMap<String, gkg_utils::clickhouse::ParamValue>,
    rendered_sql: &str,
    t: Instant,
) -> Result<(Vec<arrow::record_batch::RecordBatch>, QueryExecution), PipelineError> {
    let mut query = client.query(sql);
    for (key, param) in params.iter() {
        query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.ch_type);
    }
    let batches = query
        .fetch_arrow()
        .await
        .map_err(|e| PipelineError::Execution(e.to_string()))?;

    let elapsed = t.elapsed();
    // TODO: capture read_rows/read_bytes/memory_usage stats here.
    // The clickhouse-rs crate discards X-ClickHouse-Summary headers so we
    // need a different approach. See https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/640
    let result_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64;

    let execution = QueryExecution {
        label: "base".into(),
        rendered_sql: rendered_sql.into(),
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

    Ok((batches, execution))
}

async fn execute_profiled(
    client: &ArrowClickHouseClient,
    sql: &str,
    params: &std::collections::HashMap<String, gkg_utils::clickhouse::ParamValue>,
    rendered_sql: &str,
    profiling: &ProfilingConfig,
    t: Instant,
) -> Result<(Vec<arrow::record_batch::RecordBatch>, QueryExecution), PipelineError> {
    let http_params: Vec<(String, String)> = params
        .iter()
        .map(|(k, v)| (k.clone(), v.render_http_param()))
        .collect();

    let (batches, query_stats) = client
        .profiler()
        .execute_with_stats(sql, &http_params, &[])
        .await
        .map_err(|e| PipelineError::Execution(e.to_string()))?;

    let elapsed = t.elapsed();

    let mut execution = QueryExecution {
        label: "base".into(),
        rendered_sql: rendered_sql.into(),
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

    if profiling.explain {
        execution.explain_plan = client.profiler().explain_plan(rendered_sql).await.ok();
        execution.explain_pipeline = client.profiler().explain_pipeline(rendered_sql).await.ok();
    }

    if profiling.query_log
        && let Ok(Some(entry)) = client
            .profiler()
            .fetch_query_log(&query_stats.query_id)
            .await
    {
        execution.query_log = Some(serde_json::to_value(&entry).unwrap_or_default());
    }

    if profiling.processors
        && let Ok(profiles) = client
            .profiler()
            .fetch_processors_profile(&query_stats.query_id)
            .await
        && !profiles.is_empty()
    {
        execution.processors = Some(serde_json::to_value(&profiles).unwrap_or_default());
    }

    Ok((batches, execution))
}
