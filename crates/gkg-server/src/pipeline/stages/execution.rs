use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use clickhouse_client::{ArrowClickHouseClient, ProfilingConfig};

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};
use query_engine::shared::{
    ExecutionOutput, QueryExecution, QueryExecutionLog, QueryExecutionStats,
};

/// Everything needed to execute a compiled query against ClickHouse.
struct PreparedQuery {
    sql: String,
    params: HashMap<String, gkg_utils::clickhouse::ParamValue>,
    rendered_sql: String,
    /// HTTP-level ClickHouse settings applied to every query.
    /// Includes `log_comment` for tracing and the `QueryConfig` settings
    /// as defense-in-depth (they're also in the SQL SETTINGS clause).
    http_settings: Vec<(String, String)>,
}

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
        let start = Instant::now();
        let client = ctx
            .server_extensions
            .get::<Arc<ArrowClickHouseClient>>()
            .ok_or_else(|| PipelineError::Execution("ClickHouse client not available".into()))?;
        let profiling = ctx
            .server_extensions
            .get::<ProfilingConfig>()
            .cloned()
            .unwrap_or_default();

        let (prepared, result_context) = {
            let compiled = ctx.compiled()?;

            // Build HTTP-level settings from QueryConfig (defense-in-depth:
            // these are also baked into the SQL SETTINGS clause by codegen)
            // plus log_comment for tracing.
            let mut http_settings: Vec<(String, String)> = compiled
                .base
                .query_config
                .to_clickhouse_settings()
                .map_err(PipelineError::Execution)?;

            let log_comment = match labkit::correlation::current() {
                Some(id) => format!("gkg;correlation_id={id}"),
                None => "gkg".to_string(),
            };
            http_settings.push(("log_comment".to_string(), log_comment));

            let prepared = PreparedQuery {
                sql: compiled.base.sql.clone(),
                params: compiled.base.params.clone(),
                rendered_sql: compiled.base.render(),
                http_settings,
            };
            (prepared, compiled.base.result_context.clone())
        };

        let (batches, execution) = if profiling.enabled {
            execute_profiled(client, &prepared, &profiling, start).await?
        } else {
            execute_standard(client, &prepared, start).await?
        };

        let elapsed = start.elapsed();
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
    prepared: &PreparedQuery,
    start: Instant,
) -> Result<(Vec<arrow::record_batch::RecordBatch>, QueryExecution), PipelineError> {
    let mut query = client.query(&prepared.sql);
    for (k, v) in &prepared.http_settings {
        query = query.with_option(k, v);
    }
    for (key, param) in prepared.params.iter() {
        query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.ch_type);
    }
    let batches = query
        .fetch_arrow()
        .await
        .map_err(|e| PipelineError::Execution(e.to_string()))?;

    let elapsed = start.elapsed();
    // TODO: capture read_rows/read_bytes/memory_usage stats here.
    // The clickhouse-rs crate discards X-ClickHouse-Summary headers so we
    // need a different approach. See https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/640
    let result_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64;

    let execution = QueryExecution {
        label: "base".into(),
        rendered_sql: prepared.rendered_sql.clone(),
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
    prepared: &PreparedQuery,
    profiling: &ProfilingConfig,
    start: Instant,
) -> Result<(Vec<arrow::record_batch::RecordBatch>, QueryExecution), PipelineError> {
    let http_params: Vec<(String, String)> = prepared
        .params
        .iter()
        .map(|(k, v)| (k.clone(), v.render_http_param()))
        .collect();

    let extra_settings: Vec<(&str, &str)> = prepared
        .http_settings
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();

    let (batches, query_stats) = client
        .profiler()
        .execute_with_stats(&prepared.sql, &http_params, &extra_settings)
        .await
        .map_err(|e| PipelineError::Execution(e.to_string()))?;

    let elapsed = start.elapsed();

    let mut execution = QueryExecution {
        label: "base".into(),
        rendered_sql: prepared.rendered_sql.clone(),
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
        execution.explain_plan = client
            .profiler()
            .explain_plan(&prepared.rendered_sql)
            .await
            .ok();
        execution.explain_pipeline = client
            .profiler()
            .explain_pipeline(&prepared.rendered_sql)
            .await
            .ok();
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
