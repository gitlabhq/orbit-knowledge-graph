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

struct PreparedQuery {
    sql: String,
    params: HashMap<String, gkg_utils::clickhouse::ParamValue>,
    rendered_sql: String,
    http_settings: Vec<(String, String)>,
    /// Set when profiling is enabled. Used to find the query in
    /// system.query_log after execution.
    profiling_id: Option<String>,
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

            let mut http_settings: Vec<(String, String)> = compiled
                .base
                .query_config
                .to_clickhouse_settings()
                .map_err(PipelineError::Execution)?;

            // Build log_comment with correlation ID for tracing.
            // When profiling is enabled, append a unique profiling_id so
            // the query can be found in system.query_log after execution.
            let profiling_id = if profiling.enabled {
                Some(uuid::Uuid::new_v4().to_string())
            } else {
                None
            };

            let mut log_comment = match labkit::correlation::current() {
                Some(id) => format!("gkg;correlation_id={id}"),
                None => "gkg".to_string(),
            };
            if let Some(ref pid) = profiling_id {
                log_comment.push_str(&format!(";profiling_id={pid}"));
            }
            http_settings.push(("log_comment".to_string(), log_comment));

            let prepared = PreparedQuery {
                sql: compiled.base.sql.clone(),
                params: compiled.base.params.clone(),
                rendered_sql: compiled.base.render(),
                http_settings,
                profiling_id,
            };
            (prepared, compiled.base.result_context.clone())
        };

        let (batches, mut execution) = execute_query(client, &prepared, start).await?;

        if let Some(ref profiling_id) = prepared.profiling_id {
            enrich_execution(
                client,
                &mut execution,
                &prepared.rendered_sql,
                &profiling,
                profiling_id,
            )
            .await;
        }

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

async fn execute_query(
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

/// Enrich a QueryExecution with data from ClickHouse system tables.
async fn enrich_execution(
    client: &ArrowClickHouseClient,
    execution: &mut QueryExecution,
    rendered_sql: &str,
    profiling: &ProfilingConfig,
    profiling_id: &str,
) {
    if profiling.explain {
        execution.explain_plan = client.explain_plan(rendered_sql).await.ok();
        execution.explain_pipeline = client.explain_pipeline(rendered_sql).await.ok();
    }

    if (profiling.query_log || profiling.processors)
        && let Ok(Some(entry)) = client.fetch_query_log(profiling_id).await
    {
        let query_id = entry.query_id.clone();
        execution.query_id = query_id.clone();
        execution.stats.read_rows = entry.read_rows;
        execution.stats.read_bytes = entry.read_bytes;
        execution.stats.result_rows = entry.result_rows;
        execution.stats.result_bytes = entry.result_bytes;
        execution.stats.memory_usage = entry.memory_usage as i64;

        if profiling.query_log {
            execution.query_log = Some(serde_json::to_value(&entry).unwrap_or_default());
        }

        if profiling.processors
            && let Ok(profiles) = client.fetch_processors_profile(&query_id).await
            && !profiles.is_empty()
        {
            execution.processors = Some(serde_json::to_value(&profiles).unwrap_or_default());
        }
    }
}
