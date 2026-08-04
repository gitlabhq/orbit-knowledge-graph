use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use clickhouse_client::{ArrowClickHouseClient, QuerySummary};

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};
use query_engine::shared::{
    ExecutionOutput, QueryExecution, QueryExecutionLog, QueryExecutionStats,
};

use crate::pipeline::correlation;

struct PreparedQuery {
    sql: String,
    params: HashMap<String, gkg_utils::clickhouse::ParamValue>,
    rendered_sql: String,
    query_id: String,
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
            .ok_or_else(|| PipelineError::Execution("ClickHouse client not available".into()))
            .inspect_err(|e| obs.record_error(e))?;

        let user_id = ctx
            .server_extensions
            .get::<crate::auth::Claims>()
            .map(|c| c.user_id)
            .unwrap_or(0);
        let log_comment = correlation::log_comment_base(user_id, &ctx.query_json);

        let (prepared, result_context) = {
            let compiled = ctx.compiled().inspect_err(|e| obs.record_error(e))?;

            let mut http_settings: Vec<(String, String)> = compiled
                .base
                .query_config
                .to_clickhouse_settings()
                .map_err(PipelineError::Execution)
                .inspect_err(|e| obs.record_error(e))?;

            let query_id = correlation::query_id("base");
            http_settings.push(("query_id".to_string(), query_id.clone()));
            http_settings.push(("log_comment".to_string(), log_comment));

            let prepared = PreparedQuery {
                sql: compiled.base.sql.clone(),
                params: compiled.base.params.clone(),
                rendered_sql: compiled.base.render(),
                query_id,
                http_settings,
            };
            (prepared, compiled.base.result_context.clone())
        };

        let (batches, execution) = execute_query(client, &prepared, start)
            .await
            .inspect_err(|e| obs.record_error(e))?;

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
        query = query.with_setting(k, v);
    }
    for (key, param) in prepared.params.iter() {
        query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.ch_type);
    }
    let (batches, summary) = query
        .fetch_arrow_with_summary()
        .await
        .map_err(|e| PipelineError::Execution(e.to_string()))?;

    let elapsed = start.elapsed();
    let result_rows = batches.iter().map(|b| b.num_rows()).sum::<usize>() as u64;

    let stats = apply_summary(
        QueryExecutionStats {
            result_rows,
            elapsed_ns: elapsed.as_nanos() as u64,
            ..Default::default()
        },
        summary.as_ref(),
    );

    let execution = QueryExecution {
        label: "base".into(),
        rendered_sql: prepared.rendered_sql.clone(),
        query_id: prepared.query_id.clone(),
        elapsed_ms: elapsed.as_secs_f64() * 1000.0,
        stats,
        explain_plan: None,
        explain_pipeline: None,
        query_log: None,
        processors: None,
    };

    Ok((batches, execution))
}

/// Stats come from the `X-ClickHouse-Summary` HTTP header.
pub(crate) fn apply_summary(
    mut stats: QueryExecutionStats,
    summary: Option<&QuerySummary>,
) -> QueryExecutionStats {
    if let Some(s) = summary {
        if stats.read_rows == 0 {
            stats.read_rows = s.read_rows().unwrap_or(0);
        }
        if stats.read_bytes == 0 {
            stats.read_bytes = s.read_bytes().unwrap_or(0);
        }
        if stats.memory_usage == 0 {
            stats.memory_usage = i64::try_from(s.memory_usage().unwrap_or(0)).unwrap_or(i64::MAX);
        }
        // Prefer ClickHouse server elapsed over the caller's local wall time.
        if let Some(ns) = s.elapsed_ns() {
            stats.elapsed_ns = ns;
        }
    }
    stats
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_summary_no_op_when_none() {
        let stats = QueryExecutionStats {
            read_rows: 0,
            read_bytes: 0,
            result_rows: 42,
            result_bytes: 0,
            elapsed_ns: 1000,
            memory_usage: 0,
        };
        let result = apply_summary(stats, None);
        assert_eq!(result.read_rows, 0);
        assert_eq!(result.read_bytes, 0);
        assert_eq!(result.memory_usage, 0);
        assert_eq!(result.result_rows, 42);
    }
}
