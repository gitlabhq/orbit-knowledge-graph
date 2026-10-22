use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse_client::{ArrowClickHouseClient, QuerySummary};
use query_engine::compiler::ParameterizedQuery;

use query_engine::pipeline::{
    PipelineError, PipelineObserver, PipelineStage, QueryPipelineContext,
};
use query_engine::shared::{
    ExecutionOutput, QueryExecution, QueryExecutionLog, QueryExecutionStats,
};

use crate::pipeline::correlation;
use crate::pipeline::service::QuerySettings;

struct PreparedQuery {
    label: &'static str,
    sql: String,
    params: HashMap<String, orbit_utils::query_types::ParamValue>,
    rendered_sql: String,
    query_id: String,
    http_settings: Vec<(String, String)>,
}

impl PreparedQuery {
    fn new(
        label: &'static str,
        query: &ParameterizedQuery,
        log_comment: &str,
        settings: &[(String, String)],
    ) -> Self {
        let query_id = correlation::query_id(label);
        let mut http_settings = vec![
            ("query_id".to_string(), query_id.clone()),
            ("log_comment".to_string(), log_comment.to_string()),
        ];
        http_settings.extend(settings.iter().cloned());
        Self {
            label,
            sql: query.sql.clone(),
            params: query.params.clone(),
            rendered_sql: query.render(),
            query_id,
            http_settings,
        }
    }
}

#[derive(Clone)]
pub struct ClickHouseExecutor {
    pub migration_version: u32,
}

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
            .cloned()
            .ok_or_else(|| PipelineError::Execution("ClickHouse client not available".into()))
            .inspect_err(|e| obs.record_error(e))?;

        let user_id = ctx
            .server_extensions
            .get::<crate::auth::Claims>()
            .map(|c| c.user_id)
            .unwrap_or(0);
        let log_comment = correlation::log_comment_base(
            user_id,
            &ctx.query_json,
            ctx.frontend,
            self.migration_version,
        );
        let settings = ctx
            .phases
            .get::<QuerySettings>()
            .map(|QuerySettings(settings)| settings.clone())
            .unwrap_or_default();

        let compiled = ctx.compiled().inspect_err(|e| obs.record_error(e))?;
        let base = PreparedQuery::new("base", &compiled.base, &log_comment, &settings);
        let mut result_context = compiled.base.result_context.clone();

        let (mut batches, execution) = execute_query(&client, &base, start)
            .await
            .inspect_err(|e| obs.record_error(e))?;
        let fallback = compiled.fallback_for(rows(&batches)).map(|query| {
            (
                PreparedQuery::new("fallback", query, &log_comment, &settings),
                query.result_context.clone(),
            )
        });
        record(ctx, obs, execution, batches.len());
        if let Some((fallback, context)) = fallback {
            let (full, execution) = execute_query(&client, &fallback, Instant::now())
                .await
                .inspect_err(|e| obs.record_error(e))?;
            record(ctx, obs, execution, full.len());
            batches = full;
            result_context = context;
        }

        Ok(ExecutionOutput {
            batches,
            result_context,
        })
    }
}

fn rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|batch| batch.num_rows()).sum()
}

fn record(
    ctx: &mut QueryPipelineContext,
    obs: &mut dyn PipelineObserver,
    execution: QueryExecution,
    batch_count: usize,
) {
    obs.executed(
        Duration::from_secs_f64(execution.elapsed_ms / 1000.0),
        batch_count,
    );
    obs.query_executed(
        &execution.label,
        execution.stats.read_rows,
        execution.stats.read_bytes,
        execution.stats.memory_usage,
    );
    ctx.phases
        .get_or_insert_default::<QueryExecutionLog>()
        .0
        .push(execution);
}

async fn execute_query(
    client: &ArrowClickHouseClient,
    prepared: &PreparedQuery,
    start: Instant,
) -> Result<(Vec<arrow::record_batch::RecordBatch>, QueryExecution), PipelineError> {
    let mut query = client
        .query(&prepared.sql)
        .with_settings(prepared.http_settings.clone());
    for (key, param) in prepared.params.iter() {
        query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.data_type);
    }
    let (batches, summary) = query
        .fetch_arrow_with_summary()
        .await
        .map_err(|e| PipelineError::Execution(e.to_string()))?;

    let elapsed = start.elapsed();
    let result_rows = rows(&batches) as u64;

    let stats = apply_summary(
        QueryExecutionStats {
            result_rows,
            elapsed_ns: elapsed.as_nanos() as u64,
            ..Default::default()
        },
        summary.as_ref(),
    );

    let execution = QueryExecution {
        label: prepared.label.into(),
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
