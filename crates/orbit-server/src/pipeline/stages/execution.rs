use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use clickhouse_client::{ArrowClickHouseClient, QuerySummary};

use query_engine::compiler::ParameterizedQuery;
use query_engine::compiler::dag::{Contract, Request};
use query_engine::shared::dag::{Executed, Executor, ExecutorError};
use query_engine::shared::{QueryExecution, QueryExecutionStats};

use crate::pipeline::correlation;

/// The ClickHouse executor the server registers for its graph connection. `label` names the
/// stage in query ids, log comments and the execution log.
pub struct ClickHouseExecutor {
    pub client: Arc<ArrowClickHouseClient>,
    pub label: String,
    pub log_comment: String,
    pub settings: Vec<(String, String)>,
}

#[async_trait]
impl Executor for ClickHouseExecutor {
    async fn execute(
        &self,
        request: &Request,
        contract: Option<&Contract>,
    ) -> Result<Executed, ExecutorError> {
        let statement = match request {
            Request::Query(compiled) => &compiled.base,
            Request::Hydration(query) => query.as_ref(),
            other => {
                return Err(ExecutorError(format!(
                    "the ClickHouse connection runs SQL, not {other:?}"
                )));
            }
        };
        let (batches, execution) = self.run(statement).await?;
        let batches = match contract {
            Some(contract) => query_engine::shared::dag::decode(batches, contract)?,
            None => batches,
        };
        Ok(Executed {
            batches,
            log: vec![execution],
        })
    }
}

impl ClickHouseExecutor {
    async fn run(
        &self,
        statement: &ParameterizedQuery,
    ) -> Result<(Vec<arrow::record_batch::RecordBatch>, QueryExecution), ExecutorError> {
        let start = Instant::now();
        let query_id = correlation::query_id(&self.label.replace(':', "-"));
        let mut query = self
            .client
            .query(&statement.sql)
            .with_setting("query_id", &query_id)
            .with_setting("log_comment", &self.log_comment)
            .with_settings(self.settings.clone());
        for (key, param) in &statement.params {
            query = ArrowClickHouseClient::bind_param(query, key, &param.value, &param.data_type);
        }
        let (batches, summary) = query
            .fetch_arrow_with_summary()
            .await
            .map_err(|error| ExecutorError(error.to_string()))?;
        let elapsed = start.elapsed();
        let result_rows = batches.iter().map(|batch| batch.num_rows()).sum::<usize>() as u64;
        let stats = apply_summary(
            QueryExecutionStats {
                result_rows,
                elapsed_ns: elapsed.as_nanos() as u64,
                ..Default::default()
            },
            summary.as_ref(),
        );
        let execution = QueryExecution {
            label: self.label.clone(),
            rendered_sql: statement.render(),
            query_id,
            elapsed_ms: elapsed.as_secs_f64() * 1000.0,
            stats,
            explain_plan: None,
            explain_pipeline: None,
            query_log: None,
            processors: None,
        };
        Ok((batches, execution))
    }
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
