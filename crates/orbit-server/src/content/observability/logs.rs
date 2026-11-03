//! Resolves a job's log tail from a tenant's Observability ClickHouse.
//!
//! Logs carry no CI attributes of their own; they are correlated to a job by
//! the span the job emitted. Resolving a log tail is therefore two steps:
//! find the job's span, then read the log lines carrying that span ID.

use std::collections::HashMap;

use clickhouse_client::ArrowClickHouseClient;
use orbit_arrow::ColumnValue;

use super::client_pool::TenantClientPool;
use super::host_map::TenantRoute;
use super::traces::{self, TraceLookup};

pub const JOB_LOG_TAIL: &str = "job_log_tail";

/// Properties are a single string column, so the lines are newline-joined.
const LINE_SEPARATOR: &str = "\n";

pub fn build_sql() -> String {
    format!(
        "SELECT span_id AS entity_id, arrayStringConcat(groupArray(body), {separator:?}) AS value \
         FROM ( \
           SELECT span_id, body FROM logs_v2 \
           WHERE span_id IN {{span_ids:Array(String)}} \
           ORDER BY timestamp DESC \
           LIMIT {{max_lines:UInt32}} BY span_id \
         ) \
         GROUP BY entity_id",
        separator = LINE_SEPARATOR
    )
}

pub fn column_value(raw: String) -> Option<ColumnValue> {
    (!raw.is_empty()).then_some(ColumnValue::String(raw))
}

/// Returns log tails keyed by job ID. Jobs with no span, or a span with no
/// logs, are absent from the map and resolve to null.
pub async fn fetch(
    pool: &TenantClientPool,
    route: &TenantRoute,
    job_ids: &[String],
    max_lines: u32,
) -> Result<HashMap<String, String>, clickhouse_client::ClickHouseError> {
    let spans_by_job = traces::fetch(pool, route, TraceLookup::JobSpanId, job_ids).await?;
    if spans_by_job.is_empty() {
        return Ok(HashMap::new());
    }

    let span_ids: Vec<String> = spans_by_job.values().cloned().collect();
    let client: ArrowClickHouseClient = pool.logs_client(route);

    let batches = client
        .query(&build_sql())
        .param("span_ids", &span_ids)
        .param("max_lines", max_lines)
        .fetch_arrow()
        .await?;

    let tails_by_span = super::rows::two_string_columns(&batches);

    Ok(spans_by_job
        .into_iter()
        .filter_map(|(job_id, span_id)| {
            tails_by_span
                .get(&span_id)
                .map(|tail| (job_id, tail.clone()))
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sql_parameterises_the_span_ids_and_line_limit() {
        let sql = build_sql();

        assert!(sql.contains("{span_ids:Array(String)}"));
        assert!(sql.contains("{max_lines:UInt32}"));
    }

    #[test]
    fn sql_limits_lines_per_span_rather_than_overall() {
        assert!(build_sql().contains("LIMIT {max_lines:UInt32} BY span_id"));
    }

    #[test]
    fn sql_reads_the_most_recent_lines_first() {
        assert!(build_sql().contains("ORDER BY timestamp DESC"));
    }

    #[test]
    fn a_tail_resolves_to_a_string() {
        assert_eq!(
            column_value("line one\nline two".to_string()),
            Some(ColumnValue::String("line one\nline two".to_string()))
        );
    }

    #[test]
    fn an_empty_tail_resolves_to_nothing() {
        assert!(column_value(String::new()).is_none());
    }
}
