use arrow::record_batch::RecordBatch;
use clickhouse_client::{ArrowClickHouseClient, ArrowQuery};
use orbit_log_fields::debug;
use orbit_server_config::QueryConfig;
use serde::Serialize;
use tonic::Status;

pub(crate) async fn fetch_status_query_batches(
    client: &ArrowClickHouseClient,
    sql: &str,
    label: &str,
    config: &QueryConfig,
    bind_params: impl FnOnce(ArrowQuery) -> ArrowQuery,
) -> Result<Vec<RecordBatch>, Status> {
    let settings = config
        .to_http_settings()
        .map_err(|e| Status::internal(format!("query settings error ({label}): {e}")))?;

    debug!(
        expanded(SQL) = sql,
        safe(QUERY_NAME) = label,
        "Status query"
    );

    bind_params(client.query(sql).with_settings(settings))
        .fetch_arrow()
        .await
        .map_err(|e| Status::internal(format!("ClickHouse error ({label}): {e}")))
}

pub(crate) fn build_prefix_match_condition(
    column: &str,
    param: &str,
    prefix_count: usize,
) -> String {
    let conditions: Vec<String> = (0..prefix_count)
        .map(|index| format!("startsWith({column}, {{{param}_{index}:String}})"))
        .collect();
    format!("({})", conditions.join(" OR "))
}

pub(crate) fn bind_prefix_parameters(
    query: ArrowQuery,
    param: &str,
    prefixes: &[impl Serialize],
) -> ArrowQuery {
    prefixes
        .iter()
        .enumerate()
        .fold(query, |query, (index, prefix)| {
            query.param(&format!("{param}_{index}"), prefix)
        })
}

pub(crate) fn map_column_extraction_error(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

pub(crate) fn uncached_query_config() -> QueryConfig {
    let mut config = orbit_server_config::query::default_config();
    config.set("use_query_cache", false);
    config
}
