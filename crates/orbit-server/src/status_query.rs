use arrow::record_batch::RecordBatch;
use clickhouse_client::{ArrowClickHouseClient, ArrowQuery};
use orbit_server_config::QueryConfig;
use serde::Serialize;
use tonic::Status;
use tracing::debug;

#[derive(Clone, Copy)]
pub(crate) enum QueryCache {
    Use { ttl_secs: u32 },
    Skip,
}

pub(crate) async fn fetch_status_query_batches(
    client: &ArrowClickHouseClient,
    sql: &str,
    label: &str,
    cache: QueryCache,
    bind_params: impl FnOnce(ArrowQuery) -> ArrowQuery,
) -> Result<Vec<RecordBatch>, Status> {
    let sql = append_query_settings(sql, cache)
        .map_err(|e| Status::internal(format!("query settings error ({label}): {e}")))?;

    debug!(sql, label, "Status query");

    bind_params(client.query(&sql))
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

fn append_query_settings(sql: &str, cache: QueryCache) -> Result<String, String> {
    let defaults = orbit_server_config::query::default_config();
    let config = match cache {
        QueryCache::Use { ttl_secs } => QueryConfig {
            use_query_cache: Some(true),
            query_cache_ttl: Some(ttl_secs),
            ..defaults
        },
        QueryCache::Skip => QueryConfig {
            use_query_cache: Some(false),
            ..defaults
        },
    };
    let settings = config.to_clickhouse_settings()?;
    if settings.is_empty() {
        return Ok(sql.to_string());
    }
    let clause = settings
        .iter()
        .map(|(key, value)| format!("{key} = {value}"))
        .collect::<Vec<_>>()
        .join(", ");
    Ok(format!("{sql} SETTINGS {clause}"))
}
