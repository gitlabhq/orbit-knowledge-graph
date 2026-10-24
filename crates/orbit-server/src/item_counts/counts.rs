use std::collections::HashMap;

use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use futures::{StreamExt, TryStreamExt};
use orbit_server_config::QueryConfig;
use orbit_utils::traversal_path::TraversalPath;
use tonic::Status;

use super::visibility::{Visibility, VisibleEntity};
use crate::status_query::{
    bind_prefix_parameters, build_prefix_match_condition, fetch_status_query_batches,
    map_column_extraction_error,
};

const COUNT_CACHE_TTL_SECONDS: u32 = 5;
const MAX_CONCURRENT_COUNT_QUERIES: usize = 8;
const PARALLEL_REPLICA_SETTINGS: [(&str, &str); 3] = [
    ("enable_parallel_replicas", "1"),
    (
        "parallel_replicas_min_number_of_rows_per_replica",
        "50000000",
    ),
    ("cancel_http_readonly_queries_on_client_close", "1"),
];

pub type ScopeCounts = HashMap<String, HashMap<String, i64>>;

pub async fn count_visible_entities(
    client: &ArrowClickHouseClient,
    scopes: &[&TraversalPath],
    visibility: &Visibility,
) -> Result<ScopeCounts, Status> {
    futures::stream::iter(scopes.iter().enumerate())
        .map(|(index, scope)| count_scope(client, scope, visibility.entities_in_scope(index)))
        .buffer_unordered(MAX_CONCURRENT_COUNT_QUERIES)
        .try_collect()
        .await
}

async fn count_scope(
    client: &ArrowClickHouseClient,
    scope: &TraversalPath,
    entities: Vec<&VisibleEntity>,
) -> Result<(String, HashMap<String, i64>), Status> {
    if entities.is_empty() {
        return Ok((scope.as_str().to_string(), HashMap::new()));
    }
    let sql = entities
        .iter()
        .enumerate()
        .map(|(index, entity)| build_entity_count_query(index, entity))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let batches = fetch_status_query_batches(
        client,
        &sql,
        "entity counts",
        &count_query_config(),
        |query| {
            let query = (0..entities.len()).fold(query, |query, index| {
                bind_prefix_parameters(query, &build_scope_parameter_name(index), &[scope])
            });
            PARALLEL_REPLICA_SETTINGS
                .into_iter()
                .fold(query, |query, (name, value)| {
                    query.with_setting(name, value)
                })
        },
    )
    .await?;

    let names = String::extract_column(&batches, 0).map_err(map_column_extraction_error)?;
    let counts = i64::extract_column(&batches, 1).map_err(map_column_extraction_error)?;
    Ok((
        scope.as_str().to_string(),
        names.into_iter().zip(counts).collect(),
    ))
}

fn build_entity_count_query(index: usize, entity: &VisibleEntity) -> String {
    let in_scopes =
        build_prefix_match_condition("d.traversal_path", &build_scope_parameter_name(index), 1);
    format!(
        "SELECT '{name}' AS entity, toInt64(uniqIf(d.id, d._deleted = 0)) AS cnt \
           FROM {table} AS d \
          WHERE {in_scopes}",
        name = entity.name,
        table = entity.table,
    )
}

fn build_scope_parameter_name(index: usize) -> String {
    format!("scope_{index}")
}

fn count_query_config() -> QueryConfig {
    QueryConfig {
        query_cache_ttl: Some(COUNT_CACHE_TTL_SECONDS),
        ..orbit_server_config::query::default_config()
    }
}
