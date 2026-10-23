use std::collections::{BTreeSet, HashMap};

use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use orbit_server_config::QueryConfig;
use tonic::Status;

use super::visibility::VisibleEntity;
use crate::status_query::{
    bind_prefix_parameters, build_prefix_array, build_prefix_match_condition,
    fetch_status_query_batches, map_column_extraction_error,
};

const SCOPE_PARAMETER: &str = "scope";
const COUNT_CACHE_TTL_SECONDS: u32 = 5;

pub type ScopeCounts = HashMap<String, HashMap<String, i64>>;

pub async fn count_visible_entities(
    client: &ArrowClickHouseClient,
    entities: &[VisibleEntity],
) -> Result<ScopeCounts, Status> {
    let scopes: Vec<&str> = entities
        .iter()
        .flat_map(|entity| entity.scopes.iter().map(String::as_str))
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let sql = entities
        .iter()
        .map(|entity| build_entity_count_query(entity, &scopes))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let batches = fetch_status_query_batches(
        client,
        &sql,
        "entity counts",
        &count_query_config(),
        |query| bind_prefix_parameters(query, SCOPE_PARAMETER, &scopes),
    )
    .await?;

    let names = String::extract_column(&batches, 0).map_err(map_column_extraction_error)?;
    let scopes = String::extract_column(&batches, 1).map_err(map_column_extraction_error)?;
    let counts = i64::extract_column(&batches, 2).map_err(map_column_extraction_error)?;
    let mut by_scope = ScopeCounts::new();
    for ((name, scope), count) in names.into_iter().zip(scopes).zip(counts) {
        by_scope.entry(scope).or_default().insert(name, count);
    }
    Ok(by_scope)
}

fn build_entity_count_query(entity: &VisibleEntity, scopes: &[&str]) -> String {
    let indices: BTreeSet<usize> = entity
        .scopes
        .iter()
        .filter_map(|scope| scopes.binary_search(&scope.as_str()).ok())
        .collect();
    let in_scopes =
        build_prefix_match_condition("d.traversal_path", SCOPE_PARAMETER, indices.iter().copied());
    let entity_scopes = build_prefix_array(SCOPE_PARAMETER, indices);
    format!(
        "SELECT '{name}' AS entity, scope, toInt64(uniqIf(d.id, d._deleted = 0)) AS cnt \
           FROM {table} AS d \
          ARRAY JOIN arrayFilter(p -> startsWith(d.traversal_path, p), {entity_scopes}) AS scope \
          WHERE {in_scopes} \
          GROUP BY scope",
        name = entity.name,
        table = entity.table,
    )
}

fn count_query_config() -> QueryConfig {
    QueryConfig {
        use_query_cache: Some(true),
        query_cache_ttl: Some(COUNT_CACHE_TTL_SECONDS),
        ..orbit_server_config::query::default_config()
    }
}
