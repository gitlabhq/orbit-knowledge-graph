use std::collections::{BTreeSet, HashMap};

use arrow::record_batch::RecordBatch;
use clickhouse_client::{ArrowClickHouseClient, ArrowQuery, FromArrowColumn};
use orbit_utils::traversal_path::TraversalPath;
use tonic::Status;

use super::visibility::{Visibility, VisibleEntity};
use orbit_server_config::QueryConfig;

use crate::status_query::{
    bind_prefix_parameters, build_prefix_match_condition, fetch_status_query_batches,
    map_column_extraction_error,
};

const SCOPE_PARAMETER: &str = "scope";
const SCOPE_GROUP_PARAMETER: &str = "scope_group";
const COUNT_CACHE_TTL_SECONDS: u32 = 5;

pub type ScopeCounts = HashMap<String, HashMap<String, i64>>;

pub async fn count_visible_entities(
    client: &ArrowClickHouseClient,
    scopes: &[&TraversalPath],
    visibility: &Visibility,
) -> Result<ScopeCounts, Status> {
    let sql = build_count_query(scopes, visibility);
    let batches = fetch_status_query_batches(
        client,
        &sql,
        "entity counts",
        &count_query_config(),
        |query| bind_count_parameters(query, scopes, visibility),
    )
    .await?;
    group_counts_by_scope(&batches)
}

fn build_count_query(scopes: &[&TraversalPath], visibility: &Visibility) -> String {
    visibility
        .entities
        .iter()
        .map(|entity| build_entity_count_query(entity, visibility.scope_indexes(entity), scopes))
        .collect::<Vec<_>>()
        .join(" UNION ALL ")
}

fn build_entity_count_query(
    entity: &VisibleEntity,
    scope_indexes: &[usize],
    scopes: &[&TraversalPath],
) -> String {
    let (name, table) = (&entity.name, &entity.table);
    let in_scopes = build_prefix_match_condition(
        "d.traversal_path",
        SCOPE_PARAMETER,
        scope_indexes.iter().copied(),
    );
    if let [scope] = scope_indexes {
        return format!(
            "SELECT '{name}' AS entity, {{{SCOPE_PARAMETER}_{scope}:String}} AS scope, \
                    toInt64(uniqIf(d.id, d._deleted = 0)) AS cnt \
               FROM {table} AS d \
              WHERE {in_scopes}"
        );
    }

    let prefixes: Vec<String> = scope_indexes
        .iter()
        .map(|&index| scopes[index].as_str().len())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .map(|length| format!("left(d.traversal_path, {length})"))
        .collect();
    format!(
        "SELECT '{name}' AS entity, scope, toInt64(uniqIf(d.id, d._deleted = 0)) AS cnt \
           FROM {table} AS d \
          ARRAY JOIN [{prefixes}] AS scope \
          WHERE indexHint({in_scopes}) \
            AND scope IN {{{SCOPE_GROUP_PARAMETER}_{group}:Array(String)}} \
          GROUP BY scope",
        prefixes = prefixes.join(", "),
        group = entity.scope_group,
    )
}

fn bind_count_parameters(
    query: ArrowQuery,
    scopes: &[&TraversalPath],
    visibility: &Visibility,
) -> ArrowQuery {
    let query = bind_prefix_parameters(query, SCOPE_PARAMETER, scopes);
    visibility
        .scope_groups
        .iter()
        .enumerate()
        .filter(|(_, group)| group.len() > 1)
        .fold(query, |query, (index, group)| {
            let group_scopes: Vec<&str> =
                group.iter().map(|&scope| scopes[scope].as_str()).collect();
            query.param(&format!("{SCOPE_GROUP_PARAMETER}_{index}"), group_scopes)
        })
}

fn group_counts_by_scope(batches: &[RecordBatch]) -> Result<ScopeCounts, Status> {
    let names = String::extract_column(batches, 0).map_err(map_column_extraction_error)?;
    let scopes = String::extract_column(batches, 1).map_err(map_column_extraction_error)?;
    let counts = i64::extract_column(batches, 2).map_err(map_column_extraction_error)?;
    let mut by_scope = ScopeCounts::new();
    for ((name, scope), count) in names.into_iter().zip(scopes).zip(counts) {
        by_scope.entry(scope).or_default().insert(name, count);
    }
    Ok(by_scope)
}

fn count_query_config() -> QueryConfig {
    QueryConfig {
        query_cache_ttl: Some(COUNT_CACHE_TTL_SECONDS),
        ..orbit_server_config::query::default_config()
    }
}
