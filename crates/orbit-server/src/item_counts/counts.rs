use std::collections::HashMap;

use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use tonic::Status;

use super::visibility::VisibleEntity;
use crate::status_query::{
    QueryCache, TRAVERSAL_PATH_PREFIXES, bind_prefix_parameters, build_prefix_match_condition,
    fetch_status_query_batches, map_column_extraction_error,
};

// Counting is expensive, so repeated requests reuse the cached result instead of counting again.
const COUNT_CACHE: QueryCache = QueryCache::Use { ttl_secs: 300 };

pub async fn count_visible_entities(
    client: &ArrowClickHouseClient,
    entities: &[VisibleEntity],
) -> Result<HashMap<String, HashMap<String, i64>>, Status> {
    let sql = entities
        .iter()
        .enumerate()
        .map(|(index, entity)| build_entity_count_query(index, entity))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let batches = fetch_status_query_batches(client, &sql, "entity counts", COUNT_CACHE, |query| {
        entities
            .iter()
            .enumerate()
            .fold(query, |query, (index, entity)| {
                let param = build_scope_parameter_name(index);
                let query = query.param(&param, &entity.scopes);
                bind_prefix_parameters(query, &param, &entity.scopes)
            })
    })
    .await?;

    let entities = String::extract_column(&batches, 0).map_err(map_column_extraction_error)?;
    let scopes = String::extract_column(&batches, 1).map_err(map_column_extraction_error)?;
    let counts = i64::extract_column(&batches, 2).map_err(map_column_extraction_error)?;
    let mut counts_by_scope: HashMap<String, HashMap<String, i64>> = HashMap::new();
    for (entity, (scope, count)) in entities.into_iter().zip(scopes.into_iter().zip(counts)) {
        counts_by_scope
            .entry(scope)
            .or_default()
            .insert(entity, count);
    }
    Ok(counts_by_scope)
}

fn build_entity_count_query(index: usize, entity: &VisibleEntity) -> String {
    let param = build_scope_parameter_name(index);
    let in_scopes = build_prefix_match_condition("traversal_path", &param, entity.scopes.len());
    format!(
        "SELECT '{name}' AS entity, scope, toInt64(uniqIf(id, _deleted = 0)) AS cnt \
           FROM (SELECT id, _deleted, {TRAVERSAL_PATH_PREFIXES} AS path_prefixes \
                   FROM {table} \
                  WHERE {in_scopes}) \
          ARRAY JOIN path_prefixes AS scope \
          WHERE scope IN {{{param}:Array(String)}} \
          GROUP BY scope",
        name = entity.name,
        table = entity.table,
    )
}

fn build_scope_parameter_name(index: usize) -> String {
    format!("scope_{index}")
}
