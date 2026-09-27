use std::collections::HashMap;

use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use tonic::Status;

use super::visibility::VisibleEntity;
use crate::status_query::{
    QueryCache, bind_prefix_parameters, build_prefix_match_condition, fetch_status_query_batches,
    map_column_extraction_error,
};

// One count can read tens of GiB.
const COUNT_CACHE: QueryCache = QueryCache::Use { ttl_secs: 300 };

pub async fn count_visible_entities(
    client: &ArrowClickHouseClient,
    entities: &[VisibleEntity],
) -> Result<HashMap<String, i64>, Status> {
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
                bind_prefix_parameters(query, &build_scope_parameter_name(index), &entity.scopes)
            })
    })
    .await?;

    let names = String::extract_column(&batches, 0).map_err(map_column_extraction_error)?;
    let counts = i64::extract_column(&batches, 1).map_err(map_column_extraction_error)?;
    Ok(names.into_iter().zip(counts).collect())
}

fn build_entity_count_query(index: usize, entity: &VisibleEntity) -> String {
    let in_scopes = build_prefix_match_condition(
        "d.traversal_path",
        &build_scope_parameter_name(index),
        entity.scopes.len(),
    );
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
