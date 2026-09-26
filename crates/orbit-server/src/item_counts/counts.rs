use std::collections::HashMap;

use clickhouse_client::{ArrowClickHouseClient, FromArrowColumn};
use tonic::Status;

use super::visibility::VisibleEntity;
use crate::status_query::{
    bind_prefixes, extraction_error, fetch_status_rows, starts_with_any_sql,
};

pub async fn count_entities(
    client: &ArrowClickHouseClient,
    entities: &[VisibleEntity],
) -> Result<HashMap<String, i64>, Status> {
    let sql = entities
        .iter()
        .enumerate()
        .map(|(index, entity)| entity_count_sql(index, entity))
        .collect::<Vec<_>>()
        .join(" UNION ALL ");
    let batches = fetch_status_rows(client, &sql, "entity counts", |query| {
        entities
            .iter()
            .enumerate()
            .fold(query, |query, (index, entity)| {
                bind_prefixes(query, &scope_param(index), &entity.scopes)
            })
    })
    .await?;

    let names = String::extract_column(&batches, 0).map_err(extraction_error)?;
    let counts = i64::extract_column(&batches, 1).map_err(extraction_error)?;
    Ok(names.into_iter().zip(counts).collect())
}

fn entity_count_sql(index: usize, entity: &VisibleEntity) -> String {
    let in_scopes =
        starts_with_any_sql("d.traversal_path", &scope_param(index), entity.scopes.len());
    format!(
        "SELECT '{name}' AS entity, toInt64(uniqIf(d.id, d._deleted = 0)) AS cnt \
           FROM {table} AS d \
          WHERE {in_scopes}",
        name = entity.name,
        table = entity.table,
    )
}

fn scope_param(index: usize) -> String {
    format!("scope_{index}")
}
