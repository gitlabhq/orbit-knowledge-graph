use std::collections::HashMap;
use std::sync::Arc;

use crate::seeding::catalog;
use anyhow::{Context, Result};
use arrow::datatypes::{DataType, Field, Schema};
use clickhouse_client::ArrowClickHouseClient;
use tracing::warn;

pub struct SchemaRegistry {
    schemas: HashMap<String, Arc<Schema>>,
}

impl SchemaRegistry {
    pub async fn from_clickhouse(client: &ArrowClickHouseClient, database: &str) -> Result<Self> {
        let mut schemas = HashMap::new();

        for table_name in catalog::seeding_table_names() {
            match fetch_table_schema(client, database, table_name).await {
                Ok(schema) => {
                    schemas.insert(table_name.to_string(), Arc::new(schema));
                }
                Err(error) => {
                    warn!(table = table_name, %error, "table not found in ClickHouse, skipping");
                }
            }
        }

        Ok(Self { schemas })
    }

    pub fn seedable_tables(&self) -> Vec<(&str, Arc<Schema>)> {
        catalog::seeding_table_names()
            .into_iter()
            .filter_map(|name| {
                self.schemas
                    .get(name)
                    .map(|schema| (name, Arc::clone(schema)))
            })
            .collect()
    }

    pub fn schema_for_table(&self, name: &str) -> Option<Arc<Schema>> {
        self.schemas.get(name).cloned()
    }
}

async fn fetch_table_schema(
    client: &ArrowClickHouseClient,
    database: &str,
    table_name: &str,
) -> Result<Schema> {
    let query = format!(
        "SELECT name, type FROM system.columns WHERE database = '{}' AND table = '{}' ORDER BY position",
        database, table_name
    );

    let batches = client
        .query_arrow(&query)
        .await
        .with_context(|| format!("failed to query schema for {table_name}"))?;

    let mut fields = Vec::new();
    for batch in &batches {
        let name_column = batch
            .column_by_name("name")
            .context("missing 'name' column in system.columns")?;

        let type_column = batch
            .column_by_name("type")
            .context("missing 'type' column in system.columns")?;

        let names = arrow::array::as_string_array(name_column);
        let types = arrow::array::as_string_array(type_column);

        for row in 0..batch.num_rows() {
            let column_name = names.value(row);
            let column_type = types.value(row);
            let (data_type, nullable) = parse_clickhouse_type(column_type);
            fields.push(Field::new(column_name, data_type, nullable));
        }
    }

    if fields.is_empty() {
        anyhow::bail!("no columns found for table {table_name}");
    }

    Ok(Schema::new(fields))
}

fn parse_clickhouse_type(type_str: &str) -> (DataType, bool) {
    if let Some(inner) = type_str
        .strip_prefix("Nullable(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let (data_type, _) = parse_clickhouse_type(inner);
        return (data_type, true);
    }

    if let Some(inner) = type_str
        .strip_prefix("LowCardinality(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let (data_type, nullable) = parse_clickhouse_type(inner);
        return (data_type, nullable);
    }

    if let Some(inner) = type_str
        .strip_prefix("Array(")
        .and_then(|s| s.strip_suffix(')'))
    {
        let (element_type, element_nullable) = parse_clickhouse_type(inner);
        return (
            DataType::List(Arc::new(Field::new("item", element_type, element_nullable))),
            false,
        );
    }

    let data_type = match type_str {
        "Int64" | "UInt64" => DataType::Int64,
        "Int8" | "Int16" | "UInt8" | "UInt16" => DataType::Int8,
        "Float64" => DataType::Float64,
        "String" => DataType::Utf8,
        "Bool" => DataType::Boolean,
        "Date32" => DataType::Date32,
        t if t.starts_with("DateTime64") => DataType::Int64,
        other => {
            warn!(
                clickhouse_type = other,
                "unmapped ClickHouse type, defaulting to Utf8"
            );
            DataType::Utf8
        }
    };

    (data_type, false)
}
