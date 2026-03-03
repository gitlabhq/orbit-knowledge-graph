use arrow::array::Array;
use clickhouse_client::ClickHouseConfiguration;

use crate::ir::TableSchema;
use crate::ir::from_clickhouse::{DescribeRow, build_table_schema};

/// Query ClickHouse for the current schema of a specific table.
///
/// Returns `None` if the table does not exist.
pub async fn introspect_table(
    config: &ClickHouseConfiguration,
    table_name: &str,
) -> Result<Option<TableSchema>, crate::MigrationError> {
    let client = config.build_client();

    let tables = list_tables(config).await?;
    if !tables.contains(&table_name.to_string()) {
        return Ok(None);
    }

    let describe_rows = describe_table(&client, table_name).await?;
    let create_statement = show_create_table(&client, table_name).await?;
    let schema = build_table_schema(table_name, &describe_rows, &create_statement);

    Ok(Some(schema))
}

/// List all tables in the configured database.
pub async fn list_tables(
    config: &ClickHouseConfiguration,
) -> Result<Vec<String>, crate::MigrationError> {
    let client = config.build_client();
    let result = client
        .query("SHOW TABLES")
        .fetch_arrow()
        .await
        .map_err(crate::MigrationError::ClickHouse)?;

    let mut tables = Vec::new();
    for batch in &result {
        let column = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>();
        if let Some(array) = column {
            for i in 0..array.len() {
                tables.push(array.value(i).to_string());
            }
        }
    }
    Ok(tables)
}

async fn describe_table(
    client: &clickhouse_client::ArrowClickHouseClient,
    table_name: &str,
) -> Result<Vec<DescribeRow>, crate::MigrationError> {
    let sql = format!("DESCRIBE TABLE {table_name}");
    let result = client
        .query(&sql)
        .fetch_arrow()
        .await
        .map_err(crate::MigrationError::ClickHouse)?;

    let mut rows = Vec::new();
    for batch in &result {
        let name_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("DESCRIBE column 0 should be String");
        let type_col = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("DESCRIBE column 1 should be String");
        let default_type_col = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("DESCRIBE column 2 should be String");
        let default_expr_col = batch
            .column(3)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("DESCRIBE column 3 should be String");

        for i in 0..batch.num_rows() {
            rows.push(DescribeRow {
                name: name_col.value(i).to_string(),
                column_type: type_col.value(i).to_string(),
                default_type: default_type_col.value(i).to_string(),
                default_expression: default_expr_col.value(i).to_string(),
            });
        }
    }
    Ok(rows)
}

async fn show_create_table(
    client: &clickhouse_client::ArrowClickHouseClient,
    table_name: &str,
) -> Result<String, crate::MigrationError> {
    let sql = format!("SHOW CREATE TABLE {table_name}");
    let result = client
        .query(&sql)
        .fetch_arrow()
        .await
        .map_err(crate::MigrationError::ClickHouse)?;

    for batch in &result {
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .expect("SHOW CREATE TABLE column 0 should be String");
        if col.len() > 0 {
            return Ok(col.value(0).to_string());
        }
    }

    Err(crate::MigrationError::Introspection(format!(
        "SHOW CREATE TABLE {table_name} returned no rows"
    )))
}
