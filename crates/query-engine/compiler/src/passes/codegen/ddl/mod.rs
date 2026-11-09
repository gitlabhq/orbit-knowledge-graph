pub mod duckdb;

use query_data_model::implementations::duckdb::backend::DuckDbColumn;
use query_data_model::{Catalog, DataType, DuckDb};

use crate::ast::ddl::*;

pub fn generate_local_tables(catalog: &Catalog<DuckDb>) -> Vec<CreateTable> {
    catalog
        .tables()
        .map(|table| CreateTable {
            name: table.name.clone(),
            columns: table.columns.iter().map(column_def).collect(),
            indexes: vec![],
            projections: vec![],
            engine: Engine {
                name: String::new(),
                args: vec![],
            },
            partition_by: vec![],
            order_by: catalog
                .table_facts(&table.name)
                .map(|facts| facts.sort_key.clone())
                .unwrap_or_default(),
            primary_key: None,
            settings: vec![],
            ttl: None,
        })
        .collect()
}

fn column_def(column: &DuckDbColumn) -> ColumnDef {
    let column_type = column_type(column.data_type);
    let column_type = match column.nullable {
        true => ColumnType::Nullable(Box::new(column_type)),
        false => column_type,
    };
    let def = ColumnDef::new(&column.name, column_type);
    match &column.default {
        Some(default) => def.with_default(default),
        None => def,
    }
}

fn column_type(data_type: DataType) -> ColumnType {
    match data_type {
        DataType::String | DataType::Enum | DataType::Uuid => ColumnType::String,
        DataType::Int => ColumnType::Int64,
        DataType::Float => ColumnType::Float64,
        DataType::Bool => ColumnType::Bool,
        DataType::Date => ColumnType::Date32,
        DataType::DateTime => ColumnType::Timestamp {
            precision: 6,
            timezone: None,
        },
    }
}
