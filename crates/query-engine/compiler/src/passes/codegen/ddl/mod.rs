pub mod duckdb;

use ontology::{Ontology, StorageColumn};

use crate::ast::ddl::*;

pub fn generate_local_tables(ontology: &Ontology) -> Vec<CreateTable> {
    let mut tables: Vec<CreateTable> = Vec::new();

    for entity_name in ontology.local_entity_names() {
        if let Some(table) = build_local_node_table(ontology, entity_name) {
            tables.push(table);
        }
    }

    if let Some(table) = build_local_edge_table(ontology) {
        tables.push(table);
    }

    tables
}

fn storage_col_to_def(col: &StorageColumn) -> ColumnDef {
    let col_type = parse_column_type(&col.ch_type);
    let mut def = ColumnDef::new(&col.name, col_type);
    if let Some(ref default) = col.default {
        def = def.with_default(default);
    }
    if let Some(ref codecs) = col.codec {
        def = def.with_codec(codecs.iter().map(|codec| parse_codec(codec)).collect());
    }
    def
}

fn parse_column_type(column_type: &str) -> ColumnType {
    let column_type = column_type.trim();
    if let Some(inner) = strip_wrapper(column_type, "Nullable") {
        return ColumnType::Nullable(Box::new(parse_column_type(inner)));
    }
    if let Some(inner) = strip_wrapper(column_type, "LowCardinality") {
        return ColumnType::LowCardinality(Box::new(parse_column_type(inner)));
    }
    if let Some(inner) = strip_wrapper(column_type, "Array") {
        return ColumnType::Array(Box::new(parse_column_type(inner)));
    }
    if column_type.starts_with("DateTime64") {
        let inner = &column_type[11..column_type.len() - 1];
        let parts: Vec<&str> = inner.splitn(2, ',').collect();
        let precision: u8 = parts[0].trim().parse().unwrap_or(6);
        let timezone = parts
            .get(1)
            .map(|tz| tz.trim().trim_matches('\'').to_string());
        return ColumnType::Timestamp {
            precision,
            timezone,
        };
    }
    match column_type {
        "Int64" => ColumnType::Int64,
        "UInt64" => ColumnType::UInt64,
        "Bool" => ColumnType::Bool,
        "String" => ColumnType::String,
        "Date32" => ColumnType::Date32,
        _ => ColumnType::String,
    }
}

fn parse_codec(codec: &str) -> Codec {
    let lower = codec.to_lowercase();
    match lower.as_str() {
        "lz4" => Codec::LZ4,
        "doubledelta" => Codec::DoubleDelta,
        "t64" => Codec::T64,
        _ if lower.starts_with("zstd(") => {
            Codec::ZSTD(lower[5..lower.len() - 1].parse().unwrap_or(1))
        }
        _ if lower.starts_with("delta(") => {
            Codec::Delta(lower[6..lower.len() - 1].parse().unwrap_or(8))
        }
        _ => Codec::ZSTD(1),
    }
}

fn strip_wrapper<'input>(input: &'input str, prefix: &str) -> Option<&'input str> {
    if input.starts_with(prefix) && input.ends_with(')') {
        Some(&input[prefix.len() + 1..input.len() - 1])
    } else {
        None
    }
}

fn build_local_node_table(ontology: &Ontology, entity_name: &str) -> Option<CreateTable> {
    let exclude = ontology.local_entity_excludes(entity_name)?;
    let node = ontology.get_node(entity_name)?;

    let columns: Vec<ColumnDef> = node
        .storage
        .columns
        .iter()
        .filter(|column| !exclude.iter().any(|excluded| excluded == &column.name))
        .map(storage_col_to_def)
        .collect();

    Some(CreateTable {
        name: node.destination_table.clone(),
        columns,
        indexes: vec![],
        projections: vec![],
        engine: Engine {
            name: String::new(),
            args: vec![],
        },
        partition_by: vec![],
        order_by: node
            .sort_key
            .iter()
            .filter(|key| !exclude.iter().any(|excluded| excluded == *key))
            .cloned()
            .collect(),
        primary_key: None,
        settings: vec![],
        ttl: None,
    })
}

fn build_local_edge_table(ontology: &Ontology) -> Option<CreateTable> {
    let table_name = ontology.local_edge_table_name()?;
    let columns: Vec<ColumnDef> = ontology
        .local_edge_columns()
        .iter()
        .map(|column| {
            let column_type = local_data_type_to_column_type(&column.data_type);
            ColumnDef::new(&column.name, column_type)
        })
        .collect();

    Some(CreateTable {
        name: table_name.to_string(),
        columns,
        indexes: vec![],
        projections: vec![],
        engine: Engine {
            name: String::new(),
            args: vec![],
        },
        partition_by: vec![],
        order_by: ontology
            .local_edge_columns()
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        primary_key: None,
        settings: vec![],
        ttl: None,
    })
}

fn local_data_type_to_column_type(data_type: &ontology::DataType) -> ColumnType {
    match data_type {
        ontology::DataType::String | ontology::DataType::Uuid => ColumnType::String,
        ontology::DataType::Int => ColumnType::Int64,
        ontology::DataType::Bool => ColumnType::Bool,
        ontology::DataType::DateTime => ColumnType::Timestamp {
            precision: 6,
            timezone: None,
        },
        ontology::DataType::Date => ColumnType::Date32,
        _ => ColumnType::String,
    }
}
