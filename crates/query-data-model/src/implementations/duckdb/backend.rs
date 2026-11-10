use indexmap::IndexMap;
use serde::{Deserialize, Serialize};

use super::DuckDb;
use crate::DataType;
use crate::catalog::{Diagnostics, StorageBackend, StoreInput};
use crate::implementations::relational::{
    Column, RelationalOptions, RelationalRoute, RelationalStore, Table,
};
use crate::spec::parse_word;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckDbSource {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sort_key: Vec<String>,
    #[serde(default, skip_serializing_if = "IndexMap::is_empty")]
    pub columns: IndexMap<String, DuckDbColumnSpec>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckDbColumnSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
}

impl StorageBackend for DuckDb {
    const KIND: &'static str = "duckdb";
    type Options = RelationalOptions;
    type SourceOptions = DuckDbSource;
    type RouteOptions = RelationalRoute;
    type Store = RelationalStore;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> RelationalStore {
        let mut relational = RelationalStore::resolve(&input, &input.options, diagnostics);
        relational.tables = entity_tables(&input, diagnostics);
        relational.tables.extend(edge_tables(&input, diagnostics));
        relational.check_attribute_columns(input.store, input.graph, diagnostics);
        relational
    }
}

fn entity_tables(input: &StoreInput<'_, DuckDb>, diagnostics: &mut Diagnostics) -> Vec<Table> {
    let (graph, store) = (input.graph, input.store);
    input
        .sources
        .iter()
        .filter_map(|source| {
            let bound = store.source(source.entity)?;
            let entity = graph.entity(source.entity);
            let declared = &source.options.columns;
            for property in declared.keys() {
                let stored = graph
                    .property_id(source.entity, property)
                    .filter(|id| store.column(*id).is_some());
                diagnostics.require(stored, source.package, || {
                    format!(
                        "store {} lays out {}.{property}, which it does not store",
                        store.name, entity.name
                    )
                });
            }
            let columns = entity
                .properties
                .iter()
                .filter_map(|id| {
                    let property = graph.property(*id);
                    Some(Column {
                        property: Some(*id),
                        storage_type: sql_type(property.data_type).to_string(),
                        nullable: property.nullable,
                        default: declared
                            .get(&property.name)
                            .and_then(|column| column.default.clone()),
                        ..Column::new(store.column(*id)?, Some(property.data_type))
                    })
                })
                .collect();
            let sort_key = input.options.sort_key_of(&source.options.sort_key);
            Some(Table::of_source(bound, sort_key, columns))
        })
        .collect()
}

fn edge_tables(input: &StoreInput<'_, DuckDb>, diagnostics: &mut Diagnostics) -> Vec<Table> {
    let package = &input.store.package;
    input
        .options
        .edge_tables
        .iter()
        .map(|(name, spec)| {
            let columns = spec
                .columns
                .iter()
                .filter_map(|(column, word)| {
                    let data_type =
                        diagnostics.require(parse_word::<DataType>(word), package, || {
                            format!("edge table {name} column {column} has unknown type `{word}`")
                        })?;
                    Some(Column {
                        storage_type: sql_type(data_type).to_string(),
                        ..Column::new(column, Some(data_type))
                    })
                })
                .collect();
            Table::edge(name, input.store, spec.sort_key.clone(), columns)
        })
        .collect()
}

fn sql_type(data_type: DataType) -> &'static str {
    match data_type {
        DataType::String | DataType::Enum | DataType::Uuid => "VARCHAR",
        DataType::Int => "BIGINT",
        DataType::Float => "DOUBLE",
        DataType::Bool => "BOOLEAN",
        DataType::Date => "DATE",
        DataType::DateTime => "TIMESTAMP",
    }
}
