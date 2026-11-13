use serde::Deserialize;

use super::DuckDb;
use crate::DataType;
use crate::catalog::{Capabilities, Diagnostics, PolicyKind, StorageBackend, StoreInput};
use crate::implementations::relational::{
    Column, RelationalOptions, RelationalStore, Table, entity_layout,
};
use crate::spec::parse_word;

#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckDbSource {
    #[serde(default)]
    pub sort_key: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DuckDbColumn {
    #[serde(rename = "type")]
    pub storage_type: String,
}

impl StorageBackend for DuckDb {
    const KIND: &'static str = "duckdb";
    const CAPABILITIES: Capabilities = Capabilities {
        joins: true,
        writer: true,
        search: false,
    };
    const POLICIES: &'static [PolicyKind] =
        &[PolicyKind::ScopeKey, PolicyKind::Owner, PolicyKind::None];
    type Options = RelationalOptions;
    type SourceOptions = DuckDbSource;
    type ColumnOptions = Option<DuckDbColumn>;
    type Store = RelationalStore;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> RelationalStore {
        let options = input.options;
        let edge_tables: Vec<(&str, bool)> = options
            .edge_tables
            .iter()
            .map(|(name, table)| (name.as_str(), table.shared))
            .collect();
        let mut relational = RelationalStore::resolve(
            &input,
            options.version_column.as_ref(),
            options.deleted_column.as_ref(),
            &edge_tables,
            diagnostics,
        );
        relational.tables = entity_tables(&input, diagnostics);
        relational
            .tables
            .extend(edge_table_specs(&input, diagnostics));
        relational.check_edge_columns(&input, diagnostics);
        relational
    }
}

fn entity_tables(input: &StoreInput<'_, DuckDb>, diagnostics: &mut Diagnostics) -> Vec<Table> {
    let (graph, store) = (input.graph, input.store);
    input
        .entities
        .iter()
        .filter_map(|entity| {
            let (source, table, sort_key) =
                entity_layout(input, entity, &entity.options.sort_key, diagnostics)?;
            let columns = entity
                .columns
                .iter()
                .filter_map(|column| {
                    let property = graph.property(column.property);
                    let spec =
                        diagnostics.require(column.options.as_ref(), entity.package, || {
                            format!(
                                "store {} column {}.{} declares no storage type",
                                store.name,
                                graph.entity(entity.entity).name,
                                property.name
                            )
                        })?;
                    Some(Column {
                        property: Some(column.property),
                        storage_type: spec.storage_type.clone(),
                        nullable: property.nullable,
                        default: property.default.as_ref().map(|value| value.literal()),
                        ..Column::new(column.name, Some(property.data_type))
                    })
                })
                .collect();
            Some(Table::of_source(source, table, sort_key, columns))
        })
        .collect()
}

fn edge_table_specs(input: &StoreInput<'_, DuckDb>, diagnostics: &mut Diagnostics) -> Vec<Table> {
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
