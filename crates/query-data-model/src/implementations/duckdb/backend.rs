use std::collections::HashMap;

use indexmap::IndexMap;
use ontology::DataType;
use serde::{Deserialize, Serialize};

use super::DuckDb;
use crate::EntityId;
use crate::catalog::{Diagnostics, StorageBackend, StoreInput};
use crate::implementations::relational::{
    PathColumn, RelationalFacts, RelationalOptions, RelationalRoute, RelationalStore, TableFacts,
    TableSchema,
};

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

/// A table the store creates, with its columns in declaration order.
#[derive(Debug, Clone)]
pub struct DuckDbTable {
    pub name: String,
    pub entity: Option<EntityId>,
    pub columns: Vec<DuckDbColumn>,
}

#[derive(Debug, Clone)]
pub struct DuckDbColumn {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default: Option<String>,
}

#[derive(Debug)]
pub struct DuckDbStore {
    pub relational: RelationalStore,
    pub layouts: Vec<DuckDbTable>,
    tables: HashMap<String, TableFacts>,
}

impl RelationalFacts for DuckDbStore {
    fn relational(&self) -> &RelationalStore {
        &self.relational
    }

    fn table(&self, name: &str) -> Option<&TableFacts> {
        self.tables.get(name)
    }
}

impl StorageBackend for DuckDb {
    const KIND: &'static str = "duckdb";
    type Options = RelationalOptions;
    type SourceOptions = DuckDbSource;
    type RouteOptions = RelationalRoute;
    type Store = DuckDbStore;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> DuckDbStore {
        let relational = RelationalStore::resolve(
            &input,
            &input.options,
            input
                .sources
                .iter()
                .map(|source| (source.entity, source.options.sort_key.clone())),
            diagnostics,
        );
        let mut relational = relational;
        relational.tables = entity_schemas(&input).collect();
        let mut layouts = entity_layouts(&input, diagnostics);
        let mut tables = HashMap::new();
        for source in input.store.sources() {
            let properties = input.graph.entity(source.entity).properties.iter();
            let column_types: HashMap<String, DataType> = properties
                .filter_map(|property| {
                    let column = input.store.column(*property)?;
                    Some((
                        column.to_string(),
                        input.graph.property(*property).data_type,
                    ))
                })
                .collect();
            tables.insert(
                source.table.clone(),
                TableFacts {
                    entity: Some(source.entity),
                    columns: column_types.keys().cloned().collect(),
                    column_types,
                    sort_key: relational
                        .sort_key(source.entity)
                        .unwrap_or_default()
                        .to_vec(),
                    path_columns: path_columns(&input, Some(source.entity)),
                },
            );
        }
        for (name, spec) in &input.options.edge_tables {
            let column_types = spec.column_types(&input.store.package, name, diagnostics);
            layouts.push(DuckDbTable {
                name: name.clone(),
                entity: None,
                columns: spec
                    .columns
                    .keys()
                    .filter_map(|column| {
                        Some(DuckDbColumn {
                            name: column.clone(),
                            data_type: *column_types.get(column)?,
                            nullable: false,
                            default: None,
                        })
                    })
                    .collect(),
            });
            relational.tables.push(TableSchema::new(
                name,
                spec.columns
                    .keys()
                    .map(|column| (column.clone(), column_types.get(column).copied())),
            ));
            tables.insert(
                name.clone(),
                TableFacts {
                    entity: None,
                    columns: column_types.keys().cloned().collect(),
                    column_types,
                    sort_key: spec.sort_key.clone(),
                    path_columns: path_columns(&input, None),
                },
            );
        }
        let store = DuckDbStore {
            relational,
            layouts,
            tables,
        };
        store.check_attribute_columns(input.store, input.graph, diagnostics);
        store
    }
}

fn path_columns(input: &StoreInput<'_, DuckDb>, entity: Option<EntityId>) -> Vec<PathColumn> {
    let column = match entity {
        Some(entity) => input
            .store
            .source(entity)
            .and_then(|source| source.path_column.as_ref()),
        None => input.store.path_column.as_ref(),
    };
    column
        .map(|name| PathColumn {
            name: name.clone(),
            entity,
        })
        .into_iter()
        .collect()
}

fn entity_layouts(
    input: &StoreInput<'_, DuckDb>,
    diagnostics: &mut Diagnostics,
) -> Vec<DuckDbTable> {
    let (graph, store) = (input.graph, input.store);
    input
        .sources
        .iter()
        .filter_map(|source| {
            let table = &store.source(source.entity)?.table;
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
                    Some(DuckDbColumn {
                        name: store.column(*id)?.to_string(),
                        data_type: property.data_type,
                        nullable: property.nullable,
                        default: declared
                            .get(&property.name)
                            .and_then(|column| column.default.clone()),
                    })
                })
                .collect();
            Some(DuckDbTable {
                name: table.clone(),
                entity: Some(source.entity),
                columns,
            })
        })
        .collect()
}

fn entity_schemas<'a>(input: &'a StoreInput<'_, DuckDb>) -> impl Iterator<Item = TableSchema> + 'a {
    let graph = input.graph;
    input.store.sources().map(move |source| {
        let columns = graph
            .entity(source.entity)
            .properties
            .iter()
            .filter_map(|property| {
                let column = input.store.column(*property)?;
                Some((
                    column.to_string(),
                    Some(graph.property(*property).data_type),
                ))
            });
        TableSchema::new(&source.table, columns)
    })
}
