use std::collections::HashMap;

use ontology::{DataType, TraversalPathKind};
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
}

#[derive(Debug)]
pub struct DuckDbStore {
    pub relational: RelationalStore,
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
        let store = DuckDbStore { relational, tables };
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

impl crate::query_model::QueryFacts for DuckDbStore {
    fn text_index(&self, _property: crate::PropertyId) -> bool {
        false
    }

    fn null_as_epoch(&self, _property: crate::PropertyId) -> bool {
        false
    }

    fn high_fan_out(&self, _entity: crate::EntityId) -> bool {
        false
    }

    fn denormalized(&self) -> &crate::DenormalizedCatalog {
        static EMPTY: std::sync::LazyLock<crate::DenormalizedCatalog> =
            std::sync::LazyLock::new(crate::DenormalizedCatalog::default);
        &EMPTY
    }

    fn path_lookup(
        &self,
        _entity: crate::EntityId,
        _kind: TraversalPathKind,
    ) -> Option<&crate::TraversalPathLookup> {
        None
    }

    fn copies_of<'a>(&'a self, _table: &str) -> Vec<&'a str> {
        Vec::new()
    }

    fn joins(&self) -> &[crate::MaterializedJoin] {
        &[]
    }
}
