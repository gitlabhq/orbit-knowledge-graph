use std::collections::HashMap;

use ontology::TraversalPathKind;
use serde::{Deserialize, Serialize};

use super::DuckDb;
use crate::catalog::{Diagnostics, StorageBackend, StoreInput};
use crate::implementations::relational::{
    RelationalFacts, RelationalOptions, RelationalRoute, RelationalStore, TableFacts, TableSchema,
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
            tables.insert(
                source.table.clone(),
                TableFacts {
                    entity: Some(source.entity),
                    sort_key: relational
                        .sort_key(source.entity)
                        .unwrap_or_default()
                        .to_vec(),
                    ..TableFacts::default()
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
                    columns: Some(column_types.keys().cloned().collect()),
                    column_types,
                    sort_key: spec.sort_key.clone(),
                    path_columns: None,
                },
            );
        }
        let store = DuckDbStore { relational, tables };
        store.check_attribute_columns(input.store, input.graph, diagnostics);
        store
    }
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
    const SELECTIVE_LATEST_ROWS: bool = false;
    const DEFERRED_NODE_LOADING: bool = false;
    const ALL_PROPERTIES_BY_DEFAULT: bool = true;

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
