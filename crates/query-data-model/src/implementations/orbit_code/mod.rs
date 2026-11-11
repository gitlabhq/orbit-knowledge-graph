mod render;

pub use render::{RequestField, StructuralSearchRequest};

use indexmap::IndexMap;
use serde::Deserialize;

use crate::catalog::{
    Diagnostics, ExecuteError, NoOptions, Row, StorageBackend, StoreInput, check_unrelational,
};
use crate::{EntityId, PropertyId};

#[derive(Debug, Clone)]
pub struct OrbitCode;

/// The code indexing web servers, which run a structural search request.
pub trait CodeSearch {
    fn search(&self, request: &StructuralSearchRequest) -> Result<Vec<Row>, ExecuteError>;
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeOptions {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    Trigram,
    Structural,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeSource {
    #[serde(default)]
    pub search: IndexMap<String, Vec<SearchMode>>,
}

/// The search modes of each property of one entity.
type Searched = Vec<(PropertyId, Vec<SearchMode>)>;

#[derive(Debug, Default)]
pub struct CodeStore {
    indexes: Vec<(EntityId, Searched)>,
}

impl CodeStore {
    pub fn supports(&self, property: PropertyId, mode: SearchMode) -> bool {
        self.indexes
            .iter()
            .flat_map(|(_, properties)| properties)
            .any(|(id, modes)| *id == property && modes.contains(&mode))
    }

    /// The property a structural pattern runs over.
    pub fn structural(&self, entity: EntityId) -> Option<PropertyId> {
        self.indexes
            .iter()
            .filter(|(indexed, _)| *indexed == entity)
            .flat_map(|(_, properties)| properties)
            .find(|(_, modes)| modes.contains(&SearchMode::Structural))
            .map(|(property, _)| *property)
    }
}

impl StorageBackend for OrbitCode {
    const KIND: &'static str = "orbit_code";
    type Options = CodeOptions;
    type SourceOptions = CodeSource;
    type ColumnOptions = NoOptions;
    type Store = CodeStore;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> CodeStore {
        let graph = input.graph;
        let store_name = &input.store.name;
        let mut store = CodeStore::default();
        check_unrelational(&input, diagnostics);
        for source in input.entities {
            let name = &graph.entity(source.entity).name;
            let mut properties = Vec::new();
            for (property, modes) in source.options.search {
                let id = graph.property_id(source.entity, &property);
                if let Some(id) = diagnostics.require(id, source.package, || {
                    format!("store {store_name} searches unknown property {name}.{property}")
                }) {
                    properties.push((id, modes));
                }
            }
            let structural = properties
                .iter()
                .filter(|(_, modes)| modes.contains(&SearchMode::Structural))
                .count();
            if structural > 1 {
                diagnostics.error(
                    source.package,
                    format!(
                        "store {store_name} searches {name} structurally on several properties"
                    ),
                );
            }
            store.indexes.push((source.entity, properties));
        }
        store
    }
}
