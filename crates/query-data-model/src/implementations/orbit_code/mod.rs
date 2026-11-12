mod render;

pub use render::{RequestField, StructuralSearchRequest};

use indexmap::IndexMap;
use serde::Deserialize;

use crate::catalog::{
    Diagnostics, ExecuteError, NoOptions, PolicyKind, Row, StorageBackend, StoreInput,
    check_unrelational,
};
use crate::{EntityId, PropertyId};

#[derive(Debug, Clone)]
pub struct OrbitCode;

/// The code indexing web servers, which run a structural search request.
pub trait CodeSearch {
    fn search(&self, request: &StructuralSearchRequest) -> Result<Vec<Row>, ExecuteError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    Trigram,
    Structural,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodeSource {
    /// The property that names the repository each row is in, for an entity that reaches no
    /// scope through a `via` chain, whose first step names the repository otherwise.
    pub repository: Option<String>,
    #[serde(default)]
    pub search: IndexMap<String, Vec<SearchMode>>,
}

/// One entity a code store indexes: the property its store declares names its repository, and
/// the search modes of each searched property.
#[derive(Debug)]
struct Indexed {
    entity: EntityId,
    repository: Option<PropertyId>,
    searched: Vec<(PropertyId, Vec<SearchMode>)>,
}

#[derive(Debug, Default)]
pub struct CodeStore {
    indexes: Vec<Indexed>,
}

impl CodeStore {
    pub fn supports(&self, property: PropertyId, mode: SearchMode) -> bool {
        self.indexes
            .iter()
            .flat_map(|indexed| &indexed.searched)
            .any(|(id, modes)| *id == property && modes.contains(&mode))
    }

    /// The property a structural pattern runs over.
    pub fn structural(&self, entity: EntityId) -> Option<PropertyId> {
        self.indexes
            .iter()
            .filter(|indexed| indexed.entity == entity)
            .flat_map(|indexed| &indexed.searched)
            .find(|(_, modes)| modes.contains(&SearchMode::Structural))
            .map(|(property, _)| *property)
    }

    /// The property the store declares names the repository of each row of `entity`.
    pub fn repository(&self, entity: EntityId) -> Option<PropertyId> {
        self.indexes
            .iter()
            .find(|indexed| indexed.entity == entity)
            .and_then(|indexed| indexed.repository)
    }
}

impl StorageBackend for OrbitCode {
    const KIND: &'static str = "orbit_code";
    const POLICIES: &'static [PolicyKind] = &[PolicyKind::Via, PolicyKind::None];
    type Options = NoOptions;
    type SourceOptions = CodeSource;
    type ColumnOptions = NoOptions;
    type Store = CodeStore;

    fn searched(store: &CodeStore) -> Vec<PropertyId> {
        store
            .indexes
            .iter()
            .flat_map(|indexed| &indexed.searched)
            .map(|(property, _)| *property)
            .collect()
    }

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> CodeStore {
        let graph = input.graph;
        let store_name = &input.store.name;
        let mut store = CodeStore::default();
        check_unrelational(&input, diagnostics);
        for source in &input.entities {
            let name = &graph.entity(source.entity).name;
            let repository = source.options.repository.as_deref().and_then(|property| {
                let id = graph
                    .property_id(source.entity, property)
                    .filter(|id| input.store.column(*id).is_some());
                diagnostics.require(id, source.package, || {
                    format!("store {store_name} names unstored repository {name}.{property}")
                })
            });
            let via = matches!(
                input.store.guard(source.entity),
                Some(crate::catalog::Guard {
                    rule: crate::catalog::Rule::Via(_),
                    ..
                })
            );
            match (via, &source.options.repository) {
                (true, Some(_)) => diagnostics.error(
                    source.package,
                    format!(
                        "store {store_name} names a repository property for {name}, whose via chain names it"
                    ),
                ),
                (false, None) => diagnostics.error(
                    source.package,
                    format!("store {store_name} names no repository property for {name}"),
                ),
                _ => {}
            }
            let mut properties = Vec::new();
            for (property, modes) in &source.options.search {
                let id = graph.property_id(source.entity, property);
                if let Some(id) = diagnostics.require(id, source.package, || {
                    format!("store {store_name} searches unknown property {name}.{property}")
                }) {
                    properties.push((id, modes.clone()));
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
            store.indexes.push(Indexed {
                entity: source.entity,
                repository,
                searched: properties,
            });
        }
        store
    }
}
