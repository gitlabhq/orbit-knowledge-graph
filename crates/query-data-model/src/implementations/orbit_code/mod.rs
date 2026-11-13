mod render;

pub use render::{RequestField, StructuralSearchRequest};

use indexmap::IndexMap;
use schemars::JsonSchema;
use serde::Deserialize;

use crate::catalog::{
    Capabilities, Diagnostics, ExecuteError, NoOptions, PolicyKind, Row, StorageBackend,
    StoreInput, check_unrelational,
};
use crate::{DataType, EntityId, PropertyId};

#[derive(Debug, Clone)]
pub struct OrbitCode;

/// The code indexing web servers, which run a structural search request.
pub trait CodeSearch {
    fn search(&self, request: &StructuralSearchRequest) -> Result<Vec<Row>, ExecuteError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SearchMode {
    Trigram,
    Structural,
}

#[derive(Debug, Clone, Default, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CodeSource {
    #[schemars(required)]
    pub repository: Option<RepositorySpec>,
    #[serde(default)]
    pub search: IndexMap<String, Vec<SearchMode>>,
}

impl crate::spec::WrittenBlock for CodeSource {}

/// The columns that name the repository and the branch each row is in.
#[derive(Debug, Clone, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RepositorySpec {
    pub project: String,
    pub branch: String,
}

/// The properties a code store names each row's repository and branch by: Orbit Next names a
/// repository by its integer project and a head by its branch name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Repository {
    pub project: PropertyId,
    pub branch: PropertyId,
}

/// One entity a code store indexes: its repository columns, and the search modes of each
/// searched property.
#[derive(Debug)]
struct Indexed {
    entity: EntityId,
    repository: Repository,
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

    pub fn repository(&self, entity: EntityId) -> Option<Repository> {
        self.indexes
            .iter()
            .find(|indexed| indexed.entity == entity)
            .map(|indexed| indexed.repository)
    }
}

impl StorageBackend for OrbitCode {
    const KIND: &'static str = "orbit_code";
    const CAPABILITIES: Capabilities = Capabilities {
        joins: false,
        writer: false,
        search: true,
    };
    const POLICIES: &'static [PolicyKind] = &[PolicyKind::Via, PolicyKind::None];
    type Options = NoOptions;
    type SourceOptions = CodeSource;
    type ColumnOptions = NoOptions;
    type Store = CodeStore;

    fn structural(store: &CodeStore, entity: EntityId) -> Option<PropertyId> {
        store.structural(entity)
    }

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> CodeStore {
        let graph = input.graph;
        let store_name = &input.store.name;
        let mut store = CodeStore::default();
        check_unrelational(&input, diagnostics);
        for source in &input.entities {
            let name = &graph.entity(source.entity).name;
            let mut column = |written: &str, data_type: DataType| {
                let id = graph
                    .property_id(source.entity, written)
                    .filter(|id| input.store.column(*id).is_some())
                    .filter(|id| graph.property(*id).data_type == data_type);
                diagnostics.require(id, source.package, || {
                    format!(
                        "store {store_name} names {name}.{written} as a repository column, which is no stored {data_type:?} property"
                    )
                })
            };
            let repository = source.options.repository.as_ref().map(|written| {
                (
                    column(&written.project, DataType::Int),
                    column(&written.branch, DataType::String),
                )
            });
            let repository = match repository {
                Some((Some(project), Some(branch))) => Some(Repository { project, branch }),
                Some(_) => None,
                None => {
                    diagnostics.error(
                        source.package,
                        format!("store {store_name} names no repository columns for {name}"),
                    );
                    None
                }
            };
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
            if let Some(repository) = repository {
                store.indexes.push(Indexed {
                    entity: source.entity,
                    repository,
                    searched: properties,
                });
            }
        }
        store
    }
}
