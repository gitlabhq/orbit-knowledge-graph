use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use super::store::Facts;
use super::{Access, Diagnostics, Model, Source, Sources, Store};
use crate::implementations::{
    clickhouse::ClickHouse, duckdb::DuckDb, live::Live, orbit_code::OrbitCode,
};
use crate::plan::tracking::{Language, Plan};
use crate::spec::{BackendOptions, decode_options};
use crate::{DataModelError, EntityId, GraphCatalog, PropertyId};

pub trait StorageBackend: Sized + 'static {
    const KIND: &'static str;
    type Options: DeserializeOwned + Default;
    type SourceOptions: DeserializeOwned + Default;
    type ColumnOptions: DeserializeOwned + Default;
    type Store: Debug + Send + Sync + std::panic::RefUnwindSafe + 'static;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> Self::Store;
}

/// The options of a backend block that takes none.
#[derive(Debug, Clone, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NoOptions {}

/// A store that holds no rows names no tables.
pub fn check_unrelational<B: StorageBackend>(
    input: &StoreInput<'_, B>,
    diagnostics: &mut Diagnostics,
) {
    let store = input.store;
    for source in store.sources() {
        if let Some(table) = &source.table {
            diagnostics.error(
                &source.package,
                format!(
                    "store {} is {} and holds no table, but binds {} to {table}",
                    store.name,
                    store.kind,
                    input.graph.entity(source.entity).name
                ),
            );
        }
    }
}

/// One result row of a backend service, keyed by the request's field names.
pub type Row = serde_json::Map<String, serde_json::Value>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{0}")]
pub struct ExecuteError(pub String);

/// A catalog renders the plans it builds into its backend's native request.
pub trait Render {
    type Language: Language;
    type Request;
    type Error;

    fn render(&self, plan: &Plan<Self::Language>) -> Result<Self::Request, Self::Error>;
}

/// One entity a store holds, with its backend blocks parsed.
pub struct SourceInput<'a, B: StorageBackend> {
    pub entity: EntityId,
    pub package: &'a str,
    pub options: B::SourceOptions,
    pub columns: Vec<(PropertyId, B::ColumnOptions)>,
}

pub struct StoreInput<'a, B: StorageBackend> {
    pub graph: &'a GraphCatalog,
    pub access: &'a Access,
    pub sources: &'a Sources,
    pub store: &'a Store,
    pub connection: Vec<&'a Store>,
    pub options: B::Options,
    pub entities: Vec<SourceInput<'a, B>>,
}

#[derive(Debug)]
pub struct Catalog<B: StorageBackend> {
    model: Arc<Model>,
    connection: String,
    stores: Vec<usize>,
    backend: PhantomData<fn() -> B>,
}

impl<B: StorageBackend> Catalog<B> {
    pub fn model(&self) -> &Model {
        &self.model
    }

    pub fn graph(&self) -> &GraphCatalog {
        self.model.graph()
    }

    pub fn connection(&self) -> &str {
        &self.connection
    }

    pub fn stores(&self) -> impl Iterator<Item = (&Store, &B::Store)> {
        self.stores.iter().filter_map(|index| {
            let store = &self.model.stores[*index];
            Some((store, store.facts::<B::Store>()?))
        })
    }

    pub fn store(&self, name: &str) -> Option<&Store> {
        self.stores()
            .find(|(store, _)| store.name == name)
            .map(|(store, _)| store)
    }

    pub fn backend(&self, name: &str) -> Option<&B::Store> {
        self.stores()
            .find(|(store, _)| store.name == name)
            .map(|(_, facts)| facts)
    }

    pub fn sources(&self, entity: EntityId) -> impl Iterator<Item = (&Store, &Source)> {
        self.stores()
            .filter_map(move |(store, _)| Some((store, store.source(entity)?)))
    }

    pub fn home(&self, entity: EntityId) -> Option<(&Store, &B::Store)> {
        self.stores()
            .find(|(store, _)| store.source(entity).is_some())
    }

    pub fn source(&self, entity: EntityId) -> Option<(&Store, &Source)> {
        self.sources(entity).next()
    }

    /// The access level the entity's home store requires of a caller's path.
    pub fn min_role(&self, entity: EntityId) -> Option<u32> {
        self.home(entity)?.0.guard(entity)?.min_role
    }
}

impl Model {
    pub fn physical<B: StorageBackend>(
        self: &Arc<Self>,
        connection: &str,
    ) -> Result<Catalog<B>, DataModelError> {
        let stores: Vec<usize> = (0..self.stores.len())
            .filter(|index| self.stores[*index].connection == connection)
            .collect();
        let Some(first) = stores.first().map(|index| &self.stores[*index]) else {
            return Err(DataModelError::UnknownReference {
                kind: "connection",
                name: connection.to_string(),
            });
        };
        if first.kind != B::KIND {
            let mut diagnostics = Diagnostics::default();
            diagnostics.error(
                &first.package,
                format!("connection {connection} is {}, not {}", first.kind, B::KIND),
            );
            diagnostics.finish()?;
        }
        Ok(Catalog {
            model: Arc::clone(self),
            connection: connection.to_string(),
            stores,
            backend: PhantomData,
        })
    }

    /// Parses every store's backend blocks and resolves its backend facts, once per model.
    pub(super) fn resolve_backends(&mut self, diagnostics: &mut Diagnostics) {
        let backends: [&dyn Resolve; 4] = [
            &Backend::<ClickHouse>(PhantomData),
            &Backend::<DuckDb>(PhantomData),
            &Backend::<OrbitCode>(PhantomData),
            &Backend::<Live>(PhantomData),
        ];
        let facts: Vec<_> = (0..self.stores.len())
            .map(|index| {
                let store = &self.stores[index];
                let backend = backends.iter().find(|backend| backend.kind() == store.kind);
                let backend = diagnostics.require(backend, &store.package, || {
                    format!(
                        "store {} has unknown engine kind {}",
                        store.name, store.kind
                    )
                })?;
                backend.resolve(self, index, diagnostics)
            })
            .collect();
        for (store, facts) in self.stores.iter_mut().zip(facts) {
            store.facts = facts;
        }
    }
}

trait Resolve {
    fn kind(&self) -> &'static str;
    fn resolve(&self, model: &Model, store: usize, diagnostics: &mut Diagnostics) -> Option<Facts>;
}

struct Backend<B>(PhantomData<fn() -> B>);

impl<B: StorageBackend> Resolve for Backend<B> {
    fn kind(&self) -> &'static str {
        B::KIND
    }

    fn resolve(&self, model: &Model, index: usize, diagnostics: &mut Diagnostics) -> Option<Facts> {
        let store = &model.stores[index];
        let errors = diagnostics.len();
        let graph = &model.graph;
        let context = format!("store {}", store.name);
        let entities = store
            .entity_options
            .iter()
            .map(|entity| {
                let name = &graph.entity(entity.entity).name;
                let package = entity.package.as_str();
                let columns = entity
                    .columns
                    .iter()
                    .map(|(property, options)| {
                        let label =
                            format!("{context} column {name}.{}", graph.property(*property).name);
                        (*property, parse(package, &label, options, diagnostics))
                    })
                    .collect();
                SourceInput {
                    entity: entity.entity,
                    package,
                    options: parse(
                        package,
                        &format!("{context} {name}"),
                        &entity.options,
                        diagnostics,
                    ),
                    columns,
                }
            })
            .collect();
        let input = StoreInput::<B> {
            graph,
            access: &model.access,
            sources: &model.sources,
            store,
            connection: model
                .stores
                .iter()
                .filter(|other| other.connection == store.connection)
                .collect(),
            options: parse(&store.package, &context, &store.options, diagnostics),
            entities,
        };
        if diagnostics.len() > errors {
            return None;
        }
        Some(Box::new(B::resolve(input, diagnostics)))
    }
}

fn parse<T: DeserializeOwned + Default>(
    package: &str,
    context: &str,
    options: &BackendOptions,
    diagnostics: &mut Diagnostics,
) -> T {
    decode_options(options).unwrap_or_else(|error| {
        diagnostics.error(package, format!("{context}: {error}"));
        T::default()
    })
}
