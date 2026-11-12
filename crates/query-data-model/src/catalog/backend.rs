use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use super::access::EdgeScope;
use super::guards::Guard;
use super::guards::PolicyKind;
use super::store::Facts;
use super::{Access, Diagnostics, Model, Source, Sources, Store};
use crate::implementations::{
    clickhouse::ClickHouse, duckdb::DuckDb, live::Live, orbit_code::OrbitCode,
};
use crate::plan::tracking::{Language, Plan};
use crate::spec::{Block, Blocks, StorageFragment, Typed};
use crate::{DataModelError, EntityId, GraphCatalog, Packages, PropertyId, RelationshipVariantId};

/// A storage backend: the blocks its stores' fragments write, and the facts it resolves them
/// into. A block the YAML omits reads as its type's default.
pub trait StorageBackend: Sized + 'static {
    const KIND: &'static str;
    /// The access policies this backend renders into its plans.
    const POLICIES: &'static [PolicyKind];
    type Options: Block + DeserializeOwned + Default;
    type SourceOptions: Block + DeserializeOwned + Default;
    type ColumnOptions: Block + DeserializeOwned + Default;
    type Store: Debug + Send + Sync + std::panic::RefUnwindSafe + 'static;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> Self::Store;
}

impl<B: StorageBackend> Blocks for B {
    type Engine = B::Options;
    type Entity = B::SourceOptions;
    type Column = B::ColumnOptions;
}

impl<B: StorageBackend> Typed for B {}

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

/// One store's backend blocks, as its fragments wrote them.
pub(crate) struct StoreBlocks<'a> {
    pub options: &'a dyn Block,
    pub entities: Vec<EntityBlocks<'a>>,
}

pub(crate) struct EntityBlocks<'a> {
    pub entity: EntityId,
    pub package: &'a str,
    pub options: &'a dyn Block,
    pub columns: Vec<(PropertyId, &'a str, &'a dyn Block)>,
}

/// One entity a store holds, with its backend blocks.
pub struct SourceInput<'a, B: StorageBackend> {
    pub entity: EntityId,
    pub package: &'a str,
    pub options: &'a B::SourceOptions,
    pub columns: Vec<ColumnInput<'a, B>>,
}

/// One stored property, with its column name and backend block.
pub struct ColumnInput<'a, B: StorageBackend> {
    pub property: PropertyId,
    pub name: &'a str,
    pub options: &'a B::ColumnOptions,
}

pub struct StoreInput<'a, B: StorageBackend> {
    pub graph: &'a GraphCatalog,
    pub access: &'a Access,
    pub sources: &'a Sources,
    pub store: &'a Store,
    pub connection: Vec<&'a Store>,
    pub options: &'a B::Options,
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
    /// The catalog of the one connection that holds the model's stores of this backend.
    pub fn from_model(model: &Arc<Model>) -> Result<Self, DataModelError> {
        let mut connections = model
            .stores
            .iter()
            .filter(|store| store.kind == B::KIND)
            .map(|store| store.connection.as_str());
        let connection = connections
            .next()
            .ok_or_else(|| DataModelError::UnknownReference {
                kind: "backend",
                name: B::KIND.to_string(),
            })?;
        if let Some(other) = connections.find(|other| *other != connection) {
            return Err(DataModelError::Invalid(format!(
                "{} stores sit on connections {connection} and {other}",
                B::KIND
            )));
        }
        model.physical(connection)
    }

    pub fn embedded() -> Result<Self, DataModelError> {
        Self::from_model(&Packages::embedded().load()?)
    }

    pub fn model(&self) -> &Model {
        &self.model
    }

    /// The model, for callers that keep it past this catalog.
    pub fn shared_model(&self) -> &Arc<Model> {
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

    /// The store on this connection that plans read the entity from: its complete store when the
    /// connection holds it, else the one partial store here.
    pub fn home(&self, entity: EntityId) -> Option<(&Store, &B::Store)> {
        let mut holders = self
            .stores()
            .filter(|(store, _)| store.source(entity).is_some());
        let first = holders.next()?;
        Some(
            std::iter::once(first)
                .chain(holders)
                .find(|(store, _)| store.complete(entity))
                .unwrap_or(first),
        )
    }

    /// The selectivity hint the entity's home store on this connection declares for the property.
    pub fn selectivity(&self, property: PropertyId) -> Option<crate::FieldSelectivity> {
        let entity = self.graph().property(property).entity;
        self.home(entity)?.0.selectivity(property)
    }

    /// How the entity's home store on this connection keeps an enum property.
    pub fn enum_type(&self, property: PropertyId) -> crate::EnumType {
        let entity = self.graph().property(property).entity;
        match self
            .home(entity)
            .is_some_and(|(store, _)| store.stored_as_text(property))
        {
            true => crate::EnumType::String,
            false => crate::EnumType::Int,
        }
    }

    pub fn source(&self, entity: EntityId) -> Option<(&Store, &Source)> {
        let (store, _) = self.home(entity)?;
        Some((store, store.source(entity)?))
    }

    /// How the entity's home store on this connection restricts its reads.
    pub fn guard(&self, entity: EntityId) -> Option<&Guard> {
        self.home(entity)?.0.guard(entity)
    }

    /// Which endpoints a variant's edge rows share a scope key with; none on a trusted
    /// connection, where nothing is scoped.
    pub fn edge_scope(&self, variant: RelationshipVariantId) -> Option<EdgeScope> {
        match self.stores().all(|(store, _)| store.trusted) {
            true => None,
            false => self.model.access().edge_scope(variant, B::KIND),
        }
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

    /// Resolves every store's backend facts from its blocks, once per model.
    pub(super) fn resolve_backends(
        &mut self,
        blocks: &[StoreBlocks<'_>],
        diagnostics: &mut Diagnostics,
    ) {
        let facts: Vec<_> = blocks
            .iter()
            .enumerate()
            .map(|(index, blocks)| {
                let store = &self.stores[index];
                let backend =
                    diagnostics.require(registered(&store.kind), &store.package, || {
                        format!(
                            "store {} has unknown engine kind {}",
                            store.name, store.kind
                        )
                    })?;
                backend.resolve(self, index, blocks, diagnostics)
            })
            .collect();
        for (store, facts) in self.stores.iter_mut().zip(facts) {
            store.facts = facts;
        }
    }
}

/// A backend of the registry, which parses its stores' fragments and resolves their facts.
pub(crate) trait Registered {
    fn kind(&self) -> &'static str;

    fn policies(&self) -> &'static [PolicyKind];

    fn parse(&self, yaml: &str) -> Result<StorageFragment, orbit_utils::yaml::Error>;

    fn resolve(
        &self,
        model: &Model,
        store: usize,
        blocks: &StoreBlocks<'_>,
        diagnostics: &mut Diagnostics,
    ) -> Option<Facts>;
}

struct Entry<B>(PhantomData<fn() -> B>);

const REGISTRY: [&dyn Registered; 4] = [
    &Entry::<ClickHouse>(PhantomData),
    &Entry::<DuckDb>(PhantomData),
    &Entry::<OrbitCode>(PhantomData),
    &Entry::<Live>(PhantomData),
];

pub(crate) fn registered(kind: &str) -> Option<&'static dyn Registered> {
    REGISTRY.into_iter().find(|backend| backend.kind() == kind)
}

impl<B: StorageBackend> Registered for Entry<B> {
    fn kind(&self) -> &'static str {
        B::KIND
    }

    fn policies(&self) -> &'static [PolicyKind] {
        B::POLICIES
    }

    fn parse(&self, yaml: &str) -> Result<StorageFragment, orbit_utils::yaml::Error> {
        StorageFragment::<B>::parse(yaml)
    }

    fn resolve(
        &self,
        model: &Model,
        index: usize,
        blocks: &StoreBlocks<'_>,
        diagnostics: &mut Diagnostics,
    ) -> Option<Facts> {
        let store = &model.stores[index];
        let entities = blocks
            .entities
            .iter()
            .map(|entity| {
                let columns = entity.columns.iter().map(|(property, name, options)| {
                    Some(ColumnInput {
                        property: *property,
                        name,
                        options: options.downcast_ref()?,
                    })
                });
                Some(SourceInput {
                    entity: entity.entity,
                    package: entity.package,
                    options: entity.options.downcast_ref()?,
                    columns: columns.collect::<Option<_>>()?,
                })
            })
            .collect::<Option<_>>();
        let (Some(options), Some(entities)) = (blocks.options.downcast_ref(), entities) else {
            diagnostics.error(
                &store.package,
                format!(
                    "store {} holds blocks of a backend other than {}",
                    store.name,
                    B::KIND
                ),
            );
            return None;
        };
        let input = StoreInput::<B> {
            graph: &model.graph,
            access: &model.access,
            sources: &model.sources,
            store,
            connection: model
                .stores
                .iter()
                .filter(|other| other.connection == store.connection)
                .collect(),
            options,
            entities,
        };
        Some(Box::new(B::resolve(input, diagnostics)))
    }
}
