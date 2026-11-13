use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use super::access::EdgeScope;
use super::guards::Guard;
use super::guards::PolicyKind;
use super::placement::Operation;
use super::store::Facts;
use super::{Access, Diagnostics, Model, Source, Sources, Store};
use crate::implementations::{
    clickhouse::ClickHouse, duckdb::DuckDb, live::Live, orbit_code::OrbitCode,
};
use crate::plan::tracking::{Language, Plan};
use crate::spec::{Block, Blocks, StorageFragment, Typed, WrittenBlock};
use crate::{
    ConnectionId, DataModelError, EntityId, GraphCatalog, Packages, PropertyId,
    RelationshipVariantId,
};

/// A storage backend: the blocks its stores' fragments write, and the facts it resolves them
/// into. A block the YAML omits reads as its type's default.
pub trait StorageBackend: Sized + 'static {
    const KIND: &'static str;
    const CAPABILITIES: Capabilities;
    /// The access policies this backend renders into its plans, besides `via`, which every
    /// backend without joins renders.
    const POLICIES: &'static [PolicyKind];
    /// The generic storage fragment fields this backend reads; a fragment sets no other.
    const FIELDS: &'static [Field];
    type Options: Block + DeserializeOwned + Default + WrittenBlock;
    type SourceOptions: Block + DeserializeOwned + Default + WrittenBlock;
    type ColumnOptions: Block + DeserializeOwned + Default + WrittenBlock;
    type Store: Debug + Send + Sync + std::panic::RefUnwindSafe + 'static;

    fn resolve(input: StoreInput<'_, Self>, diagnostics: &mut Diagnostics) -> Self::Store;

    /// Whether the store runs `operation` on `property` of `entity` itself, beyond what its
    /// capabilities and accepts already serve.
    fn serves(
        _store: &Self::Store,
        _entity: EntityId,
        _property: PropertyId,
        _operation: Operation,
    ) -> bool {
        false
    }
}

impl<B: StorageBackend> Blocks for B {
    type Engine = B::Options;
    type Entity = B::SourceOptions;
    type Column = B::ColumnOptions;
}

impl<B: StorageBackend> Typed for B {}

/// What a backend kind can do, which engine and loader rules read instead of its kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    /// Evaluates scope filters and relationship steps in its own query.
    pub joins: bool,
    /// Holds tables that a declared source writes; otherwise its own service writes or serves
    /// what it reads, and its stores declare no writer.
    pub writer: bool,
    /// Evaluates the search modes its stores declare, and searches by repository.
    pub search: bool,
    /// Calls an external service at query time with the caller's own sign-in, so it answers an
    /// entity's delegated check.
    pub delegates: bool,
}

/// A generic field of a storage fragment.
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "snake_case")]
pub enum Field {
    Trusted,
    PathColumn,
    Complete,
    Accepts,
    Table,
    Exclude,
    Access,
    Immutable,
    Column,
    Selectivity,
    StoredAsText,
    Fk,
    EdgeTable,
    Index,
    Copies,
}

impl Field {
    pub const ALL: [Self; 15] = [
        Self::Trusted,
        Self::PathColumn,
        Self::Complete,
        Self::Accepts,
        Self::Table,
        Self::Exclude,
        Self::Access,
        Self::Immutable,
        Self::Column,
        Self::Selectivity,
        Self::StoredAsText,
        Self::Fk,
        Self::EdgeTable,
        Self::Index,
        Self::Copies,
    ];

    pub fn name(self) -> &'static str {
        self.into()
    }
}

/// The options of a backend block that takes none.
#[derive(Debug, Clone, Default, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct NoOptions {}

impl WrittenBlock for NoOptions {}

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
    connection: ConnectionId,
    stores: Vec<usize>,
    backend: PhantomData<fn() -> B>,
}

impl<B: StorageBackend> Catalog<B> {
    /// The catalog of the one connection that holds the model's stores of this backend.
    pub fn from_model(model: &Arc<Model>) -> Result<Self, DataModelError> {
        let mut connections = model
            .connections()
            .filter(|connection| connection.kind() == B::KIND);
        let connection = connections
            .next()
            .ok_or_else(|| DataModelError::UnknownReference {
                kind: "backend",
                name: B::KIND.to_string(),
            })?;
        if let Some(other) = connections.next() {
            return Err(DataModelError::Invalid(format!(
                "{} stores sit on connections {} and {}",
                B::KIND,
                connection.name(),
                other.name()
            )));
        }
        model.physical(connection.id())
    }

    /// The catalog of the embedded packages as a deployment of the shape `shape` reads them.
    pub fn embedded(shape: &str) -> Result<Self, DataModelError> {
        Self::from_model(&Packages::embedded().deployment(shape)?)
    }

    pub fn model(&self) -> &Model {
        &self.model
    }

    /// The model's name resolver, for a frontend and for tests.
    pub fn names(&self) -> super::Names<'_> {
        self.model.names()
    }

    /// The model, for callers that keep it past this catalog.
    pub fn shared_model(&self) -> &Arc<Model> {
        &self.model
    }

    pub fn graph(&self) -> &GraphCatalog {
        self.model.graph()
    }

    pub fn connection(&self) -> ConnectionId {
        self.connection
    }

    pub fn stores(&self) -> impl Iterator<Item = (&Store, &B::Store)> {
        self.stores.iter().filter_map(|index| {
            let store = &self.model.stores[*index];
            Some((store, store.facts::<B::Store>()?))
        })
    }

    /// The backend facts of `store`, when it sits on this connection.
    pub fn facts(&self, store: crate::StoreId) -> Option<&B::Store> {
        self.stores()
            .find(|(held, _)| held.id() == store)
            .map(|(_, facts)| facts)
    }

    pub fn sources(&self, entity: EntityId) -> impl Iterator<Item = (&Store, &Source)> {
        self.stores()
            .filter_map(move |(store, _)| Some((store, store.source(entity)?)))
    }

    /// The store on this connection that plans read the entity from: its complete store when the
    /// connection holds it, else the one partial store here.
    pub(crate) fn home(&self, entity: EntityId) -> Option<(&Store, &B::Store)> {
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
    /// The catalog of `connection`, whose stores are all of backend `B`.
    pub fn physical<B: StorageBackend>(
        self: &Arc<Self>,
        connection: ConnectionId,
    ) -> Result<Catalog<B>, DataModelError> {
        let stores: Vec<usize> = (0..self.stores.len())
            .filter(|index| self.stores[*index].connection_id == connection)
            .collect();
        let served = self.connection(connection);
        if served.kind() != B::KIND {
            let mut diagnostics = Diagnostics::default();
            let package = stores
                .first()
                .map_or("", |index| self.stores[*index].package.as_str());
            diagnostics.error(
                package,
                format!(
                    "connection {} is {}, not {}",
                    served.name(),
                    served.kind(),
                    B::KIND
                ),
            );
            diagnostics.finish()?;
        }
        Ok(Catalog {
            model: Arc::clone(self),
            connection,
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

    fn capabilities(&self) -> Capabilities;

    fn serves(
        &self,
        facts: &Facts,
        entity: EntityId,
        property: PropertyId,
        operation: Operation,
    ) -> bool;

    fn policies(&self) -> &'static [PolicyKind];

    fn fields(&self) -> &'static [Field];

    fn renders(&self, policy: PolicyKind) -> bool {
        self.policies().contains(&policy) || policy == PolicyKind::Via && !self.capabilities().joins
    }

    fn reads(&self, field: Field) -> bool {
        self.fields().contains(&field)
    }

    fn parse(&self, yaml: &str) -> Result<StorageFragment, orbit_utils::yaml::Error>;

    /// The schema of a package file, whose storage fragments are this backend's.
    fn schema(&self) -> schemars::Schema;

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

pub(crate) fn backends() -> impl Iterator<Item = &'static dyn Registered> {
    REGISTRY.into_iter()
}

pub(crate) fn registered(kind: &str) -> Option<&'static dyn Registered> {
    backends().find(|backend| backend.kind() == kind)
}

impl<B: StorageBackend> Registered for Entry<B> {
    fn kind(&self) -> &'static str {
        B::KIND
    }

    fn capabilities(&self) -> Capabilities {
        B::CAPABILITIES
    }

    fn serves(
        &self,
        facts: &Facts,
        entity: EntityId,
        property: PropertyId,
        operation: Operation,
    ) -> bool {
        (facts.as_ref() as &dyn std::any::Any)
            .downcast_ref::<B::Store>()
            .is_some_and(|store| B::serves(store, entity, property, operation))
    }

    fn policies(&self) -> &'static [PolicyKind] {
        B::POLICIES
    }

    fn fields(&self) -> &'static [Field] {
        B::FIELDS
    }

    fn parse(&self, yaml: &str) -> Result<StorageFragment, orbit_utils::yaml::Error> {
        StorageFragment::<B>::parse(yaml)
    }

    fn schema(&self) -> schemars::Schema {
        schemars::schema_for!(crate::spec::Fragment<StorageFragment<B>>)
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
                .filter(|other| other.connection_name == store.connection_name)
                .collect(),
            options,
            entities,
        };
        Some(Box::new(B::resolve(input, diagnostics)))
    }
}
