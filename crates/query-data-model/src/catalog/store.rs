use std::any::Any;
use std::collections::HashSet;
use std::fmt::Debug;
use std::panic::RefUnwindSafe;

use super::guards::Guard;
use crate::spec::{AccessHints, Binding, Resolved};
use crate::{
    ConnectionId, Endpoint, EntityId, FieldSelectivity, ForeignKey, PropertyId,
    RelationshipVariantId, StoreId,
};

/// A store's backend facts, which only the backend's catalog reads.
pub(crate) type Facts = Box<dyn BackendFacts>;

pub(crate) trait BackendFacts: Any + Debug + Send + Sync + RefUnwindSafe {}

impl<T: Any + Debug + Send + Sync + RefUnwindSafe> BackendFacts for T {}

/// One entity a store holds: its table, when the store is relational, and how queries read it.
#[derive(Debug, Clone)]
pub struct Source {
    pub entity: EntityId,
    pub package: String,
    /// The store holds or excludes every property, and plans read the entity here.
    pub complete: bool,
    /// The stored properties a stage takes key sets on.
    pub accepts: Vec<PropertyId>,
    /// Every property the store lists, in declaration order.
    pub returned: Vec<PropertyId>,
    pub table: Option<String>,
    /// The columns of the entity's identity, one per part; empty when the store returns no
    /// identity.
    pub key: Vec<String>,
    pub path_column: Option<String>,
    pub access: AccessHints<Resolved>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredColumn {
    pub name: String,
    pub immutable: bool,
    pub selectivity: Option<FieldSelectivity>,
    pub stored_as_text: bool,
}

#[derive(Debug)]
pub struct Store {
    pub(crate) name: String,
    pub(crate) connection_name: String,
    pub(crate) connection_id: ConnectionId,
    pub(crate) id: StoreId,
    pub(crate) kind: String,
    pub(crate) trusted: bool,
    pub(crate) path_column: Option<String>,
    pub(crate) package: String,
    pub(super) sources: Vec<Option<Source>>,
    pub(super) columns: Vec<Option<StoredColumn>>,
    pub(super) excluded: HashSet<PropertyId>,
    pub(super) bindings: Vec<Option<(String, Binding<Resolved>)>>,
    pub(super) declared: HashSet<RelationshipVariantId>,
    pub(super) guards: Vec<Option<Guard>>,
    pub(super) edge_guards: std::collections::BTreeMap<String, Guard>,
    pub(super) facts: Option<Facts>,
}

impl Store {
    pub fn id(&self) -> StoreId {
        self.id
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    /// Nothing restricts the store's reads.
    pub fn trusted(&self) -> bool {
        self.trusted
    }

    /// The connection the store sits on.
    pub fn connection(&self) -> ConnectionId {
        self.connection_id
    }

    pub fn source(&self, entity: EntityId) -> Option<&Source> {
        self.sources[entity.index()].as_ref()
    }

    pub fn sources(&self) -> impl Iterator<Item = &Source> {
        self.sources.iter().flatten()
    }

    pub fn column(&self, property: PropertyId) -> Option<&str> {
        Some(&self.columns[property.index()].as_ref()?.name)
    }

    pub fn immutable(&self, property: PropertyId) -> bool {
        self.columns[property.index()]
            .as_ref()
            .is_some_and(|column| column.immutable)
    }

    /// The selectivity hint the store declares for the column, if any.
    pub fn selectivity(&self, property: PropertyId) -> Option<FieldSelectivity> {
        self.columns[property.index()].as_ref()?.selectivity
    }

    pub fn stored_as_text(&self, property: PropertyId) -> bool {
        self.columns[property.index()]
            .as_ref()
            .is_some_and(|column| column.stored_as_text)
    }

    /// Every property of `entity` the store lists, each of which its reads return.
    pub fn returns(&self, entity: EntityId) -> impl Iterator<Item = PropertyId> + '_ {
        self.source(entity)
            .into_iter()
            .flat_map(|source| source.returned.iter().copied())
    }

    /// Whether the store's reads of `entity` return its whole identity.
    pub fn returns_identity(&self, entity: EntityId) -> bool {
        self.source(entity)
            .is_some_and(|source| !source.key.is_empty())
    }

    /// Whether a stage on this store takes a key set on `property`.
    pub fn accepts(&self, entity: EntityId, property: PropertyId) -> bool {
        self.source(entity)
            .is_some_and(|source| source.accepts.contains(&property))
    }

    pub fn complete(&self, entity: EntityId) -> bool {
        self.source(entity).is_some_and(|source| source.complete)
    }

    pub fn excludes(&self, property: PropertyId) -> bool {
        self.excluded.contains(&property)
    }

    pub fn binding(&self, variant: RelationshipVariantId) -> Option<&Binding<Resolved>> {
        Some(&self.bindings[variant.index()].as_ref()?.1)
    }

    pub fn bindings(
        &self,
    ) -> impl Iterator<Item = (RelationshipVariantId, &str, &Binding<Resolved>)> {
        self.bindings
            .iter()
            .enumerate()
            .filter_map(|(index, bound)| {
                let (package, binding) = bound.as_ref()?;
                Some((RelationshipVariantId(index), package.as_str(), binding))
            })
    }

    /// The foreign key a plan can join on; a discriminated or composite key is not one.
    pub fn foreign_key(&self, variant: RelationshipVariantId) -> Option<ForeignKey> {
        let key = self.binding(variant)?.fk.as_ref()?;
        (key.discriminator.is_none() && key.composite.is_empty()).then_some(key.foreign_key)
    }

    /// The holder and the column pairs of a key on a composite identity, which only the Combine stage
    /// joins on, between stages.
    pub fn composite_key(
        &self,
        variant: RelationshipVariantId,
    ) -> Option<(Endpoint, &[(PropertyId, PropertyId)])> {
        let key = self.binding(variant)?.fk.as_ref()?;
        (!key.composite.is_empty()).then_some((key.foreign_key.holder, key.composite.as_slice()))
    }

    /// The edge table a variant's rows live in, or the one derived from its foreign key.
    pub fn edge_table(&self, variant: RelationshipVariantId) -> Option<&str> {
        let binding = self.binding(variant)?;
        let index = binding
            .index
            .as_ref()
            .map(|index| index.edge_table.as_str());
        binding.edge_table.as_deref().or(index)
    }

    /// The edge column that copies `property` of the variant's `endpoint`.
    pub fn copy(
        &self,
        variant: RelationshipVariantId,
        endpoint: Endpoint,
        property: PropertyId,
    ) -> Option<&str> {
        self.binding(variant)?
            .copies
            .iter()
            .find(|(_, sources)| sources.contains(&(endpoint, property)))
            .map(|(column, _)| column.as_str())
    }

    pub fn guard(&self, entity: EntityId) -> Option<&Guard> {
        self.guards[entity.index()].as_ref()
    }

    /// The guard of an edge table the store holds.
    pub fn edge_guard(&self, table: &str) -> Option<&Guard> {
        self.edge_guards.get(table)
    }

    pub(super) fn facts<T: 'static>(&self) -> Option<&T> {
        (self.facts.as_deref()? as &dyn Any).downcast_ref()
    }
}
