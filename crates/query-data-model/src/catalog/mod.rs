mod access;
mod backend;
mod bindings;
mod composition;
mod diagnostics;
mod guards;
mod layout;
mod lookups;
mod names;
pub mod sources;
mod store;
mod writers;

pub use access::{
    Access, Check, EdgeScope, EntityAccess, Permission, PermissionId, Policy, PropertyAccess,
    ProviderUse, RelationshipAccess, Scope, ScopeId, ScopeKind,
};
pub(crate) use backend::registered;
pub use backend::{
    Catalog, ColumnInput, ExecuteError, NoOptions, Render, Row, SourceInput, StorageBackend,
    StoreInput, check_unrelational,
};
pub use diagnostics::{Diagnostics, SpecError};
pub use guards::{Guard, PolicyKind, Rule};
pub use lookups::{Lookup, Lookups};
pub use names::{Reversed, Unresolved};
pub use sources::Sources;
pub use store::{Source, Store, StoredColumn};

use std::collections::HashSet;

use sha2::{Digest as _, Sha256};

use crate::packages::{Tier, directory};
use crate::spec::{Fragment, Fragments};
use crate::{DataModelError, EntityId, GraphCatalog, PropertyId};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ModelId([u8; 32]);

#[derive(Debug)]
pub struct Model {
    id: ModelId,
    graph: GraphCatalog,
    access: Access,
    lookups: Lookups,
    stores: Vec<Store>,
    sources: Sources,
}

impl Model {
    /// A model of package files that were all loaded at `tier`.
    pub fn from_yaml<'a>(
        tier: Tier,
        sources: impl IntoIterator<Item = (&'a str, &'a str)>,
    ) -> Result<Self, DataModelError> {
        let sources: Vec<(&str, &str)> = sources.into_iter().collect();
        let tiers = sources
            .iter()
            .map(|(path, _)| (directory(path).to_string(), tier))
            .collect();
        Self::load(&Fragments::parse(sources, &tiers)?)
    }

    pub fn load(fragments: &Fragments) -> Result<Self, DataModelError> {
        let mut diagnostics = Diagnostics::default();
        let (mut graphs, mut authorizations, mut storages, mut services, mut sources) =
            (Vec::new(), Vec::new(), Vec::new(), Vec::new(), Vec::new());
        for fragment in fragments.iter() {
            match fragment {
                Fragment::Graph(graph) => graphs.push(graph.clone()),
                Fragment::Authorization(access) => authorizations.push(access),
                Fragment::Storage(storage) => storages.push(storage.as_ref()),
                Fragment::Service(service) => services.push(service),
                Fragment::Source(source) => sources.push(source.as_ref()),
                Fragment::Package(_) => {}
            }
        }

        let graph = GraphCatalog::from_spec(graphs, &mut diagnostics);
        let access = Access::resolve(&graph, &authorizations, &mut diagnostics);
        composition::check(fragments, &access, &mut diagnostics);
        let lookups = Lookups::resolve(&graph, &services, &mut diagnostics);
        let served: HashSet<PropertyId> = lookups.served().collect();
        let (mut stores, blocks) = layout::resolve(&graph, &storages, &served, &mut diagnostics);
        let complete = access.covers(&graph);
        if complete {
            guards::resolve(&graph, &access, &mut stores, &mut diagnostics);
        }
        let global = |entity| complete && access.is_global(entity);
        let sources = Sources::resolve(
            &graph,
            &global,
            &sources,
            &served,
            &stores,
            &mut diagnostics,
        );
        diagnostics.finish()?;
        let mut model = Self {
            id: fragments.id(),
            graph,
            access,
            lookups,
            stores,
            sources,
        };
        let mut diagnostics = Diagnostics::default();
        model.resolve_backends(&blocks, &mut diagnostics);
        diagnostics.finish()?;
        Ok(model)
    }

    pub fn id(&self) -> ModelId {
        self.id
    }

    pub fn graph(&self) -> &GraphCatalog {
        &self.graph
    }

    pub fn access(&self) -> &Access {
        &self.access
    }

    pub fn lookup(&self, property: PropertyId) -> Option<&Lookup> {
        self.lookups.get(property)
    }

    pub fn stores(&self) -> &[Store] {
        &self.stores
    }

    pub fn sources(&self) -> &Sources {
        &self.sources
    }

    /// The foreign key some store binds the variant to.
    pub fn foreign_key(&self, variant: crate::RelationshipVariantId) -> Option<crate::ForeignKey> {
        self.stores
            .iter()
            .find_map(|store| store.foreign_key(variant))
    }

    /// Whether some store binds a variant to this property as its foreign key.
    pub fn holds_foreign_key(&self, property: PropertyId) -> bool {
        self.stores.iter().any(|store| {
            store.bindings().any(|(_, _, binding)| {
                binding
                    .fk
                    .as_ref()
                    .is_some_and(|key| key.foreign_key.property == property)
            })
        })
    }

    /// The one store that holds or excludes every property of the entity.
    pub fn complete_store(&self, entity: EntityId) -> Option<&Store> {
        self.stores.iter().find(|store| store.complete(entity))
    }

    /// How the entity's complete store keeps an enum property: as labels or as integer codes.
    pub fn enum_type(&self, property: PropertyId) -> crate::EnumType {
        let entity = self.graph.property(property).entity;
        match self
            .complete_store(entity)
            .is_some_and(|store| store.stored_as_text(property))
        {
            true => crate::EnumType::String,
            false => crate::EnumType::Int,
        }
    }

    /// Whether every store that holds the property marks it immutable.
    pub fn immutable(&self, property: PropertyId) -> bool {
        let mut holders = self
            .stores
            .iter()
            .filter(|store| store.column(property).is_some())
            .peekable();
        holders.peek().is_some() && holders.all(|store| store.immutable(property))
    }
}

impl ModelId {
    /// The digest of `parts`, each length-prefixed so no two sequences of parts collide.
    pub(crate) fn digest<'a>(parts: impl IntoIterator<Item = &'a [u8]>) -> Self {
        let mut hasher = Sha256::new();
        for part in parts {
            hasher.update(part.len().to_le_bytes());
            hasher.update(part);
        }
        Self(hasher.finalize().into())
    }

    pub(crate) fn tagged(self, tag: &str) -> Self {
        Self::digest([self.0.as_slice(), tag.as_bytes()])
    }
}
