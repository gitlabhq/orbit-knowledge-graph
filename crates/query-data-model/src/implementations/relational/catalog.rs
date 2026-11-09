use ontology::{EdgeVariantScope, RequiredRole};

use super::RelationalFacts;
use crate::catalog::{Catalog, Lookup, Redaction, StorageBackend};
use crate::{EntityId, PropertyId, RelationshipVariantId};

impl<B: StorageBackend> Catalog<B>
where
    B::Store: RelationalFacts,
{
    pub fn table_of(&self, entity: EntityId) -> Option<&str> {
        Some(&self.source(entity)?.1.table)
    }

    pub fn has_path_column(&self, entity: EntityId) -> bool {
        self.source(entity)
            .is_some_and(|(_, source)| source.path_column.is_some())
    }

    /// The column that stores a property; none when a lookup serves it or its store excludes it.
    pub fn column(&self, property: PropertyId) -> Option<&str> {
        let (store, _) = self.home(self.graph().property(property).entity)?;
        if store.excludes(property) || self.model().lookup(property).is_some() {
            return None;
        }
        store.column(property)
    }

    /// The lookup that serves a property its entity's store holds.
    pub fn lookup(&self, property: PropertyId) -> Option<&Lookup> {
        let (store, _) = self.home(self.graph().property(property).entity)?;
        if store.excludes(property) {
            return None;
        }
        self.model().lookup(property)
    }

    pub fn readable(&self, property: PropertyId) -> bool {
        self.column(property).is_some() || self.lookup(property).is_some()
    }

    /// Every store on the connection is trusted, so no read is scoped or redacted.
    pub fn trusted(&self) -> bool {
        self.stores().all(|(store, _)| store.trusted)
    }

    pub fn variant_scope(&self, variant: RelationshipVariantId) -> Option<EdgeVariantScope> {
        match self.trusted() {
            true => None,
            false => self.model().access().variant_scope(variant),
        }
    }

    pub fn admin_only(&self, property: PropertyId) -> bool {
        !self.trusted() && self.model().access().property(property).admin_only
    }

    pub fn redaction(&self, entity: EntityId) -> Option<&Redaction> {
        match self.trusted() {
            true => None,
            false => self.model().access().entity(entity)?.redaction.as_ref(),
        }
    }

    /// The access level a redacted entity requires; reporter unless it names a role.
    pub fn role_floor(&self, entity: EntityId) -> Option<u32> {
        self.redaction(entity)?;
        let role = self.model().access().entity(entity)?.role;
        Some(role.unwrap_or(RequiredRole::Reporter).as_access_level())
    }

    /// Each foreign key of a namespace-anchor variant, with the entity it anchors to.
    pub fn anchor_keys(&self) -> impl Iterator<Item = (PropertyId, EntityId)> + '_ {
        self.graph()
            .variants()
            .filter(|variant| {
                self.variant_scope(variant.id) == Some(EdgeVariantScope::NamespaceAnchor)
            })
            .filter_map(|variant| {
                let key = self
                    .routes(variant.id)
                    .find_map(|route| route.foreign_key)?;
                Some((key.key.property, variant.target))
            })
    }
}
