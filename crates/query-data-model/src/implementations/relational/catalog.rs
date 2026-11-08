use super::RelationalFacts;
use crate::catalog::{Catalog, Lookup, StorageBackend};
use crate::{EntityId, PropertyId};

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
}
