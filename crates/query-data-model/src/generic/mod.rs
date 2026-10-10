mod catalog;
mod ids;

pub use catalog::{Entity, GraphCatalog, Property, Relationship, RelationshipVariant};
pub use ids::{EntityId, PropertyId, RelationshipId, RelationshipVariantId};

use crate::{Storage, StorageModel};

pub struct DataModel<T: StorageModel, A> {
    graph: GraphCatalog,
    storage: Storage<T>,
    authorization: A,
}

impl<T: StorageModel, A> DataModel<T, A> {
    pub fn new(graph: GraphCatalog, storage: Storage<T>, authorization: A) -> Self {
        Self {
            graph,
            storage,
            authorization,
        }
    }

    pub fn graph(&self) -> &GraphCatalog {
        &self.graph
    }

    pub fn storage(&self) -> &Storage<T> {
        &self.storage
    }

    pub fn backend(&self) -> &T::Mapping {
        &self.storage.mapping
    }

    pub fn authorization(&self) -> &A {
        &self.authorization
    }
}
