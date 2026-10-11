mod catalog;
mod ids;

pub use catalog::{Entity, GraphCatalog, Property, Relationship, RelationshipVariant};
pub use ids::{EntityId, PropertyId, RelationshipId, RelationshipVariantId};

use crate::{Layout, LayoutModel};

pub struct DataModel<T: LayoutModel, A> {
    graph: GraphCatalog,
    layout: Layout<T>,
    authorization: A,
}

impl<T: LayoutModel, A> DataModel<T, A> {
    pub fn new(graph: GraphCatalog, layout: Layout<T>, authorization: A) -> Self {
        Self {
            graph,
            layout,
            authorization,
        }
    }

    pub fn graph(&self) -> &GraphCatalog {
        &self.graph
    }

    pub fn layout(&self) -> &Layout<T> {
        &self.layout
    }

    pub fn backend(&self) -> &T::Mapping {
        &self.layout.mapping
    }

    pub fn authorization(&self) -> &A {
        &self.authorization
    }
}
