mod catalog;
mod ids;

pub use catalog::{Entity, GraphCatalog, Property, Relationship, RelationshipVariant};
pub use ids::{EntityId, PropertyId, RelationshipId, RelationshipVariantId};

use crate::{DataModelError, Layout, Mapping};

pub struct DataModel<S, M: Mapping<S>, A> {
    graph: GraphCatalog,
    layout: Layout<S, M>,
    authorization: A,
}

impl<S, M: Mapping<S>, A> DataModel<S, M, A> {
    pub fn build(graph: GraphCatalog, schema: S, authorization: A) -> Result<Self, DataModelError> {
        let layout = Layout::build(&graph, schema)?;
        Ok(Self {
            graph,
            layout,
            authorization,
        })
    }

    pub fn graph(&self) -> &GraphCatalog {
        &self.graph
    }

    pub fn layout(&self) -> &Layout<S, M> {
        &self.layout
    }

    pub fn backend(&self) -> &M {
        self.layout.mapping()
    }

    pub fn authorization(&self) -> &A {
        &self.authorization
    }
}
