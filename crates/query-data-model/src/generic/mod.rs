mod catalog;
mod ids;

use std::marker::PhantomData;
use std::sync::Arc;

pub use catalog::{
    Entity, GraphCatalog, Property, PropertyRealization, Relationship, RelationshipVariant,
};
pub use ids::{EntityId, PropertyId, RelationshipId, RelationshipVariantId};

use crate::DataModelError;

pub trait Backend: Send + Sync + 'static {
    type Catalog: Send + Sync;

    fn derive(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self::Catalog, DataModelError>;
}

pub trait Authz: Send + Sync + 'static {
    type Catalog: Send + Sync;

    fn derive(
        ontology: &ontology::Ontology,
        graph: &GraphCatalog,
    ) -> Result<Self::Catalog, DataModelError>;
}

pub struct DataModel<B: Backend, A: Authz> {
    ontology: Arc<ontology::Ontology>,
    graph: GraphCatalog,
    backend: B::Catalog,
    authorization: A::Catalog,
    marker: PhantomData<(B, A)>,
}

impl<B: Backend, A: Authz> DataModel<B, A> {
    pub fn derive(ontology: Arc<ontology::Ontology>) -> Result<Self, DataModelError> {
        let graph = GraphCatalog::derive(&ontology)?;
        let backend = B::derive(&ontology, &graph)?;
        let authorization = A::derive(&ontology, &graph)?;

        Ok(Self {
            ontology,
            graph,
            backend,
            authorization,
            marker: PhantomData,
        })
    }

    pub fn ontology(&self) -> &Arc<ontology::Ontology> {
        &self.ontology
    }

    pub fn graph(&self) -> &GraphCatalog {
        &self.graph
    }

    pub fn backend(&self) -> &B::Catalog {
        &self.backend
    }

    pub fn authorization(&self) -> &A::Catalog {
        &self.authorization
    }
}
