mod catalog;
mod ids;

use std::collections::{HashMap, HashSet};
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

#[derive(Debug, Clone)]
pub struct ForeignKey {
    pub holder: EntityId,
    pub column: String,
}

#[derive(Debug, Clone)]
pub struct TraversalPathLookup {
    pub table: String,
    pub property: PropertyId,
}

pub type DenormalizedKey = (String, String, String);
pub type DenormalizedColumns = HashMap<DenormalizedKey, (String, String)>;
pub type DenormalizedRelationships = HashMap<DenormalizedKey, Vec<String>>;

#[derive(Debug, Default)]
pub struct DenormalizedCatalog {
    pub columns: DenormalizedColumns,
    pub relationships: DenormalizedRelationships,
}

pub trait QueryBackendCatalog {
    fn supports_foreign_key_elision(&self) -> bool;
    fn requires_node_joins(&self) -> bool;
    fn requires_node_projection(&self) -> bool;
    fn entity_table(&self, entity: EntityId) -> Option<&str>;
    fn entity_has_traversal_path(&self, entity: EntityId) -> bool;
    fn entity_is_global(&self, entity: EntityId) -> bool;
    fn property_column(&self, property: PropertyId) -> Option<&str>;
    fn default_edge_table(&self) -> &str;
    fn relationship_table(&self, relationship: RelationshipId) -> Option<&str>;
    fn edge_tables(&self, relationships: &[RelationshipId]) -> Vec<String>;
    fn foreign_key(
        &self,
        graph: &GraphCatalog,
        relationships: &[RelationshipId],
        source: EntityId,
        target: EntityId,
    ) -> Option<ForeignKey>;
    fn table_columns(&self, table: &str) -> Option<&HashSet<String>>;
    fn table_sort_key(&self, table: &str) -> Option<&[String]>;
    fn denormalized(&self) -> &DenormalizedCatalog;
    fn traversal_path_lookup(
        &self,
        entity: EntityId,
        kind: ontology::TraversalPathKind,
    ) -> Option<&TraversalPathLookup>;
}

pub trait QueryAuthorizationCatalog {
    fn variant_scope(&self, variant: RelationshipVariantId) -> Option<ontology::EdgeVariantScope>;
}

pub trait QueryDataModel {
    type BackendCatalog: QueryBackendCatalog;
    type AuthorizationCatalog: QueryAuthorizationCatalog;

    fn ontology(&self) -> &ontology::Ontology;
    fn graph(&self) -> &GraphCatalog;
    fn query_backend(&self) -> &Self::BackendCatalog;
    fn query_authorization(&self) -> &Self::AuthorizationCatalog;
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

impl<B, A> QueryDataModel for DataModel<B, A>
where
    B: Backend,
    A: Authz,
    B::Catalog: QueryBackendCatalog,
    A::Catalog: QueryAuthorizationCatalog,
{
    type BackendCatalog = B::Catalog;
    type AuthorizationCatalog = A::Catalog;

    fn ontology(&self) -> &ontology::Ontology {
        &self.ontology
    }

    fn graph(&self) -> &GraphCatalog {
        &self.graph
    }

    fn query_backend(&self) -> &Self::BackendCatalog {
        &self.backend
    }

    fn query_authorization(&self) -> &Self::AuthorizationCatalog {
        &self.authorization
    }
}
