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
    fn anchor_foreign_keys(&self) -> &HashMap<String, EntityId>;
    fn is_admin_only(&self, property: PropertyId) -> bool;
}

pub trait QueryDataModel {
    type BackendCatalog: QueryBackendCatalog;
    type AuthorizationCatalog: QueryAuthorizationCatalog;

    fn ontology(&self) -> &ontology::Ontology;
    fn graph(&self) -> &GraphCatalog;
    fn query_backend(&self) -> &Self::BackendCatalog;
    fn query_authorization(&self) -> &Self::AuthorizationCatalog;

    fn entity(&self, name: &str) -> Option<&Entity> {
        self.graph().entity_named(name)
    }

    fn property(&self, entity: &str, property: &str) -> Option<&Property> {
        self.graph().property_named(entity, property)
    }

    fn entity_table(&self, entity: &str) -> Option<&str> {
        let entity = self.graph().entity_id(entity)?;
        self.query_backend().entity_table(entity)
    }

    fn entity_has_traversal_path(&self, entity: &str) -> bool {
        self.graph()
            .entity_id(entity)
            .is_some_and(|entity| self.query_backend().entity_has_traversal_path(entity))
    }

    fn property_column_named(&self, entity: &str, property: &str) -> Option<&str> {
        let property = self.property(entity, property)?;
        self.query_backend().property_column(property.id)
    }

    fn admin_only(&self, entity: &str, property: &str) -> bool {
        self.property(entity, property)
            .is_some_and(|property| self.query_authorization().is_admin_only(property.id))
    }

    fn relationship_table(&self, relationship: &str) -> Option<&str> {
        let relationship = self.graph().relationship_id(relationship)?;
        self.query_backend().relationship_table(relationship)
    }

    fn variant_scope(
        &self,
        relationship: &str,
        source: &str,
        target: &str,
    ) -> Option<ontology::EdgeVariantScope> {
        let relationship = self.graph().relationship_id(relationship)?;
        let source = self.graph().entity_id(source)?;
        let target = self.graph().entity_id(target)?;
        let variant = self.graph().variant_id(relationship, source, target)?;
        self.query_authorization().variant_scope(variant)
    }

    fn traversal_path_lookup(
        &self,
        entity: &str,
        kind: ontology::TraversalPathKind,
    ) -> Option<(&str, &str)> {
        let entity = self.graph().entity_id(entity)?;
        let lookup = self.query_backend().traversal_path_lookup(entity, kind)?;
        let column = self.query_backend().property_column(lookup.property)?;
        Some((&lookup.table, column))
    }
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
