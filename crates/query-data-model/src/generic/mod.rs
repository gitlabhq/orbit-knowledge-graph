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

#[derive(Debug, Clone)]
pub struct PathColumn {
    pub name: String,
    pub entity: Option<EntityId>,
}

#[derive(Debug)]
pub struct RelationshipRoute<'a> {
    pub table: &'a str,
    pub sources: &'a [EntityId],
    pub targets: &'a [EntityId],
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
    fn default_properties(&self, entity: EntityId) -> &[PropertyId];
    fn property_column(&self, property: PropertyId) -> Option<&str>;
    fn table_column_type(&self, table: &str, column: &str) -> Option<ontology::DataType>;
    fn has_text_index(&self, property: PropertyId) -> bool;
    fn table_path_scopable(&self, table: &str) -> bool;
    fn table_path_columns(&self, table: &str) -> Option<&[PathColumn]>;
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
    fn entity_auth(&self) -> &HashMap<String, crate::EntityAuthConfig>;
    fn redaction_id_property(&self, entity: EntityId) -> Option<PropertyId>;
    fn required_access_level(&self, entity: EntityId) -> Option<u32>;
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

    fn property_for_entity_id(&self, entity: EntityId, property: &str) -> Option<&Property> {
        self.graph()
            .property_id(entity, property)
            .map(|property| self.graph().property(property))
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

    fn entity_is_global(&self, entity: &str) -> bool {
        self.graph()
            .entity_id(entity)
            .is_some_and(|entity| self.query_backend().entity_is_global(entity))
    }

    fn property_column_named(&self, entity: &str, property: &str) -> Option<&str> {
        let property = self.property(entity, property)?;
        self.query_backend().property_column(property.id)
    }

    fn default_properties(&self, entity: EntityId) -> &[PropertyId] {
        self.query_backend().default_properties(entity)
    }

    fn table_column_type(&self, table: &str, column: &str) -> Option<ontology::DataType> {
        self.query_backend().table_column_type(table, column)
    }

    fn table_columns(&self, table: &str) -> Option<&HashSet<String>> {
        self.query_backend().table_columns(table)
    }

    fn table_sort_key(&self, table: &str) -> Option<&[String]> {
        self.query_backend().table_sort_key(table)
    }

    fn has_text_index(&self, property: PropertyId) -> bool {
        self.query_backend().has_text_index(property)
    }

    fn admin_only(&self, entity: &str, property: &str) -> bool {
        self.property(entity, property)
            .is_some_and(|property| self.query_authorization().is_admin_only(property.id))
    }

    fn redaction_id_column(&self, entity: EntityId) -> Option<&str> {
        let property = self.query_authorization().redaction_id_property(entity)?;
        self.query_backend().property_column(property)
    }

    fn table_path_scopable(&self, table: &str) -> bool {
        self.query_backend().table_path_scopable(table)
    }

    fn table_has_path_columns(&self, table: &str) -> bool {
        self.query_backend()
            .table_path_columns(table)
            .is_none_or(|columns| !columns.is_empty())
    }

    fn table_minimum_access_level(&self, table: &str) -> u32 {
        self.query_backend()
            .table_path_columns(table)
            .into_iter()
            .flatten()
            .filter_map(|column| column.entity)
            .filter_map(|entity| self.query_authorization().required_access_level(entity))
            .max()
            .unwrap_or(ontology::RequiredRole::Reporter.as_access_level())
    }

    fn relationship_table(&self, relationship: &str) -> Option<&str> {
        let relationship = self.graph().relationship_id(relationship)?;
        self.query_backend().relationship_table(relationship)
    }

    fn default_edge_table(&self) -> &str {
        self.query_backend().default_edge_table()
    }

    fn denormalized(&self) -> &DenormalizedCatalog {
        self.query_backend().denormalized()
    }

    fn relationship_tables(&self, relationships: &[String]) -> Vec<String> {
        let relationships: Vec<_> = relationships
            .iter()
            .filter_map(|relationship| self.graph().relationship_id(relationship))
            .collect();
        self.query_backend().edge_tables(&relationships)
    }

    fn relationship_route(&self, relationship: &str) -> Option<RelationshipRoute<'_>> {
        let relationship = self.graph().relationship_id(relationship)?;
        let graph_relationship = self.graph().relationship(relationship);
        Some(RelationshipRoute {
            table: self
                .query_backend()
                .relationship_table(relationship)
                .unwrap_or_else(|| self.default_edge_table()),
            sources: &graph_relationship.sources,
            targets: &graph_relationship.targets,
        })
    }

    fn foreign_key(
        &self,
        relationships: &[String],
        source: &str,
        target: &str,
    ) -> Option<ForeignKey> {
        let relationships: Vec<_> = relationships
            .iter()
            .filter_map(|relationship| self.graph().relationship_id(relationship))
            .collect();
        let source = self.graph().entity_id(source)?;
        let target = self.graph().entity_id(target)?;
        self.query_backend()
            .foreign_key(self.graph(), &relationships, source, target)
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
