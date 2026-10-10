use std::collections::{HashMap, HashSet};

use crate::storage::relational::{
    DenormalizedCatalog, ForeignKey, PropertyRealization, RelationalMapping, RelationshipRoute,
};
use crate::{Entity, EntityId, GraphCatalog, Property, PropertyId};

use crate::GitLabPolicy;

impl<B, A> OrbitQueryModel for crate::DataModel<crate::Relational<B>, A>
where
    B: crate::RelationalBackend,
    B::Mapping: RelationalMapping,
    A: GitLabPolicy,
{
    type BackendCatalog = B::Mapping;
    type AuthorizationCatalog = A;

    fn graph(&self) -> &GraphCatalog {
        self.graph()
    }
    fn query_backend(&self) -> &B::Mapping {
        self.backend()
    }
    fn query_authorization(&self) -> &A {
        self.authorization()
    }
}

pub trait OrbitQueryModel {
    type BackendCatalog: RelationalMapping;
    type AuthorizationCatalog: GitLabPolicy;

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
        self.property_column(property.id)
    }

    fn property_column(&self, property: PropertyId) -> Option<&str> {
        self.query_backend().property_column(property)
    }

    fn property_is_stored(&self, property: PropertyId) -> bool {
        self.property_column(property).is_some()
    }

    fn property_realization(&self, property: PropertyId) -> Option<&PropertyRealization> {
        self.query_backend().property_realization(property)
    }

    fn property_selectivity(&self, property: PropertyId) -> ontology::FieldSelectivity {
        self.query_backend()
            .property_selectivity(property)
            .unwrap_or_default()
    }

    fn property_is_virtual(&self, entity: EntityId, property: &str) -> bool {
        self.virtual_source_for_entity_id(entity, property)
            .is_some()
    }

    fn virtual_source_for_entity_id(
        &self,
        entity: EntityId,
        property: &str,
    ) -> Option<&ontology::VirtualSource> {
        let property = self.graph().property_id(entity, property)?;
        let PropertyRealization::Virtual(source) = self.property_realization(property)? else {
            return None;
        };
        Some(source)
    }

    fn virtual_source(&self, entity: &str, property: &str) -> Option<&ontology::VirtualSource> {
        let entity = self.graph().entity_id(entity)?;
        self.virtual_source_for_entity_id(entity, property)
    }

    fn relationship_exists(&self, relationship: &str) -> bool {
        self.graph().relationship_id(relationship).is_some()
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

    fn in_sort_key(&self, table: &str, column: &str) -> bool {
        self.table_sort_key(table)
            .is_some_and(|key| key.iter().any(|sort_column| sort_column == column))
    }

    fn has_text_index(&self, property: PropertyId) -> bool {
        self.query_backend().has_text_index(property)
    }

    fn admin_only(&self, entity: &str, property: &str) -> bool {
        self.property(entity, property)
            .is_some_and(|property| self.query_authorization().is_admin_only(property.id))
    }

    fn entity_auth(&self) -> &HashMap<String, crate::EntityAuthConfig> {
        self.query_authorization().entity_auth()
    }

    fn anchor_foreign_keys(&self) -> &HashMap<String, EntityId> {
        self.query_authorization().anchor_foreign_keys()
    }

    fn entity_minimum_access_level(&self, entity: &str) -> Option<u32> {
        let entity = self.graph().entity_id(entity)?;
        self.query_authorization().required_access_level(entity)
    }

    fn property_is_admin_only(&self, property: PropertyId) -> bool {
        self.query_authorization().is_admin_only(property)
    }

    fn property_is_filterable(&self, entity: &str, property: &str) -> bool {
        self.property(entity, property)
            .is_some_and(|property| self.query_authorization().is_filterable(property.id))
    }

    fn property_allows_like(&self, entity: &str, property: &str) -> bool {
        self.property(entity, property)
            .is_some_and(|property| self.query_authorization().is_like_allowed(property.id))
    }

    fn redaction_id_column(&self, entity: EntityId) -> Option<&str> {
        self.query_authorization().redaction_id_column(entity)
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

    fn relationship_table_for_query(&self, relationships: &[String]) -> &str {
        relationships
            .iter()
            .find_map(|relationship| self.relationship_table(relationship))
            .unwrap_or_else(|| self.default_edge_table())
    }

    fn redaction_id_column_named(&self, entity: &str) -> Option<&str> {
        let entity = self.graph().entity_id(entity)?;
        self.redaction_id_column(entity)
    }

    fn relationship_route(&self, relationship: &str) -> Option<RelationshipRoute<'_>> {
        let relationship = self.graph().relationship_id(relationship)?;
        let graph_relationship = self.graph().relationship(relationship);
        Some(RelationshipRoute {
            table: self
                .query_backend()
                .relationship_table(relationship)
                .unwrap_or_else(|| self.default_edge_table()),
            graph: self.graph(),
            relationship: graph_relationship,
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
