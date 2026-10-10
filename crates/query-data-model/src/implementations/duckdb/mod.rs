pub(crate) mod mapping;
pub mod storage;

use std::collections::HashSet;

use super::PropertyBackendFacts;
use crate::{
    DenormalizedCatalog, EntityId, GraphCatalog, PropertyId, PropertyRealization,
    QueryBackendCatalog, RelationshipId, TraversalPathLookup,
};

#[derive(Debug, Clone)]
pub struct DuckDbEntityLayout {
    pub table: String,
    pub default_properties: Vec<PropertyId>,
    pub sort_key: Vec<String>,
    pub has_traversal_path: bool,
}

#[derive(Debug)]
pub struct DuckDbCatalog {
    edge_table: String,
    edge_sort_key: Vec<String>,
    edge_column_types: std::collections::BTreeMap<String, ontology::DataType>,
    edge_columns: HashSet<String>,
    entities: Vec<Option<DuckDbEntityLayout>>,
    property_facts: Vec<PropertyBackendFacts>,
    relationships: Vec<String>,
    denormalized: DenormalizedCatalog,
}

impl QueryBackendCatalog for DuckDbCatalog {
    fn entity_table(&self, entity: EntityId) -> Option<&str> {
        self.entity(entity).map(|layout| layout.table.as_str())
    }

    fn entity_has_traversal_path(&self, entity: EntityId) -> bool {
        self.entity(entity)
            .is_some_and(|layout| layout.has_traversal_path)
    }

    fn entity_is_global(&self, _entity: EntityId) -> bool {
        false
    }

    fn default_properties(&self, entity: EntityId) -> &[PropertyId] {
        self.entity(entity)
            .map(|layout| layout.default_properties.as_slice())
            .unwrap_or_default()
    }

    fn property_realization(&self, property: PropertyId) -> Option<&PropertyRealization> {
        self.property_facts
            .get(property.index())
            .and_then(|facts| facts.realization.as_ref())
    }

    fn property_selectivity(&self, property: PropertyId) -> Option<ontology::FieldSelectivity> {
        self.property_facts
            .get(property.index())
            .map(|facts| facts.selectivity)
    }

    fn table_column_type(&self, table: &str, column: &str) -> Option<ontology::DataType> {
        (table == self.edge_table())
            .then(|| self.edge_column_type(column))
            .flatten()
    }

    fn has_text_index(&self, _property: PropertyId) -> bool {
        false
    }

    fn table_path_scopable(&self, _table: &str) -> bool {
        false
    }

    fn table_path_columns(&self, _table: &str) -> Option<&[crate::PathColumn]> {
        None
    }

    fn default_edge_table(&self) -> &str {
        self.edge_table()
    }

    fn relationship_table(&self, relationship: RelationshipId) -> Option<&str> {
        DuckDbCatalog::relationship_table(self, relationship)
    }

    fn edge_tables(&self, _relationships: &[RelationshipId]) -> Vec<String> {
        vec![self.edge_table().to_string()]
    }

    fn foreign_key(
        &self,
        _graph: &GraphCatalog,
        _relationships: &[RelationshipId],
        _source: EntityId,
        _target: EntityId,
    ) -> Option<crate::ForeignKey> {
        None
    }

    fn table_columns(&self, table: &str) -> Option<&HashSet<String>> {
        (table == self.edge_table()).then(|| self.edge_columns())
    }

    fn table_sort_key(&self, table: &str) -> Option<&[String]> {
        if table == self.edge_table() {
            return Some(&self.edge_sort_key);
        }
        self.entities
            .iter()
            .flatten()
            .find(|layout| layout.table == table)
            .map(|layout| layout.sort_key.as_slice())
    }

    fn denormalized(&self) -> &DenormalizedCatalog {
        &self.denormalized
    }

    fn traversal_path_lookup(
        &self,
        _entity: EntityId,
        _kind: ontology::TraversalPathKind,
    ) -> Option<&TraversalPathLookup> {
        None
    }
}

impl DuckDbCatalog {
    pub fn entity(&self, id: EntityId) -> Option<&DuckDbEntityLayout> {
        self.entities.get(id.index())?.as_ref()
    }

    pub fn relationship_table(&self, id: RelationshipId) -> Option<&str> {
        self.relationships.get(id.index()).map(String::as_str)
    }

    pub fn edge_table(&self) -> &str {
        &self.edge_table
    }

    pub fn edge_columns(&self) -> &HashSet<String> {
        &self.edge_columns
    }

    pub fn edge_column_type(&self, column: &str) -> Option<ontology::DataType> {
        self.edge_column_types.get(column).copied()
    }
}
