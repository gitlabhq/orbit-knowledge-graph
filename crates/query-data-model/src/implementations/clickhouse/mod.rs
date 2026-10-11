pub mod layout;
pub(crate) mod mapping;

use std::collections::{BTreeSet, HashMap, HashSet};

use super::PropertyBackendFacts;
use crate::{
    DenormalizedCatalog, EntityId, ForeignKey, GraphCatalog, PathColumn, PropertyId,
    PropertyRealization, RelationalMapping, RelationshipId, TraversalPathLookup,
};

#[derive(Debug, Clone)]
pub struct TableLayout {
    pub name: String,
    pub columns: HashSet<String>,
    pub column_types: HashMap<String, ontology::DataType>,
    pub sort_key: Vec<String>,
    pub entity: Option<EntityId>,
    pub path_columns: Vec<PathColumn>,
    pub path_scopable: bool,
}

impl TableLayout {
    fn from_storage(
        table: &layout::Table,
        entity: Option<EntityId>,
        path_columns: Vec<PathColumn>,
        path_scopable: bool,
    ) -> Self {
        Self {
            name: table.name.clone(),
            columns: table
                .columns
                .iter()
                .filter(|column| ontology::denormalized::copies(&column.name))
                .map(|column| column.name.trim_matches('`').to_string())
                .collect(),
            column_types: table
                .column_types
                .iter()
                .filter(|(name, _)| ontology::denormalized::copies(name))
                .map(|(name, data_type)| (name.clone(), *data_type))
                .collect(),
            sort_key: table.sort_key.clone(),
            entity,
            path_columns,
            path_scopable,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EntityLayout {
    pub table: String,
    pub has_traversal_path: bool,
    pub global: bool,
    pub default_properties: Vec<PropertyId>,
}

#[derive(Debug)]
pub struct ClickHouseMapping {
    default_edge_table: String,
    entities: Vec<Option<EntityLayout>>,
    relationships: Vec<Option<String>>,
    variants: Vec<Option<ForeignKey>>,
    property_facts: Vec<PropertyBackendFacts>,
    tables: HashMap<String, TableLayout>,
    denormalized: DenormalizedCatalog,
    traversal_path_lookups: HashMap<(EntityId, ontology::TraversalPathKind), TraversalPathLookup>,
}

impl ClickHouseMapping {
    pub fn entity(&self, id: EntityId) -> Option<&EntityLayout> {
        self.entities.get(id.index())?.as_ref()
    }

    pub fn relationship_table(&self, id: RelationshipId) -> Option<&str> {
        self.relationships.get(id.index())?.as_deref()
    }

    pub fn property_column(&self, id: PropertyId) -> Option<&str> {
        RelationalMapping::property_column(self, id)
    }

    pub fn table(&self, name: &str) -> Option<&TableLayout> {
        self.tables.get(name)
    }

    pub fn table_for_entity(&self, entity: EntityId) -> Option<&TableLayout> {
        self.entity(entity)
            .and_then(|layout| self.table(&layout.table))
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableLayout> {
        self.tables.values()
    }

    pub fn edge_tables(&self) -> impl Iterator<Item = &TableLayout> {
        self.relationships
            .iter()
            .filter_map(Option::as_deref)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|name| self.tables.get(name))
    }

    pub fn default_edge_table(&self) -> &str {
        &self.default_edge_table
    }

    pub fn traversal_path_lookup(
        &self,
        entity: EntityId,
        kind: ontology::TraversalPathKind,
    ) -> Option<&TraversalPathLookup> {
        self.traversal_path_lookups.get(&(entity, kind))
    }
}

impl RelationalMapping for ClickHouseMapping {
    fn entity_table(&self, entity: EntityId) -> Option<&str> {
        self.entity(entity).map(|layout| layout.table.as_str())
    }

    fn entity_has_traversal_path(&self, entity: EntityId) -> bool {
        self.entity(entity)
            .is_some_and(|layout| layout.has_traversal_path)
    }

    fn entity_is_global(&self, entity: EntityId) -> bool {
        self.entity(entity).is_some_and(|layout| layout.global)
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
        self.table(table)
            .and_then(|layout| layout.column_types.get(column).copied())
    }

    fn has_text_index(&self, property: PropertyId) -> bool {
        self.property_facts
            .get(property.index())
            .is_some_and(|facts| facts.has_text_index)
    }

    fn table_path_scopable(&self, table: &str) -> bool {
        self.table(table).is_some_and(|layout| layout.path_scopable)
    }

    fn table_path_columns(&self, table: &str) -> Option<&[PathColumn]> {
        self.table(table)
            .map(|layout| layout.path_columns.as_slice())
    }

    fn default_edge_table(&self) -> &str {
        ClickHouseMapping::default_edge_table(self)
    }

    fn relationship_table(&self, relationship: RelationshipId) -> Option<&str> {
        ClickHouseMapping::relationship_table(self, relationship)
    }

    fn edge_tables(&self, relationships: &[RelationshipId]) -> Vec<String> {
        if relationships.is_empty() {
            return self.edge_tables().map(|table| table.name.clone()).collect();
        }
        relationships
            .iter()
            .filter_map(|relationship| self.relationship_table(*relationship))
            .map(String::from)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    fn foreign_key(
        &self,
        graph: &GraphCatalog,
        relationships: &[RelationshipId],
        source: EntityId,
        target: EntityId,
    ) -> Option<ForeignKey> {
        let mut foreign_keys = relationships.iter().map(|relationship| {
            let variant = graph.variant_id(*relationship, source, target)?;
            *self.variants.get(variant.index())?
        });
        let first = foreign_keys.next()??;
        foreign_keys
            .all(|foreign_key| foreign_key == Some(first))
            .then_some(first)
    }

    fn table_columns(&self, table: &str) -> Option<&HashSet<String>> {
        self.table(table).map(|layout| &layout.columns)
    }

    fn table_sort_key(&self, table: &str) -> Option<&[String]> {
        self.table(table).map(|layout| layout.sort_key.as_slice())
    }

    fn denormalized(&self) -> &DenormalizedCatalog {
        &self.denormalized
    }

    fn traversal_path_lookup(
        &self,
        entity: EntityId,
        kind: ontology::TraversalPathKind,
    ) -> Option<&TraversalPathLookup> {
        ClickHouseMapping::traversal_path_lookup(self, entity, kind)
    }
}
