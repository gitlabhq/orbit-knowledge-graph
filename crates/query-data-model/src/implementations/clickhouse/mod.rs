pub mod layout;
pub(crate) mod mapping;

use std::collections::{BTreeSet, HashMap, HashSet};

use super::PropertyBackendFacts;
use crate::{
    DenormalizedCatalog, EntityId, ForeignKey, GraphCatalog, PathColumn, PropertyId,
    PropertyRealization, RelationalMapping, RelationshipId, TraversalPathLookup,
};
use crate::{MaterializedJoin, RelationshipVariantId, VariantRoute};
use layout::ReorderedCopy;

#[derive(Debug, Clone)]
pub struct TableLayout {
    pub name: String,
    pub columns: HashSet<String>,
    pub column_types: HashMap<String, ontology::DataType>,
    pub sort_key: Vec<String>,
    pub entity: Option<EntityId>,
    pub path_columns: Vec<PathColumn>,
}

impl TableLayout {
    pub fn replacement_identity(&self) -> &[String] {
        &self.sort_key
    }

    fn from_storage(
        table: &layout::Table,
        entity: Option<EntityId>,
        path_columns: Vec<PathColumn>,
    ) -> Self {
        Self {
            name: table.name.clone(),
            columns: table
                .columns
                .iter()
                .map(|column| column.name.trim_matches('`').to_string())
                .collect(),
            column_types: table
                .column_types
                .iter()
                .map(|(name, data_type)| (name.clone(), *data_type))
                .collect(),
            sort_key: table.sort_key.clone(),
            entity,
            path_columns,
        }
    }
}

#[derive(Debug, Clone)]
pub struct EntityLayout {
    pub table: String,
    pub has_traversal_path: bool,
    pub global: bool,
    pub high_fan_out: bool,
    pub default_properties: Vec<PropertyId>,
}

#[derive(Debug)]
pub struct ClickHouseBindings {
    default_edge_table: String,
    entities: Vec<Option<EntityLayout>>,
    relationships: Vec<Vec<String>>,
    variant_routes: Vec<Option<VariantRoute>>,
    materialized_joins: Vec<MaterializedJoin>,
    reordered_layouts: Vec<ReorderedCopy>,
    property_facts: Vec<PropertyBackendFacts>,
    tables: HashMap<String, TableLayout>,
    denormalized: DenormalizedCatalog,
    traversal_path_lookups: HashMap<(EntityId, ontology::TraversalPathKind), TraversalPathLookup>,
}

impl ClickHouseBindings {
    pub fn variant_route(&self, id: RelationshipVariantId) -> Option<&VariantRoute> {
        self.variant_routes.get(id.index())?.as_ref()
    }
    pub fn materialized_joins(&self) -> &[MaterializedJoin] {
        &self.materialized_joins
    }
    pub fn reordered_layouts(&self) -> &[ReorderedCopy] {
        &self.reordered_layouts
    }
    pub fn entity(&self, id: EntityId) -> Option<&EntityLayout> {
        self.entities.get(id.index())?.as_ref()
    }

    pub fn relationship_table(&self, id: RelationshipId) -> Option<&str> {
        match self.relationships.get(id.index())?.as_slice() {
            [table] => Some(table),
            _ => None,
        }
    }

    pub fn property_column(&self, id: PropertyId) -> Option<&str> {
        RelationalMapping::property_column(self, id)
    }

    pub fn table(&self, name: &str) -> Option<&TableLayout> {
        self.tables.get(name)
    }

    pub fn tables(&self) -> impl Iterator<Item = &TableLayout> {
        self.tables.values()
    }

    pub fn edge_tables(&self) -> impl Iterator<Item = &TableLayout> {
        self.relationships
            .iter()
            .flatten()
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

impl RelationalMapping for ClickHouseBindings {
    fn supports_selective_latest_rows(&self) -> bool {
        true
    }

    fn variant_route(&self, variant: RelationshipVariantId) -> Option<&VariantRoute> {
        self.variant_route(variant)
    }
    fn equivalent_layouts(&self, table: &str) -> Vec<&str> {
        self.reordered_layouts
            .iter()
            .filter(|copy| copy.source == table)
            .map(|copy| copy.table.as_str())
            .collect()
    }
    fn materialized_joins(&self) -> &[MaterializedJoin] {
        &self.materialized_joins
    }
    fn entity_table(&self, entity: EntityId) -> Option<&str> {
        self.entity(entity).map(|layout| layout.table.as_str())
    }

    fn table_entity(&self, table: &str) -> Option<EntityId> {
        self.table(table)?.entity
    }

    fn entity_has_traversal_path(&self, entity: EntityId) -> bool {
        self.entity(entity)
            .is_some_and(|layout| layout.has_traversal_path)
    }

    fn entity_is_global(&self, entity: EntityId) -> bool {
        self.entity(entity).is_some_and(|layout| layout.global)
    }

    fn entity_high_fan_out(&self, entity: EntityId) -> bool {
        self.entity(entity)
            .is_some_and(|layout| layout.high_fan_out)
    }

    fn property_is_foreign_key(&self, property: PropertyId) -> bool {
        self.property_facts
            .get(property.index())
            .is_some_and(|facts| facts.foreign_key)
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

    fn null_as_epoch(&self, property: PropertyId) -> bool {
        self.property_facts
            .get(property.index())
            .is_some_and(|facts| facts.null_as_epoch)
    }

    fn property_unique(&self, property: PropertyId) -> bool {
        self.property_facts
            .get(property.index())
            .is_some_and(|facts| facts.unique)
    }

    fn property_mutable(&self, property: PropertyId) -> bool {
        self.property_facts
            .get(property.index())
            .is_none_or(|facts| facts.mutable)
    }

    fn table_path_columns(&self, table: &str) -> Option<&[PathColumn]> {
        self.table(table)
            .map(|layout| layout.path_columns.as_slice())
    }

    fn default_edge_table(&self) -> &str {
        ClickHouseBindings::default_edge_table(self)
    }

    fn relationship_table(&self, relationship: RelationshipId) -> Option<&str> {
        ClickHouseBindings::relationship_table(self, relationship)
    }

    fn edge_tables(&self, relationships: &[RelationshipId]) -> Vec<String> {
        if relationships.is_empty() {
            return self.edge_tables().map(|table| table.name.clone()).collect();
        }
        relationships
            .iter()
            .filter_map(|relationship| self.relationships.get(relationship.index()))
            .flatten()
            .cloned()
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
            self.variant_route(variant)?.foreign_key
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
        ClickHouseBindings::traversal_path_lookup(self, entity, kind)
    }
}
