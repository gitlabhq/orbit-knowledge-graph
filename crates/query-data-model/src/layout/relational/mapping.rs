use std::collections::{HashMap, HashSet};

use crate::{EntityId, GraphCatalog, PropertyId, Relationship, RelationshipId};

#[derive(Debug, Clone)]
pub enum PropertyRealization {
    Stored { column: String },
    Virtual(ontology::VirtualSource),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Endpoint {
    Source,
    Target,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ForeignKey {
    pub holder: Endpoint,
    pub property: PropertyId,
    pub referenced_key: PropertyId,
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
    pub(crate) graph: &'a GraphCatalog,
    pub(crate) relationship: &'a Relationship,
}

impl RelationshipRoute<'_> {
    pub fn source_entities(&self) -> impl Iterator<Item = EntityId> + '_ {
        self.relationship
            .variants
            .iter()
            .map(|variant| self.graph.variant(*variant).source)
    }

    pub fn target_entities(&self) -> impl Iterator<Item = EntityId> + '_ {
        self.relationship
            .variants
            .iter()
            .map(|variant| self.graph.variant(*variant).target)
    }

    pub fn has_source(&self, entity: EntityId) -> bool {
        self.source_entities().any(|source| source == entity)
    }

    pub fn has_target(&self, entity: EntityId) -> bool {
        self.target_entities().any(|target| target == entity)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DenormalizedDirection {
    Source,
    Target,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DenormalizedKey {
    pub property: PropertyId,
    pub direction: DenormalizedDirection,
}

#[derive(Debug, Clone)]
pub struct DenormalizedProperty {
    pub edge_column: String,
    pub tag_key: String,
    pub relationships: Vec<RelationshipId>,
}

#[derive(Debug, Clone, Default)]
pub struct DenormalizedCatalog {
    properties: HashMap<DenormalizedKey, DenormalizedProperty>,
}

impl DenormalizedCatalog {
    pub fn new(properties: HashMap<DenormalizedKey, DenormalizedProperty>) -> Self {
        Self { properties }
    }

    pub fn property(&self, key: DenormalizedKey) -> Option<&DenormalizedProperty> {
        self.properties.get(&key)
    }
}

pub trait RelationalMapping: Send + Sync {
    fn entity_table(&self, entity: EntityId) -> Option<&str>;
    fn entity_has_traversal_path(&self, entity: EntityId) -> bool;
    fn entity_is_global(&self, entity: EntityId) -> bool;
    fn default_properties(&self, entity: EntityId) -> &[PropertyId];
    fn property_column(&self, property: PropertyId) -> Option<&str> {
        match self.property_realization(property)? {
            PropertyRealization::Stored { column } => Some(column),
            PropertyRealization::Virtual(_) => None,
        }
    }
    fn property_realization(&self, property: PropertyId) -> Option<&PropertyRealization>;
    fn property_selectivity(&self, property: PropertyId) -> Option<ontology::FieldSelectivity>;
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
