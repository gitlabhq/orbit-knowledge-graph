use super::MaterializedJoin;
use crate::RelationshipVariantId;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct VariantRoute {
    pub table: String,
    pub foreign_key: Option<ForeignKey>,
}

use crate::{EntityId, GraphCatalog, PropertyId, RelationshipId, RelationshipVariant};

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
    pub(crate) variants: Vec<&'a RelationshipVariant>,
}

impl RelationshipRoute<'_> {
    pub fn variants(&self) -> impl Iterator<Item = &crate::RelationshipVariant> + '_ {
        self.variants.iter().copied()
    }

    pub fn source_entities(&self) -> impl Iterator<Item = EntityId> + '_ {
        self.variants.iter().map(|variant| variant.source)
    }

    pub fn target_entities(&self) -> impl Iterator<Item = EntityId> + '_ {
        self.variants.iter().map(|variant| variant.target)
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
    pub variants: HashSet<(RelationshipId, EntityId, EntityId)>,
    pub nullable: bool,
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
    fn supports_selective_latest_rows(&self) -> bool {
        false
    }

    fn supports_deferred_node_loading(&self) -> bool {
        true
    }
    fn variant_route(&self, variant: RelationshipVariantId) -> Option<&VariantRoute>;
    fn equivalent_layouts(&self, _table: &str) -> Vec<&str> {
        Vec::new()
    }
    fn materialized_joins(&self) -> &[MaterializedJoin] {
        &[]
    }
    fn entity_table(&self, entity: EntityId) -> Option<&str>;
    fn table_entity(&self, table: &str) -> Option<EntityId>;
    fn entity_has_traversal_path(&self, entity: EntityId) -> bool;
    fn entity_is_global(&self, entity: EntityId) -> bool;
    fn entity_high_fan_out(&self, entity: EntityId) -> bool;
    fn property_is_foreign_key(&self, property: PropertyId) -> bool;
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
    fn null_as_epoch(&self, property: PropertyId) -> bool;
    fn property_unique(&self, property: PropertyId) -> bool;
    fn property_mutable(&self, property: PropertyId) -> bool;
    fn table_path_columns(&self, table: &str) -> Option<&[PathColumn]>;
    fn table_owner(&self, _table: &str) -> Option<&TableOwner> {
        None
    }
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableOwner {
    pub column: String,
    pub admin_bypass: bool,
}
