use std::collections::{BTreeMap, HashMap, HashSet};

use crate::{EntityId, PropertyId, RelationshipId, RelationshipVariantId};

#[derive(Debug, Clone)]
pub struct TraversalPathLookup {
    pub table: String,
    pub property: PropertyId,
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
    declared: Vec<(RelationshipId, DenormalizedKey)>,
}

impl DenormalizedCatalog {
    pub fn new(
        properties: HashMap<DenormalizedKey, DenormalizedProperty>,
        declared: Vec<(RelationshipId, DenormalizedKey)>,
    ) -> Self {
        Self {
            properties,
            declared,
        }
    }

    pub fn property(&self, key: DenormalizedKey) -> Option<&DenormalizedProperty> {
        self.properties.get(&key)
    }

    /// Each denormalized relationship property in declaration order.
    pub fn declared(
        &self,
    ) -> impl Iterator<Item = (RelationshipId, DenormalizedKey, &DenormalizedProperty)> {
        self.declared
            .iter()
            .map(|(relationship, key)| (*relationship, *key, &self.properties[key]))
    }
}

#[derive(Debug, Clone)]
pub struct MaterializedNode {
    pub entity: EntityId,
    pub source_occurrence: usize,
    pub identity_column: String,
    pub properties: BTreeMap<PropertyId, String>,
}

#[derive(Debug, Clone)]
pub struct MaterializedRelationship {
    pub variant: RelationshipVariantId,
    pub source_slot: usize,
    pub target_slot: usize,
    pub edge_occurrence: Option<usize>,
    pub source_id_column: String,
    pub target_id_column: String,
    pub columns: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct MaterializedJoin {
    pub table: String,
    pub nodes: Vec<MaterializedNode>,
    pub relationships: Vec<MaterializedRelationship>,
    pub sources: Vec<String>,
}
