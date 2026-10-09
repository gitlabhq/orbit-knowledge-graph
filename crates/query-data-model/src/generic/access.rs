use std::collections::{BTreeMap, BTreeSet};

use super::{EntityId, PropertyId, RelationshipVariantId};

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
    pub sources: BTreeSet<String>,
}
