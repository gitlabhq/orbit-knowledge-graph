pub mod aggregation;
pub mod hydration;
pub mod neighbors;
pub mod pathfinding;
pub mod traversal;

use std::collections::BTreeSet;

use query_data_model::OrbitQueryModel;

pub struct EdgeTableConfig {
    pub tables: Vec<String>,
    pub rel_type_filter: Option<Vec<String>>,
    /// Entity kinds the relationship types allow at each end; empty for a wildcard.
    pub source_kinds: Vec<String>,
    pub target_kinds: Vec<String>,
    /// Tables a neighbors arm scans per direction; `neighbors::plan` narrows them so empty arms are skipped.
    pub outgoing_tables: Vec<String>,
    pub incoming_tables: Vec<String>,
}

impl EdgeTableConfig {
    pub fn from_model(model: &(impl OrbitQueryModel + ?Sized), rel_types: &[String]) -> Self {
        let mut source_kinds = BTreeSet::new();
        let mut target_kinds = BTreeSet::new();
        for rt in rel_types {
            if let Some(route) = model.relationship_route(rt) {
                source_kinds.extend(
                    route
                        .source_entities()
                        .map(|entity| model.graph().entity(entity).name.clone()),
                );
                target_kinds.extend(
                    route
                        .target_entities()
                        .map(|entity| model.graph().entity(entity).name.clone()),
                );
            }
        }
        let tables = model.relationship_tables(rel_types);
        Self {
            rel_type_filter: if rel_types.is_empty() {
                None
            } else {
                Some(rel_types.to_vec())
            },
            source_kinds: source_kinds.into_iter().collect(),
            target_kinds: target_kinds.into_iter().collect(),
            outgoing_tables: tables.clone(),
            incoming_tables: tables.clone(),
            tables,
        }
    }
}
