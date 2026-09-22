//! What the planner needs from the ontology about relationships: which
//! physical edge table a kind lives in, and whether its edges carry the
//! containing namespace's traversal path.

use ontology::Ontology;
use std::collections::HashMap;

// ── Join graph ──────────────────────────────────────────────────────────────

pub struct JoinGraph {
    by_kind: HashMap<String, JoinPath>,
}

#[derive(Clone)]
pub struct JoinPath {
    pub scope_preserving: bool,
    pub edge_table: String,
}

impl JoinGraph {
    pub fn build(ontology: &Ontology) -> Self {
        let mut by_kind = HashMap::new();
        for edge in ontology.edges() {
            by_kind
                .entry(edge.relationship_kind.clone())
                .or_insert(JoinPath {
                    scope_preserving: edge.scope.is_some_and(|s| s.is_scope_preserving()),
                    edge_table: edge.destination_table.clone(),
                });
        }
        Self { by_kind }
    }

    pub fn edge_table(&self, rel_types: &[String], default: &str) -> String {
        rel_types
            .iter()
            .find_map(|t| self.by_kind.get(t).map(|jp| jp.edge_table.clone()))
            .unwrap_or_else(|| default.to_string())
    }

    /// Every kind's edge carries the containing namespace's traversal path,
    /// so a scope prefix on the edge implies containment.
    pub fn scope_preserving(&self, rel_types: &[String]) -> bool {
        !rel_types.is_empty()
            && rel_types
                .iter()
                .all(|t| self.by_kind.get(t).is_some_and(|jp| jp.scope_preserving))
    }
}
