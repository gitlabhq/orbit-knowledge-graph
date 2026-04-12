use code_graph_types::{Range, Relationship};
use std::sync::Arc;

/// A resolved edge in the code graph.
///
/// Combines the relationship metadata (edge kind, source/target node kinds,
/// optional def kinds) with the concrete source and target locations.
#[derive(Debug, Clone)]
pub struct Edge {
    pub relationship: Relationship,
    pub source_path: Arc<str>,
    pub target_path: Arc<str>,
    pub source_range: Range,
    pub target_range: Range,
    /// For call edges: the range of the enclosing definition at the call site.
    pub source_definition_range: Option<Range>,
    /// For call edges: the range of the target definition.
    pub target_definition_range: Option<Range>,
}
