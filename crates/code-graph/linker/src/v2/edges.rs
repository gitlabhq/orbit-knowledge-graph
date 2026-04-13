use code_graph_types::{Range, Relationship};

use super::context::DefRef;

/// A resolved edge produced by reference resolution.
///
/// References source and target definitions by index (file_idx + def_idx)
/// rather than by path strings. The pipeline maps these to petgraph
/// NodeIndex values when adding to the CodeGraph.
#[derive(Debug, Clone)]
pub struct ResolvedEdge {
    pub relationship: Relationship,
    pub source: DefRef,
    pub target: DefRef,
    /// Range of the reference site (call expression).
    pub reference_range: Range,
}
