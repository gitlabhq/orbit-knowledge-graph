use code_graph_types::{Range, Relationship};

use super::context::DefRef;

/// Source of a resolved edge — either a definition or a file (for module-level calls).
#[derive(Debug, Clone, Copy)]
pub enum EdgeSource {
    /// Call from within a definition (method, function, etc.)
    Definition(DefRef),
    /// Call at module/file level (no enclosing definition)
    File(usize),
}

impl EdgeSource {
    pub fn file_idx(&self) -> usize {
        match self {
            EdgeSource::Definition(d) => d.file_idx,
            EdgeSource::File(f) => *f,
        }
    }
}

/// A resolved edge produced by reference resolution.
#[derive(Debug, Clone)]
pub struct ResolvedEdge {
    pub relationship: Relationship,
    pub source: EdgeSource,
    pub target: DefRef,
    pub reference_range: Range,
}
