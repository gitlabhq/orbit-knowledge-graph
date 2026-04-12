use code_graph_types::{
    CanonicalDefinition, CanonicalDirectory, CanonicalFile, CanonicalImport, CanonicalResult,
};

use super::edges::Edge;

/// The complete output of the graph builder — all nodes and edges,
/// ready for serialization.
pub struct GraphData {
    pub directories: Vec<CanonicalDirectory>,
    pub files: Vec<CanonicalFile>,
    pub definitions: Vec<(String, CanonicalDefinition)>,
    pub imports: Vec<(String, CanonicalImport)>,
    pub edges: Vec<Edge>,
}

/// Builds a language-agnostic code graph from `CanonicalResult` entries.
///
/// Call `add_result` for each parsed file, then `build()` to produce
/// the final `GraphData` with all containment, definition, import,
/// and reference edges resolved.
pub struct GraphBuilder {
    results: Vec<CanonicalResult>,
}

impl GraphBuilder {
    pub fn new() -> Self {
        Self {
            results: Vec::new(),
        }
    }

    pub fn add_result(&mut self, result: CanonicalResult) {
        self.results.push(result);
    }

    pub fn build(self) -> GraphData {
        todo!()
    }
}
