use std::sync::Arc;

use code_graph_types::{
    CanonicalDefinition, CanonicalDirectory, CanonicalFile, CanonicalImport, Range, Relationship,
};
use petgraph::graph::{DiGraph, NodeIndex};
use rustc_hash::FxHashMap;

/// A node in the code graph.
#[derive(Debug, Clone)]
pub enum GraphNode {
    Directory(CanonicalDirectory),
    File(CanonicalFile),
    Definition {
        file_path: Arc<str>,
        def: CanonicalDefinition,
    },
    Import {
        file_path: Arc<str>,
        import: CanonicalImport,
    },
}

impl GraphNode {
    pub fn path(&self) -> &str {
        match self {
            GraphNode::Directory(d) => &d.path,
            GraphNode::File(f) => &f.path,
            GraphNode::Definition { file_path, .. } => file_path,
            GraphNode::Import { file_path, .. } => file_path,
        }
    }

    pub fn name(&self) -> &str {
        match self {
            GraphNode::Directory(d) => &d.name,
            GraphNode::File(f) => &f.name,
            GraphNode::Definition { def, .. } => &def.name,
            GraphNode::Import { import, .. } => import.name.as_deref().unwrap_or("*"),
        }
    }

    pub fn range(&self) -> Range {
        match self {
            GraphNode::Directory(_) | GraphNode::File(_) => Range::empty(),
            GraphNode::Definition { def, .. } => def.range,
            GraphNode::Import { import, .. } => import.range,
        }
    }

    pub fn as_directory(&self) -> Option<&CanonicalDirectory> {
        match self {
            GraphNode::Directory(d) => Some(d),
            _ => None,
        }
    }

    pub fn as_file(&self) -> Option<&CanonicalFile> {
        match self {
            GraphNode::File(f) => Some(f),
            _ => None,
        }
    }

    pub fn as_definition(&self) -> Option<(&Arc<str>, &CanonicalDefinition)> {
        match self {
            GraphNode::Definition { file_path, def } => Some((file_path, def)),
            _ => None,
        }
    }

    pub fn as_import(&self) -> Option<(&Arc<str>, &CanonicalImport)> {
        match self {
            GraphNode::Import { file_path, import } => Some((file_path, import)),
            _ => None,
        }
    }
}

/// An edge in the code graph.
#[derive(Debug, Clone)]
pub struct GraphEdge {
    pub relationship: Relationship,
    /// For call edges: the range of the enclosing definition at the call site.
    pub source_definition_range: Option<Range>,
    /// For call edges: the range of the target definition.
    pub target_definition_range: Option<Range>,
}

/// The complete code graph — petgraph-backed directed graph.
pub struct CodeGraph {
    pub graph: DiGraph<GraphNode, GraphEdge>,
    /// Quick lookup: path → NodeIndex for directories.
    pub dir_index: FxHashMap<String, NodeIndex>,
    /// Quick lookup: path → NodeIndex for files.
    pub file_index: FxHashMap<String, NodeIndex>,
    /// Quick lookup: (file_idx, def_idx) → NodeIndex for definitions.
    pub def_index: FxHashMap<(usize, usize), NodeIndex>,
}

impl CodeGraph {
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            dir_index: FxHashMap::default(),
            file_index: FxHashMap::default(),
            def_index: FxHashMap::default(),
        }
    }

    // ── Convenience iterators ───────────────────────────────────

    pub fn directories(&self) -> impl Iterator<Item = (NodeIndex, &CanonicalDirectory)> {
        self.graph
            .node_indices()
            .filter_map(|idx| self.graph[idx].as_directory().map(|d| (idx, d)))
    }

    pub fn files(&self) -> impl Iterator<Item = (NodeIndex, &CanonicalFile)> {
        self.graph
            .node_indices()
            .filter_map(|idx| self.graph[idx].as_file().map(|f| (idx, f)))
    }

    pub fn definitions(
        &self,
    ) -> impl Iterator<Item = (NodeIndex, &Arc<str>, &CanonicalDefinition)> {
        self.graph
            .node_indices()
            .filter_map(|idx| self.graph[idx].as_definition().map(|(p, d)| (idx, p, d)))
    }

    pub fn imports(&self) -> impl Iterator<Item = (NodeIndex, &Arc<str>, &CanonicalImport)> {
        self.graph
            .node_indices()
            .filter_map(|idx| self.graph[idx].as_import().map(|(p, i)| (idx, p, i)))
    }

    pub fn edges(&self) -> impl Iterator<Item = (&GraphNode, &GraphNode, &GraphEdge)> {
        self.graph.edge_indices().map(|idx| {
            let (src, tgt) = self.graph.edge_endpoints(idx).unwrap();
            (&self.graph[src], &self.graph[tgt], &self.graph[idx])
        })
    }

    // ── Counts ──────────────────────────────────────────────────

    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }

    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }
}

impl Default for CodeGraph {
    fn default() -> Self {
        Self::new()
    }
}
