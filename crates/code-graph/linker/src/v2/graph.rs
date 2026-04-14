use std::hash::{Hash, Hasher};
use std::sync::Arc;

use code_graph_types::{
    CanonicalDefinition, CanonicalDirectory, CanonicalFile, CanonicalImport, Range, Relationship,
};
use gkg_utils::arrow::{AsRecordBatch, BatchBuilder};
use petgraph::graph::{DiGraph, NodeIndex};
use rustc_hash::{FxHashMap, FxHasher};

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

// ── Arrow serialization ─────────────────────────────────────────

/// Context for `AsRecordBatch` implementations on v2 graph types.
pub struct RowContext<'a> {
    pub project_id: i64,
    pub branch: &'a str,
    pub commit_sha: &'a str,
}

impl<'a> RowContext<'a> {
    pub fn empty() -> Self {
        Self {
            project_id: 0,
            branch: "",
            commit_sha: "",
        }
    }
}

fn compute_id(components: &[&str]) -> i64 {
    let mut hasher = FxHasher::default();
    components.hash(&mut hasher);
    hasher.finish() as i64
}

/// Directory node with assigned ID for Arrow serialization.
pub struct DirectoryRow<'a> {
    pub dir: &'a CanonicalDirectory,
    pub id: i64,
}

impl AsRecordBatch<RowContext<'_>> for DirectoryRow<'_> {
    fn write_row(
        &self,
        b: &mut BatchBuilder,
        ctx: &RowContext<'_>,
    ) -> Result<(), arrow::error::ArrowError> {
        b.col("id")?.push_int(self.id)?;
        b.col("project_id")?.push_int(ctx.project_id)?;
        b.col("branch")?.push_str(ctx.branch)?;
        b.col("commit_sha")?.push_str(ctx.commit_sha)?;
        b.col("path")?.push_str(&self.dir.path)?;
        b.col("name")?.push_str(&self.dir.name)?;
        Ok(())
    }
}

/// File node with assigned ID for Arrow serialization.
pub struct FileRow<'a> {
    pub file: &'a CanonicalFile,
    pub id: i64,
}

impl AsRecordBatch<RowContext<'_>> for FileRow<'_> {
    fn write_row(
        &self,
        b: &mut BatchBuilder,
        ctx: &RowContext<'_>,
    ) -> Result<(), arrow::error::ArrowError> {
        b.col("id")?.push_int(self.id)?;
        b.col("project_id")?.push_int(ctx.project_id)?;
        b.col("branch")?.push_str(ctx.branch)?;
        b.col("commit_sha")?.push_str(ctx.commit_sha)?;
        b.col("path")?.push_str(&self.file.path)?;
        b.col("name")?.push_str(&self.file.name)?;
        b.col("extension")?.push_str(&self.file.extension)?;
        b.col("language")?.push_str(self.file.language.names()[0])?;
        Ok(())
    }
}

/// Definition node with assigned ID for Arrow serialization.
pub struct DefinitionRow<'a> {
    pub file_path: &'a str,
    pub def: &'a CanonicalDefinition,
    pub id: i64,
}

impl AsRecordBatch<RowContext<'_>> for DefinitionRow<'_> {
    fn write_row(
        &self,
        b: &mut BatchBuilder,
        ctx: &RowContext<'_>,
    ) -> Result<(), arrow::error::ArrowError> {
        b.col("id")?.push_int(self.id)?;
        b.col("project_id")?.push_int(ctx.project_id)?;
        b.col("branch")?.push_str(ctx.branch)?;
        b.col("commit_sha")?.push_str(ctx.commit_sha)?;
        b.col("file_path")?.push_str(self.file_path)?;
        b.col("fqn")?.push_str(self.def.fqn.to_string())?;
        b.col("name")?.push_str(&self.def.name)?;
        b.col("definition_type")?
            .push_str(self.def.definition_type)?;
        b.col("start_line")?
            .push_int(self.def.range.start.line as i64)?;
        b.col("end_line")?
            .push_int(self.def.range.end.line as i64)?;
        b.col("start_byte")?
            .push_int(self.def.range.byte_offset.0 as i64)?;
        b.col("end_byte")?
            .push_int(self.def.range.byte_offset.1 as i64)?;
        Ok(())
    }
}

/// Import node with assigned ID for Arrow serialization.
pub struct ImportRow<'a> {
    pub file_path: &'a str,
    pub import: &'a CanonicalImport,
    pub id: i64,
}

impl AsRecordBatch<RowContext<'_>> for ImportRow<'_> {
    fn write_row(
        &self,
        b: &mut BatchBuilder,
        ctx: &RowContext<'_>,
    ) -> Result<(), arrow::error::ArrowError> {
        b.col("id")?.push_int(self.id)?;
        b.col("project_id")?.push_int(ctx.project_id)?;
        b.col("branch")?.push_str(ctx.branch)?;
        b.col("commit_sha")?.push_str(ctx.commit_sha)?;
        b.col("file_path")?.push_str(self.file_path)?;
        b.col("import_type")?.push_str(self.import.import_type)?;
        b.col("import_path")?.push_str(&self.import.path)?;
        b.col("identifier_name")?
            .push_opt_str(self.import.name.as_deref())?;
        b.col("identifier_alias")?
            .push_opt_str(self.import.alias.as_deref())?;
        b.col("start_line")?
            .push_int(self.import.range.start.line as i64)?;
        b.col("end_line")?
            .push_int(self.import.range.end.line as i64)?;
        b.col("start_byte")?
            .push_int(self.import.range.byte_offset.0 as i64)?;
        b.col("end_byte")?
            .push_int(self.import.range.byte_offset.1 as i64)?;
        Ok(())
    }
}

/// Edge row for Arrow serialization.
pub struct EdgeRow {
    pub source_id: i64,
    pub target_id: i64,
    pub edge_kind: String,
    pub source_node_kind: String,
    pub target_node_kind: String,
}

impl AsRecordBatch for EdgeRow {
    fn write_row(&self, b: &mut BatchBuilder, _ctx: &()) -> Result<(), arrow::error::ArrowError> {
        b.col("source_id")?.push_int(self.source_id)?;
        b.col("source_kind")?.push_str(&self.source_node_kind)?;
        b.col("relationship_kind")?.push_str(&self.edge_kind)?;
        b.col("target_id")?.push_int(self.target_id)?;
        b.col("target_kind")?.push_str(&self.target_node_kind)?;
        Ok(())
    }
}

impl CodeGraph {
    /// Assign stable IDs to all nodes for Arrow serialization.
    /// Returns a map from NodeIndex → i64 ID.
    pub fn assign_ids(&self, project_id: i64, branch: &str) -> FxHashMap<NodeIndex, i64> {
        let mut ids = FxHashMap::default();
        for idx in self.graph.node_indices() {
            let node = &self.graph[idx];
            let id = match node {
                GraphNode::Directory(d) => {
                    compute_id(&[&project_id.to_string(), branch, "dir", &d.path])
                }
                GraphNode::File(f) => {
                    compute_id(&[&project_id.to_string(), branch, "file", &f.path])
                }
                GraphNode::Definition { file_path, def } => compute_id(&[
                    &project_id.to_string(),
                    branch,
                    "def",
                    file_path,
                    &def.fqn.to_string(),
                ]),
                GraphNode::Import { file_path, import } => compute_id(&[
                    &project_id.to_string(),
                    branch,
                    "import",
                    file_path,
                    &import.path,
                    import.name.as_deref().unwrap_or("*"),
                ]),
            };
            ids.insert(idx, id);
        }
        ids
    }
}
