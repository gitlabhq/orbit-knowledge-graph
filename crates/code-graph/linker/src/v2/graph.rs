use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;

use code_graph_types::{
    CanonicalDefinition, CanonicalDirectory, CanonicalFile, CanonicalImport, CanonicalResult,
    EdgeKind, NodeKind, Range, Relationship, containment_relationship,
};
use gkg_utils::arrow::{AsRecordBatch, BatchBuilder};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::{Bfs, EdgeFiltered};
use rustc_hash::{FxHashMap, FxHasher};

// ── Node + Edge types ───────────────────────────────────────────

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
    pub source_definition_range: Option<Range>,
    pub target_definition_range: Option<Range>,
}

impl GraphEdge {
    /// Structural edge (containment, defines, imports) — no range info.
    pub fn structural(edge_kind: EdgeKind, source: NodeKind, target: NodeKind) -> Self {
        Self {
            relationship: Relationship {
                edge_kind,
                source_node: source,
                target_node: target,
                source_def_kind: None,
                target_def_kind: None,
            },
            source_definition_range: None,
            target_definition_range: None,
        }
    }
}

// ── CodeGraph ───────────────────────────────────────────────────

/// The complete code graph — petgraph-backed directed graph with
/// resolution indexes for the walker and resolver.
pub struct CodeGraph {
    pub graph: DiGraph<GraphNode, GraphEdge>,

    // Structural indexes
    pub dir_index: FxHashMap<String, NodeIndex>,
    pub file_index: FxHashMap<String, NodeIndex>,

    // Resolution indexes
    pub def_by_fqn: FxHashMap<String, Vec<NodeIndex>>,
    pub def_by_name: FxHashMap<String, Vec<NodeIndex>>,
    pub members: FxHashMap<String, FxHashMap<String, Vec<NodeIndex>>>,

    pub root_path: String,
}

impl CodeGraph {
    pub fn new_with_root(root_path: String) -> Self {
        Self {
            root_path,
            ..Self::new()
        }
    }

    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            dir_index: FxHashMap::default(),
            file_index: FxHashMap::default(),

            def_by_fqn: FxHashMap::default(),
            def_by_name: FxHashMap::default(),
            members: FxHashMap::default(),
            root_path: String::new(),
        }
    }

    /// Add a single file's nodes to the graph. Returns (file_node, def_nodes, import_nodes)
    /// so the walker can write `Value::Def(NodeIndex)` immediately.
    ///
    /// Called under a Mutex during the parallel parse+walk phase.
    /// Does NOT add directory nodes or flatten supers — call `finalize()` after.
    pub fn add_file_nodes(
        &mut self,
        result: &CanonicalResult,
        _file_order: usize,
    ) -> (NodeIndex, Vec<NodeIndex>, Vec<NodeIndex>) {
        let relative_path = self.relative_path(&result.file_path);
        let file_path: Arc<str> = Arc::from(relative_path.as_str());

        let file_name = Path::new(&relative_path)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        let file_node = self.graph.add_node(GraphNode::File(CanonicalFile {
            path: relative_path.clone(),
            name: file_name,
            extension: result.extension.clone(),
            language: result.language,
            size: result.file_size,
        }));
        self.file_index.insert(relative_path.clone(), file_node);

        // Build directory chain and dir→file edge inline.
        if let Some(dir_idx) = self.ensure_directory_chain(&relative_path) {
            self.graph.add_edge(
                dir_idx,
                file_node,
                GraphEdge::structural(EdgeKind::Contains, NodeKind::Directory, NodeKind::File),
            );
        }

        let mut def_nodes = Vec::with_capacity(result.definitions.len());
        for def in result.definitions.iter() {
            let def_node = self.graph.add_node(GraphNode::Definition {
                file_path: file_path.clone(),
                def: def.clone(),
            });
            def_nodes.push(def_node);

            let fqn_str = def.fqn.to_string();
            self.def_by_fqn
                .entry(fqn_str.clone())
                .or_default()
                .push(def_node);
            self.def_by_name
                .entry(def.name.clone())
                .or_default()
                .push(def_node);

            if let Some(parent_fqn) = def.fqn.parent() {
                self.members
                    .entry(parent_fqn.to_string())
                    .or_default()
                    .entry(def.name.clone())
                    .or_default()
                    .push(def_node);
            }

            self.graph.add_edge(
                file_node,
                def_node,
                GraphEdge::structural(EdgeKind::Defines, NodeKind::File, NodeKind::Definition),
            );
        }

        let mut import_nodes = Vec::with_capacity(result.imports.len());
        for imp in &result.imports {
            let imp_node = self.graph.add_node(GraphNode::Import {
                file_path: file_path.clone(),
                import: imp.clone(),
            });
            import_nodes.push(imp_node);
            self.graph.add_edge(
                file_node,
                imp_node,
                GraphEdge::structural(EdgeKind::Imports, NodeKind::File, NodeKind::ImportedSymbol),
            );
        }
        (file_node, def_nodes, import_nodes)
    }

    /// Finalize the graph after all files have been added.
    /// Builds containment edges between definitions and links supertypes.
    /// Directory chains are already built during add_file_nodes().
    pub fn finalize(&mut self) {
        let file_nodes: Vec<NodeIndex> = self.file_index.values().copied().collect();
        for file_node in file_nodes {
            build_containment_edges(file_node, self);
        }
        self.link_supers();
    }

    /// Create directory nodes and dir→dir edges for a file path.
    /// Returns the immediate parent directory's NodeIndex.
    /// Only creates edges when a directory is first seen, so no dedup set needed.
    fn ensure_directory_chain(&mut self, file_path: &str) -> Option<NodeIndex> {
        let path = Path::new(file_path);
        let mut ancestors: Vec<String> = Vec::new();
        let mut current = path.parent();
        while let Some(dir) = current {
            ancestors.push(dir_to_string(dir));
            current = dir.parent();
        }
        ancestors.reverse();

        for dir_path in &ancestors {
            if !self.dir_index.contains_key(dir_path) {
                let name = Path::new(dir_path)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| dir_path.clone());
                let idx = self.graph.add_node(GraphNode::Directory(CanonicalDirectory {
                    path: dir_path.clone(),
                    name,
                }));
                self.dir_index.insert(dir_path.clone(), idx);

                // Parent was created/exists from a previous iteration — add edge.
                if let Some(parent_dir) = Path::new(dir_path).parent() {
                    let parent_str = dir_to_string(parent_dir);
                    if let Some(&parent_idx) = self.dir_index.get(&parent_str) {
                        self.graph.add_edge(
                            parent_idx,
                            idx,
                            GraphEdge::structural(
                                EdgeKind::Contains,
                                NodeKind::Directory,
                                NodeKind::Directory,
                            ),
                        );
                    }
                }
            }
        }

        let parent_dir = dir_to_string(path.parent()?);
        self.dir_index.get(&parent_dir).copied()
    }

    /// Build the complete graph from parsed results in a single pass.
    /// Convenience: build complete graph from results in one call.
    /// Used by tests and custom pipelines. The main pipeline uses
    /// `add_file_nodes()` + `finalize()` instead.
    pub fn from_results(results: Vec<CanonicalResult>, root_path: String) -> Self {
        let mut cg = Self::new_with_root(root_path);
        for (i, result) in results.iter().enumerate() {
            cg.add_file_nodes(result, i);
        }
        cg.finalize();
        cg
    }

    pub fn relative_path(&self, file_path: &str) -> String {
        file_path
            .strip_prefix(&self.root_path)
            .map(|p| p.strip_prefix('/').unwrap_or(p))
            .unwrap_or(file_path)
            .to_string()
    }

    // ── Resolution lookups ──────────────────────────────────

    pub fn lookup_fqn(&self, fqn: &str) -> &[NodeIndex] {
        self.def_by_fqn
            .get(fqn)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn lookup_name(&self, name: &str) -> &[NodeIndex] {
        self.def_by_name
            .get(name)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn lookup_member(&self, class_fqn: &str, member_name: &str) -> &[NodeIndex] {
        self.members
            .get(class_fqn)
            .and_then(|ms| ms.get(member_name))
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    pub fn lookup_member_with_supers(
        &self,
        class_fqn: &str,
        member_name: &str,
        out: &mut Vec<NodeIndex>,
    ) -> bool {
        // Resolve class_fqn to NodeIndex(es): try FQN first, then bare name
        let start_nodes = self
            .def_by_fqn
            .get(class_fqn)
            .or_else(|| self.def_by_name.get(class_fqn));

        let Some(start_nodes) = start_nodes else {
            return false;
        };

        let extends_only = EdgeFiltered(&self.graph, |e: petgraph::graph::EdgeReference<'_, GraphEdge>| {
            e.weight().relationship.edge_kind == EdgeKind::Extends
        });

        for &start in start_nodes {
            let mut bfs = Bfs::new(&extends_only, start);
            while let Some(node) = bfs.next(&extends_only) {
                let fqn = self.def_fqn(node);
                let found = self.lookup_member(&fqn, member_name);
                if !found.is_empty() {
                    out.extend_from_slice(found);
                    return true;
                }
            }
        }
        false
    }

    /// Does this definition node belong to the given file?
    pub fn def_in_file(&self, def_idx: NodeIndex, file_path: &str) -> bool {
        self.graph[def_idx].path() == file_path
    }

    pub fn def(&self, idx: NodeIndex) -> &CanonicalDefinition {
        match &self.graph[idx] {
            GraphNode::Definition { def, .. } => def,
            other => panic!("Expected Definition, got {other:?}"),
        }
    }

    pub fn import(&self, idx: NodeIndex) -> &CanonicalImport {
        match &self.graph[idx] {
            GraphNode::Import { import, .. } => import,
            other => panic!("Expected Import, got {other:?}"),
        }
    }

    pub fn def_fqn(&self, idx: NodeIndex) -> String {
        self.def(idx).fqn.to_string()
    }

    // ── Iterators ───────────────────────────────────────────

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

    pub fn node_count(&self) -> usize {
        self.graph.node_count()
    }
    pub fn edge_count(&self) -> usize {
        self.graph.edge_count()
    }

    // ── Internal ────────────────────────────────────────────

    /// Add Extends edges from each class/interface to its supertypes.
    /// Resolves super_type names (which may be bare names or FQNs)
    /// to NodeIndexes in the graph.
    fn link_supers(&mut self) {
        let mut edges = Vec::new();

        for idx in self.graph.node_indices() {
            if let GraphNode::Definition { def, .. } = &self.graph[idx]
                && let Some(meta) = &def.metadata
                && !meta.super_types.is_empty()
            {
                for super_name in &meta.super_types {
                    let targets = self.resolve_type_to_nodes(super_name);
                    for &target in targets {
                        if target != idx {
                            edges.push((idx, target));
                        }
                    }
                }
            }
        }

        for (child, parent) in edges {
            self.graph.add_edge(
                child,
                parent,
                GraphEdge::structural(
                    EdgeKind::Extends,
                    NodeKind::Definition,
                    NodeKind::Definition,
                ),
            );
        }
    }

    /// Resolve a type name (FQN or bare name) to graph NodeIndexes.
    fn resolve_type_to_nodes(&self, name: &str) -> &[NodeIndex] {
        if let Some(nodes) = self.def_by_fqn.get(name) {
            if !nodes.is_empty() {
                return nodes;
            }
        }
        self.def_by_name
            .get(name)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }

    /// Assign stable IDs to all nodes for Arrow serialization.
    pub fn assign_ids(&self, project_id: i64, branch: &str) -> FxHashMap<NodeIndex, i64> {
        let pid = project_id.to_string();
        let mut ids = FxHashMap::default();
        for idx in self.graph.node_indices() {
            let id = match &self.graph[idx] {
                GraphNode::Directory(d) => compute_id(&[&pid, branch, "dir", &d.path]),
                GraphNode::File(f) => compute_id(&[&pid, branch, "file", &f.path]),
                GraphNode::Definition { file_path, def } => {
                    compute_id(&[&pid, branch, "def", file_path, &def.fqn.to_string()])
                }
                GraphNode::Import { file_path, import } => compute_id(&[
                    &pid,
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

impl Default for CodeGraph {
    fn default() -> Self {
        Self::new()
    }
}

// ── Graph construction helpers ──────────────────────────────────

fn dir_to_string(dir: &Path) -> String {
    if dir.as_os_str().is_empty() {
        ".".to_string()
    } else {
        dir.to_string_lossy().to_string()
    }
}


fn build_containment_edges(file_node: NodeIndex, cg: &mut CodeGraph) {
    // Collect def nodes for this file from graph neighbors
    let defs: Vec<(NodeIndex, code_graph_types::IStr, code_graph_types::DefKind)> = cg
        .graph
        .neighbors_directed(file_node, petgraph::Direction::Outgoing)
        .filter_map(|idx| {
            let (_, def) = cg.graph[idx].as_definition()?;
            Some((idx, def.fqn.as_istr(), def.kind))
        })
        .collect();

    let fqn_to_pos: FxHashMap<code_graph_types::IStr, usize> = defs
        .iter()
        .enumerate()
        .map(|(i, (_, fqn, _))| (*fqn, i))
        .collect();

    let mut edges = Vec::new();
    for (i, (idx, _, kind)) in defs.iter().enumerate() {
        let def = cg.def(*idx);
        let Some(parent_fqn) = def.fqn.parent() else {
            continue;
        };
        if let Some(&parent_pos) = fqn_to_pos.get(&parent_fqn.as_istr())
            && parent_pos != i
            && let Some(rel) = containment_relationship(defs[parent_pos].2, *kind)
        {
            edges.push((defs[parent_pos].0, *idx, rel));
        }
    }

    for (src, tgt, rel) in edges {
        cg.graph.add_edge(
            src,
            tgt,
            GraphEdge {
                relationship: rel,
                source_definition_range: None,
                target_definition_range: None,
            },
        );
    }
}

fn compute_id(components: &[&str]) -> i64 {
    let mut hasher = FxHasher::default();
    components.hash(&mut hasher);
    hasher.finish() as i64
}

// ── Arrow serialization ─────────────────────────────────────────

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

fn write_node_header(
    b: &mut BatchBuilder,
    id: i64,
    ctx: &RowContext<'_>,
) -> Result<(), arrow::error::ArrowError> {
    b.col("id")?.push_int(id)?;
    b.col("project_id")?.push_int(ctx.project_id)?;
    b.col("branch")?.push_str(ctx.branch)?;
    b.col("commit_sha")?.push_str(ctx.commit_sha)?;
    Ok(())
}

fn write_range(b: &mut BatchBuilder, range: &Range) -> Result<(), arrow::error::ArrowError> {
    b.col("start_line")?.push_int(range.start.line as i64)?;
    b.col("end_line")?.push_int(range.end.line as i64)?;
    b.col("start_byte")?.push_int(range.byte_offset.0 as i64)?;
    b.col("end_byte")?.push_int(range.byte_offset.1 as i64)?;
    Ok(())
}

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
        write_node_header(b, self.id, ctx)?;
        b.col("path")?.push_str(&self.dir.path)?;
        b.col("name")?.push_str(&self.dir.name)?;
        Ok(())
    }
}

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
        write_node_header(b, self.id, ctx)?;
        b.col("path")?.push_str(&self.file.path)?;
        b.col("name")?.push_str(&self.file.name)?;
        b.col("extension")?.push_str(&self.file.extension)?;
        b.col("language")?.push_str(self.file.language.names()[0])?;
        Ok(())
    }
}

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
        write_node_header(b, self.id, ctx)?;
        b.col("file_path")?.push_str(self.file_path)?;
        b.col("fqn")?.push_str(self.def.fqn.to_string())?;
        b.col("name")?.push_str(&self.def.name)?;
        b.col("definition_type")?
            .push_str(self.def.definition_type)?;
        write_range(b, &self.def.range)?;
        Ok(())
    }
}

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
        write_node_header(b, self.id, ctx)?;
        b.col("file_path")?.push_str(self.file_path)?;
        b.col("import_type")?.push_str(self.import.import_type)?;
        b.col("import_path")?.push_str(&self.import.path)?;
        b.col("identifier_name")?
            .push_opt_str(self.import.name.as_deref())?;
        b.col("identifier_alias")?
            .push_opt_str(self.import.alias.as_deref())?;
        write_range(b, &self.import.range)?;
        Ok(())
    }
}

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

// ── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use code_graph_config::Language;
    use code_graph_types::*;

    fn make_result(file_path: &str, defs: Vec<CanonicalDefinition>) -> CanonicalResult {
        CanonicalResult {
            file_path: file_path.to_string(),
            extension: "py".to_string(),
            file_size: 100,
            language: Language::Python,
            definitions: defs,
            imports: vec![],
            references: vec![],
            bindings: vec![],
            control_flow: vec![],
        }
    }

    fn make_def(name: &str, fqn_parts: &[&str], kind: DefKind) -> CanonicalDefinition {
        CanonicalDefinition {
            definition_type: "Class",
            kind,
            name: name.to_string(),
            fqn: Fqn::from_parts(fqn_parts, "."),
            range: Range::new(Position::new(0, 0), Position::new(10, 0), (0, 100)),
            is_top_level: fqn_parts.len() == 1,
            metadata: None,
        }
    }

    #[test]
    fn builds_file_and_directory_nodes() {
        let cg = CodeGraph::from_results(
            vec![
                make_result("/repo/src/main.py", vec![]),
                make_result("/repo/src/utils/helpers.py", vec![]),
            ],
            "/repo".to_string(),
        );

        let files: Vec<_> = cg.files().map(|(_, f)| &f.path).collect();
        assert_eq!(files.len(), 2);
        assert!(files.contains(&&"src/main.py".to_string()));
        assert!(files.contains(&&"src/utils/helpers.py".to_string()));

        let dir_paths: Vec<_> = cg.directories().map(|(_, d)| d.path.as_str()).collect();
        assert!(dir_paths.contains(&"."));
        assert!(dir_paths.contains(&"src"));
        assert!(dir_paths.contains(&"src/utils"));
    }

    #[test]
    fn builds_directory_containment_edges() {
        let cg = CodeGraph::from_results(
            vec![make_result("/repo/src/utils/helpers.py", vec![])],
            "/repo".to_string(),
        );

        let dir_dir: Vec<_> = cg
            .edges()
            .filter(|(_s, _t, e)| {
                e.relationship.source_node == NodeKind::Directory
                    && e.relationship.target_node == NodeKind::Directory
            })
            .collect();
        assert!(!dir_dir.is_empty());
    }

    #[test]
    fn builds_dir_to_file_edges() {
        let cg = CodeGraph::from_results(
            vec![make_result("/repo/src/main.py", vec![])],
            "/repo".to_string(),
        );

        let dir_file: Vec<_> = cg
            .edges()
            .filter(|(_, _, e)| {
                e.relationship.source_node == NodeKind::Directory
                    && e.relationship.target_node == NodeKind::File
            })
            .collect();
        assert_eq!(dir_file.len(), 1);
        assert_eq!(dir_file[0].0.path(), "src");
        assert_eq!(dir_file[0].1.path(), "src/main.py");
    }

    #[test]
    fn builds_file_to_definition_edges() {
        let cg = CodeGraph::from_results(
            vec![make_result(
                "/repo/main.py",
                vec![make_def("Foo", &["Foo"], DefKind::Class)],
            )],
            "/repo".to_string(),
        );

        let file_def: Vec<_> = cg
            .edges()
            .filter(|(_, _, e)| {
                e.relationship.source_node == NodeKind::File
                    && e.relationship.target_node == NodeKind::Definition
            })
            .collect();
        assert_eq!(file_def.len(), 1);
    }

    #[test]
    fn builds_definition_containment_edges() {
        let cg = CodeGraph::from_results(
            vec![make_result(
                "/repo/main.py",
                vec![
                    make_def("Foo", &["Foo"], DefKind::Class),
                    make_def("bar", &["Foo", "bar"], DefKind::Method),
                ],
            )],
            "/repo".to_string(),
        );

        let def_def: Vec<_> = cg
            .edges()
            .filter(|(_, _, e)| {
                e.relationship.source_node == NodeKind::Definition
                    && e.relationship.target_node == NodeKind::Definition
            })
            .collect();
        assert_eq!(def_def.len(), 1);
        assert_eq!(def_def[0].2.relationship.edge_kind, EdgeKind::Defines);
        assert_eq!(
            def_def[0].2.relationship.source_def_kind,
            Some(DefKind::Class)
        );
        assert_eq!(
            def_def[0].2.relationship.target_def_kind,
            Some(DefKind::Method)
        );
    }

    #[test]
    fn no_duplicate_directories() {
        let cg = CodeGraph::from_results(
            vec![
                make_result("/repo/src/a.py", vec![]),
                make_result("/repo/src/b.py", vec![]),
            ],
            "/repo".to_string(),
        );

        let src_count = cg.directories().filter(|(_, d)| d.path == "src").count();
        assert_eq!(src_count, 1);
    }

    #[test]
    fn resolution_indexes_populated() {
        let cg = CodeGraph::from_results(
            vec![make_result(
                "/repo/main.py",
                vec![
                    make_def("Foo", &["Foo"], DefKind::Class),
                    make_def("bar", &["Foo", "bar"], DefKind::Method),
                ],
            )],
            "/repo".to_string(),
        );

        assert_eq!(cg.lookup_fqn("Foo").len(), 1);
        assert_eq!(cg.lookup_fqn("Foo.bar").len(), 1);
        assert_eq!(cg.lookup_name("Foo").len(), 1);
        assert_eq!(cg.lookup_name("bar").len(), 1);
        assert_eq!(cg.lookup_member("Foo", "bar").len(), 1);
    }
}
