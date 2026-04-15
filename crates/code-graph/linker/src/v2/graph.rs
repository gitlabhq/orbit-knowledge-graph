use std::hash::{Hash, Hasher};
use std::sync::Arc;

use code_graph_types::{
    CanonicalDefinition, CanonicalDirectory, CanonicalFile, CanonicalImport, CanonicalResult,
    EdgeKind, NodeKind, Range, Relationship, containment_relationship,
};
use gkg_utils::arrow::{AsRecordBatch, BatchBuilder};
use petgraph::graph::{DiGraph, NodeIndex};
use rustc_hash::{FxHashMap, FxHashSet, FxHasher};
use std::path::Path;

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

// ── GraphBuilder ────────────────────────────────────────────────

/// Builds a language-agnostic code graph from `CanonicalResult` entries.
pub struct GraphBuilder {
    results: Vec<CanonicalResult>,
    root_path: String,
}

impl GraphBuilder {
    pub fn new(root_path: String) -> Self {
        Self {
            results: Vec::new(),
            root_path,
        }
    }

    pub fn add_result(&mut self, result: CanonicalResult) {
        self.results.push(result);
    }

    pub fn build(self) -> CodeGraph {
        let mut cg = CodeGraph::new();
        let mut seen_dir_edges: FxHashSet<(String, String)> = FxHashSet::default();

        for result in &self.results {
            let relative_path = self.relative_path(&result.file_path);
            let file_path: Arc<str> = Arc::from(relative_path.as_str());

            let file_name = Path::new(&relative_path)
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();

            let file_node = cg.graph.add_node(GraphNode::File(CanonicalFile {
                path: relative_path.clone(),
                name: file_name,
                extension: result.extension.clone(),
                language: result.language,
                size: result.file_size,
            }));
            cg.file_index.insert(relative_path.clone(), file_node);

            let dir_idx = self.build_directory_chain(&relative_path, &mut cg, &mut seen_dir_edges);

            if let Some(parent_idx) = dir_idx {
                cg.graph.add_edge(
                    parent_idx,
                    file_node,
                    GraphEdge {
                        relationship: Relationship {
                            edge_kind: EdgeKind::Contains,
                            source_node: NodeKind::Directory,
                            target_node: NodeKind::File,
                            source_def_kind: None,
                            target_def_kind: None,
                        },
                        source_definition_range: None,
                        target_definition_range: None,
                    },
                );
            }

            let file_idx = self
                .results
                .iter()
                .position(|r| std::ptr::eq(r, result))
                .unwrap();
            let mut def_indices = Vec::new();
            for (di, def) in result.definitions.iter().enumerate() {
                let def_node_idx = cg.graph.add_node(GraphNode::Definition {
                    file_path: file_path.clone(),
                    def: def.clone(),
                });
                def_indices.push(def_node_idx);
                cg.def_index.insert((file_idx, di), def_node_idx);

                cg.graph.add_edge(
                    file_node,
                    def_node_idx,
                    GraphEdge {
                        relationship: Relationship {
                            edge_kind: EdgeKind::Defines,
                            source_node: NodeKind::File,
                            target_node: NodeKind::Definition,
                            source_def_kind: None,
                            target_def_kind: None,
                        },
                        source_definition_range: None,
                        target_definition_range: None,
                    },
                );
            }

            self.build_containment_edges(&result.definitions, &def_indices, &mut cg);

            for imp in &result.imports {
                let imp_idx = cg.graph.add_node(GraphNode::Import {
                    file_path: file_path.clone(),
                    import: imp.clone(),
                });

                cg.graph.add_edge(
                    file_node,
                    imp_idx,
                    GraphEdge {
                        relationship: Relationship {
                            edge_kind: EdgeKind::Imports,
                            source_node: NodeKind::File,
                            target_node: NodeKind::ImportedSymbol,
                            source_def_kind: None,
                            target_def_kind: None,
                        },
                        source_definition_range: None,
                        target_definition_range: None,
                    },
                );
            }
        }

        cg
    }

    fn relative_path(&self, file_path: &str) -> String {
        file_path
            .strip_prefix(&self.root_path)
            .map(|p| p.strip_prefix('/').unwrap_or(p))
            .unwrap_or(file_path)
            .to_string()
    }

    fn build_directory_chain(
        &self,
        file_path: &str,
        cg: &mut CodeGraph,
        seen_dir_edges: &mut FxHashSet<(String, String)>,
    ) -> Option<NodeIndex> {
        let path = Path::new(file_path);
        let mut ancestors: Vec<String> = Vec::new();

        let mut current = path.parent();
        while let Some(dir) = current {
            let dir_str = if dir.as_os_str().is_empty() {
                ".".to_string()
            } else {
                dir.to_string_lossy().to_string()
            };
            ancestors.push(dir_str);
            current = dir.parent();
        }

        ancestors.reverse();

        for dir_path in &ancestors {
            if !cg.dir_index.contains_key(dir_path) {
                let name = Path::new(dir_path)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| dir_path.clone());

                let idx = cg.graph.add_node(GraphNode::Directory(CanonicalDirectory {
                    path: dir_path.clone(),
                    name,
                }));
                cg.dir_index.insert(dir_path.clone(), idx);
            }
        }

        for pair in ancestors.windows(2) {
            let key = (pair[0].clone(), pair[1].clone());
            if seen_dir_edges.insert(key)
                && let (Some(&src), Some(&tgt)) =
                    (cg.dir_index.get(&pair[0]), cg.dir_index.get(&pair[1]))
            {
                cg.graph.add_edge(
                    src,
                    tgt,
                    GraphEdge {
                        relationship: Relationship {
                            edge_kind: EdgeKind::Contains,
                            source_node: NodeKind::Directory,
                            target_node: NodeKind::Directory,
                            source_def_kind: None,
                            target_def_kind: None,
                        },
                        source_definition_range: None,
                        target_definition_range: None,
                    },
                );
            }
        }

        let parent_dir = path.parent().map(|p| {
            if p.as_os_str().is_empty() {
                ".".to_string()
            } else {
                p.to_string_lossy().to_string()
            }
        })?;
        cg.dir_index.get(&parent_dir).copied()
    }

    fn build_containment_edges(
        &self,
        definitions: &[CanonicalDefinition],
        def_indices: &[NodeIndex],
        cg: &mut CodeGraph,
    ) {
        let fqn_to_idx: FxHashMap<code_graph_types::IStr, usize> = definitions
            .iter()
            .enumerate()
            .map(|(i, d)| (d.fqn.as_istr(), i))
            .collect();

        for (i, def) in definitions.iter().enumerate() {
            let Some(parent_fqn) = def.fqn.parent() else {
                continue;
            };

            if let Some(&parent_idx) = fqn_to_idx.get(&parent_fqn.as_istr())
                && parent_idx != i
                && let Some(rel) = containment_relationship(definitions[parent_idx].kind, def.kind)
            {
                cg.graph.add_edge(
                    def_indices[parent_idx],
                    def_indices[i],
                    GraphEdge {
                        relationship: rel,
                        source_definition_range: None,
                        target_definition_range: None,
                    },
                );
            }
        }
    }
}

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
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result("/repo/src/main.py", vec![]));
        builder.add_result(make_result("/repo/src/utils/helpers.py", vec![]));

        let cg = builder.build();

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
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result("/repo/src/utils/helpers.py", vec![]));

        let cg = builder.build();

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
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result("/repo/src/main.py", vec![]));

        let cg = builder.build();

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
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result(
            "/repo/main.py",
            vec![make_def("Foo", &["Foo"], DefKind::Class)],
        ));

        let cg = builder.build();

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
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result(
            "/repo/main.py",
            vec![
                make_def("Foo", &["Foo"], DefKind::Class),
                make_def("bar", &["Foo", "bar"], DefKind::Method),
            ],
        ));

        let cg = builder.build();

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
        let mut builder = GraphBuilder::new("/repo".to_string());
        builder.add_result(make_result("/repo/src/a.py", vec![]));
        builder.add_result(make_result("/repo/src/b.py", vec![]));

        let cg = builder.build();

        let src_count = cg.directories().filter(|(_, d)| d.path == "src").count();
        assert_eq!(src_count, 1);
    }
}
