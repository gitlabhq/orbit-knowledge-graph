use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;

use crate::v2::config::Language;
use crate::v2::types::{
    CanonicalDefinition, CanonicalDirectory, CanonicalFile, CanonicalImport, EdgeKind, NodeKind,
    Range, Relationship, containment_relationship,
};
use gkg_utils::arrow::{AsRecordBatch, BatchBuilder};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::{Bfs, EdgeFiltered};
use rustc_hash::{FxHashMap, FxHasher};
use smallvec::SmallVec;

use super::state::{GraphDef, GraphImport, GraphIndexes, StrId, StringPool};

// ── Node + Edge types ───────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum GraphNode {
    Directory(CanonicalDirectory),
    File(CanonicalFile),
    Definition { file_path: Arc<str>, id: DefId },
    Import { file_path: Arc<str>, id: ImportId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DefId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ImportId(pub u32);

impl GraphNode {
    pub fn path(&self) -> &str {
        match self {
            GraphNode::Directory(d) => &d.path,
            GraphNode::File(f) => &f.path,
            GraphNode::Definition { file_path, .. } => file_path,
            GraphNode::Import { file_path, .. } => file_path,
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

    pub fn def_id(&self) -> Option<DefId> {
        match self {
            GraphNode::Definition { id, .. } => Some(*id),
            _ => None,
        }
    }

    pub fn import_id(&self) -> Option<ImportId> {
        match self {
            GraphNode::Import { id, .. } => Some(*id),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct GraphEdge {
    pub relationship: Relationship,
}

impl GraphEdge {
    pub fn structural(edge_kind: EdgeKind, source: NodeKind, target: NodeKind) -> Self {
        Self {
            relationship: Relationship {
                edge_kind,
                source_node: source,
                target_node: target,
                source_def_kind: None,
                target_def_kind: None,
            },
        }
    }
}

// ── CodeGraph ───────────────────────────────────────────────────

/// The complete code graph. No lifetime parameter.
///
/// All definition/import strings live in the owned [`StringPool`].
/// Access strings via `self.str(id)`.
pub struct CodeGraph {
    pub graph: DiGraph<GraphNode, GraphEdge>,
    pub defs: Vec<GraphDef>,
    pub imports: Vec<GraphImport>,
    /// All strings for defs/imports. Owned, dropped with the graph.
    pub strings: StringPool,
    pub indexes: GraphIndexes,
    pub root_path: String,
}

impl CodeGraph {
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            defs: Vec::new(),
            imports: Vec::new(),
            strings: StringPool::new(),
            indexes: GraphIndexes::new(),
            root_path: String::new(),
        }
    }

    pub fn new_with_root(root_path: String) -> Self {
        Self {
            root_path,
            ..Self::new()
        }
    }

    /// Resolve a StrId to its string.
    #[inline]
    pub fn str(&self, id: StrId) -> &str {
        self.strings.get(id)
    }

    /// Add a single file's parsed defs and imports to the graph.
    pub fn add_file(
        &mut self,
        path: &str,
        extension: &str,
        language: Language,
        file_size: u64,
        definitions: &[CanonicalDefinition],
        imports: &[CanonicalImport],
    ) -> (NodeIndex, Vec<NodeIndex>, Vec<NodeIndex>) {
        let relative_path = self.relative_path(path);
        let file_path: Arc<str> = Arc::from(relative_path.as_str());

        let file_name = Path::new(&relative_path)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        let file_node = self.graph.add_node(GraphNode::File(CanonicalFile {
            path: relative_path.clone(),
            name: file_name,
            extension: extension.to_string(),
            language,
            size: file_size,
        }));
        if let Some(fi) = &mut self.indexes.file_index {
            fi.insert(relative_path.clone(), file_node);
        }

        if let Some(dir_idx) = self.ensure_directory_chain(&relative_path) {
            self.graph.add_edge(
                dir_idx,
                file_node,
                GraphEdge::structural(EdgeKind::Contains, NodeKind::Directory, NodeKind::File),
            );
        }

        // Convert canonical defs → pool-backed GraphDefs.
        let def_base = self.defs.len() as u32;
        let mut def_nodes = Vec::with_capacity(definitions.len());

        let graph_defs: Vec<GraphDef> = definitions
            .iter()
            .map(|d| GraphDef::from_canonical(d, &mut self.strings))
            .collect();

        for (i, gdef) in graph_defs.iter().enumerate() {
            let id = DefId(def_base + i as u32);
            let def_node = self.graph.add_node(GraphNode::Definition {
                file_path: file_path.clone(),
                id,
            });
            def_nodes.push(def_node);

            let fqn_str = self.strings.get(gdef.fqn);
            let name_str = self.strings.get(gdef.name);
            self.indexes.by_fqn.insert(fqn_str, def_node);
            self.indexes.by_name.insert(name_str, def_node);

            if let Some(sep_pos) = fqn_str.rfind(gdef.fqn_sep) {
                let parent = &fqn_str[..sep_pos];
                self.indexes.nested.insert(parent, name_str, def_node);
            }

            self.graph.add_edge(
                file_node,
                def_node,
                GraphEdge::structural(EdgeKind::Defines, NodeKind::File, NodeKind::Definition),
            );
        }

        // Containment edges.
        for (i, gdef) in graph_defs.iter().enumerate() {
            let fqn_str = self.strings.get(gdef.fqn);
            let Some(sep_pos) = fqn_str.rfind(gdef.fqn_sep) else {
                continue;
            };
            let parent_fqn = &fqn_str[..sep_pos];
            for (j, parent_def) in graph_defs.iter().enumerate() {
                if j != i
                    && self.strings.get(parent_def.fqn) == parent_fqn
                    && let Some(rel) = containment_relationship(parent_def.kind, gdef.kind)
                {
                    self.graph.add_edge(
                        def_nodes[j],
                        def_nodes[i],
                        GraphEdge { relationship: rel },
                    );
                    break;
                }
            }
        }

        self.defs.extend(graph_defs);

        // Convert canonical imports → pool-backed GraphImports.
        let mut import_nodes = Vec::with_capacity(imports.len());
        let import_base = self.imports.len() as u32;
        let graph_imports: Vec<GraphImport> = imports
            .iter()
            .map(|imp| GraphImport::from_canonical(imp, &mut self.strings))
            .collect();

        for (i, _) in graph_imports.iter().enumerate() {
            let id = ImportId(import_base + i as u32);
            let imp_node = self.graph.add_node(GraphNode::Import {
                file_path: file_path.clone(),
                id,
            });
            import_nodes.push(imp_node);
            self.graph.add_edge(
                file_node,
                imp_node,
                GraphEdge::structural(EdgeKind::Imports, NodeKind::File, NodeKind::ImportedSymbol),
            );
        }
        self.imports.extend(graph_imports);

        (file_node, def_nodes, import_nodes)
    }

    pub fn drop_construction_indexes(&mut self) {
        self.indexes.drop_construction_indexes();
    }

    /// Build extends edges and ancestor chains. Must be called after all
    /// files are added and before resolution.
    ///
    /// NOTE: `link_extends` resolves super types via `by_name` index which
    /// can return multiple nodes for the same name. When files are added via
    /// `par_iter`, insertion order is non-deterministic, so the `by_name`
    /// iteration order varies across runs. This causes flaky resolution when
    /// two classes share a name (e.g. `kotlin_v1_same_class_name`).
    pub fn finalize(&mut self, tracer: &crate::v2::trace::Tracer) {
        self.link_extends(tracer);
        self.build_ancestor_table(tracer);
    }

    fn build_ancestor_table(&mut self, tracer: &crate::v2::trace::Tracer) {
        let extends_only = EdgeFiltered(
            &self.graph,
            |e: petgraph::graph::EdgeReference<'_, GraphEdge>| {
                e.weight().relationship.edge_kind == EdgeKind::Extends
            },
        );

        for idx in self.graph.node_indices() {
            if !matches!(self.graph[idx], GraphNode::Definition { .. }) {
                continue;
            }
            let has_extends = self
                .graph
                .edges_directed(idx, petgraph::Direction::Outgoing)
                .any(|e| e.weight().relationship.edge_kind == EdgeKind::Extends);
            if !has_extends {
                continue;
            }

            let mut chain: SmallVec<[NodeIndex; 8]> = SmallVec::new();
            let mut bfs = Bfs::new(&extends_only, idx);
            bfs.next(&extends_only);
            while let Some(ancestor) = bfs.next(&extends_only) {
                chain.push(ancestor);
            }
            if !chain.is_empty() {
                let fqn = self.def_fqn(idx).to_string();
                let ancestor_fqns: Vec<String> =
                    chain.iter().map(|&a| self.def_fqn(a).to_string()).collect();
                tracer.event(crate::v2::trace::TraceEvent::AncestorChainBuilt {
                    fqn,
                    ancestors: ancestor_fqns,
                });
                self.indexes.ancestors.insert(idx, chain);
            }
        }

        tracer.dump("finalize (extends + ancestors)");
    }

    fn ensure_directory_chain(&mut self, file_path: &str) -> Option<NodeIndex> {
        let dir_index = self.indexes.dir_index.as_mut()?;
        let path = Path::new(file_path);
        let mut parent_dirs: Vec<String> = Vec::new();
        let mut current = path.parent();
        while let Some(dir) = current {
            parent_dirs.push(dir_to_string(dir));
            current = dir.parent();
        }
        parent_dirs.reverse();

        for dir_path in &parent_dirs {
            if !dir_index.contains_key(dir_path) {
                let name = Path::new(dir_path)
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_else(|| dir_path.clone());
                let idx = self
                    .graph
                    .add_node(GraphNode::Directory(CanonicalDirectory {
                        path: dir_path.clone(),
                        name,
                    }));
                dir_index.insert(dir_path.clone(), idx);

                if let Some(parent_dir) = Path::new(dir_path).parent() {
                    let parent_str = dir_to_string(parent_dir);
                    if let Some(&parent_idx) = dir_index.get(&parent_str) {
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
        dir_index.get(&parent_dir).copied()
    }

    pub fn relative_path(&self, file_path: &str) -> String {
        file_path
            .strip_prefix(&self.root_path)
            .map(|p| p.strip_prefix('/').unwrap_or(p))
            .unwrap_or(file_path)
            .to_string()
    }

    /// Pre-resolve all imports for a file into a name → defs map.
    pub fn pre_resolve_file_imports(
        &self,
        file_node: NodeIndex,
        sep: &str,
    ) -> FxHashMap<String, Vec<NodeIndex>> {
        let mut map = FxHashMap::default();
        for neighbor in self
            .graph
            .neighbors_directed(file_node, petgraph::Direction::Outgoing)
        {
            let Some(import_id) = self.graph[neighbor].import_id() else {
                continue;
            };
            let imp = &self.imports[import_id.0 as usize];
            if imp.wildcard {
                continue;
            }
            let name = imp.alias.or(imp.name).map(|id| self.str(id)).unwrap_or("");
            if name.is_empty() {
                continue;
            }

            let imp_path = self.str(imp.path);
            let full_fqn = if imp_path.is_empty() {
                name.to_string()
            } else {
                format!("{imp_path}{sep}{name}")
            };

            let mut defs: Vec<_> = self
                .indexes
                .by_fqn
                .lookup(&full_fqn, |idx| self.def_fqn(idx) == full_fqn)
                .to_vec();
            if defs.is_empty() && !imp_path.is_empty() {
                defs = self
                    .indexes
                    .by_fqn
                    .lookup(imp_path, |idx| self.def_fqn(idx) == imp_path)
                    .to_vec();
            }
            if !defs.is_empty() {
                map.entry(name.to_string()).or_insert(defs);
            }
        }
        map
    }

    // ── Resolution lookups ──────────────────────────────────

    pub fn lookup_nested_with_hierarchy(
        &self,
        scope_fqn: &str,
        member_name: &str,
        out: &mut Vec<NodeIndex>,
    ) -> bool {
        let start_nodes = self.resolve_scope_nodes(scope_fqn);
        if start_nodes.is_empty() {
            return false;
        }

        let verify_member = |idx: NodeIndex| self.def_name(idx) == member_name;

        for &start in &start_nodes {
            let actual_fqn = self.def_fqn(start);

            if self
                .indexes
                .nested
                .lookup_into(actual_fqn, member_name, verify_member, out)
            {
                return true;
            }

            if let Some(chain) = self.indexes.ancestors.get(&start) {
                for &ancestor in chain {
                    let ancestor_fqn = self.def_fqn(ancestor);
                    if self.indexes.nested.lookup_into(
                        ancestor_fqn,
                        member_name,
                        verify_member,
                        out,
                    ) {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Find methods whose `receiver_type` metadata matches `type_name` and
    /// whose name matches `member_name`. Used for Go methods and Kotlin
    /// extension functions where the method is not nested inside the type.
    ///
    /// Receiver types in source are often bare names (`Service`) while the
    /// lookup FQN is fully qualified (`main.Service`). We match if the
    /// receiver_type equals the FQN or its last segment after `sep`.
    pub fn lookup_by_receiver_type(
        &self,
        type_name: &str,
        member_name: &str,
        sep: &str,
        out: &mut Vec<NodeIndex>,
    ) {
        let candidates = self.indexes.by_name.lookup(member_name, |idx| {
            self.graph[idx]
                .def_id()
                .is_some_and(|d| self.str(self.defs[d.0 as usize].name) == member_name)
        });
        let bare_type = type_name.rsplit_once(sep).map_or(type_name, |(_, t)| t);

        for idx in candidates {
            if let Some(did) = self.graph[idx].def_id() {
                let gdef = &self.defs[did.0 as usize];
                if let Some(meta) = &gdef.metadata
                    && let Some(rt) = meta.receiver_type
                {
                    let rt_str = self.str(rt);
                    if rt_str == type_name || rt_str == bare_type {
                        out.push(idx);
                    }
                }
            }
        }
    }

    pub fn def_in_file(&self, def_idx: NodeIndex, file_path: &str) -> bool {
        self.graph[def_idx].path() == file_path
    }

    pub fn def(&self, idx: NodeIndex) -> &GraphDef {
        match &self.graph[idx] {
            GraphNode::Definition { id, .. } => &self.defs[id.0 as usize],
            other => panic!("Expected Definition, got {other:?}"),
        }
    }

    pub fn import(&self, idx: NodeIndex) -> &GraphImport {
        match &self.graph[idx] {
            GraphNode::Import { id, .. } => &self.imports[id.0 as usize],
            other => panic!("Expected Import, got {other:?}"),
        }
    }

    /// Returns the definition name as `&str`.
    #[inline]
    pub fn def_name(&self, idx: NodeIndex) -> &str {
        self.strings.get(self.def(idx).name)
    }

    /// Returns the FQN as `&str`.
    #[inline]
    pub fn def_fqn(&self, idx: NodeIndex) -> &str {
        self.strings.get(self.def(idx).fqn)
    }

    /// Returns the def kind.
    #[inline]
    pub fn def_kind(&self, idx: NodeIndex) -> crate::v2::types::DefKind {
        self.def(idx).kind
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

    pub fn definitions(&self) -> impl Iterator<Item = (NodeIndex, &Arc<str>, &GraphDef)> {
        self.graph.node_indices().filter_map(|idx| {
            if let GraphNode::Definition { file_path, id } = &self.graph[idx] {
                Some((idx, file_path, &self.defs[id.0 as usize]))
            } else {
                None
            }
        })
    }

    pub fn imports_iter(&self) -> impl Iterator<Item = (NodeIndex, &Arc<str>, &GraphImport)> {
        self.graph.node_indices().filter_map(|idx| {
            if let GraphNode::Import { file_path, id } = &self.graph[idx] {
                Some((idx, file_path, &self.imports[id.0 as usize]))
            } else {
                None
            }
        })
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

    fn link_extends(&mut self, tracer: &crate::v2::trace::Tracer) {
        let mut edges = Vec::new();

        for idx in self.graph.node_indices() {
            if let GraphNode::Definition { id, .. } = &self.graph[idx]
                && let Some(meta) = &self.defs[id.0 as usize].metadata
                && !meta.super_types.is_empty()
            {
                let child_fqn = self.strings.get(self.defs[id.0 as usize].fqn).to_string();
                for &super_id in &meta.super_types {
                    let super_name = self.strings.get(super_id);
                    let mut targets = self.resolve_scope_nodes(super_name);
                    targets.retain(|t| *t != idx);
                    // Sort by FQN for deterministic edge ordering.
                    targets.sort_by(|&a, &b| self.def_fqn(a).cmp(self.def_fqn(b)));
                    let resolved_fqns: Vec<String> = targets
                        .iter()
                        .filter_map(|&t| {
                            self.graph[t]
                                .def_id()
                                .map(|d| self.strings.get(self.defs[d.0 as usize].fqn).to_string())
                        })
                        .collect();
                    tracer.event(crate::v2::trace::TraceEvent::ExtendsLinked {
                        child_fqn: child_fqn.clone(),
                        super_type: super_name.to_string(),
                        resolved_to: resolved_fqns,
                    });
                    for &target in &targets {
                        edges.push((idx, target));
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

    /// Resolve a type name to graph nodes. Tries FQN index first, then
    /// name index, then qualified name resolution (split on the def's own
    /// FQN separator and resolve via nested index).
    pub fn resolve_scope_nodes(&self, name: &str) -> SmallVec<[NodeIndex; 8]> {
        let by_fqn = self
            .indexes
            .by_fqn
            .lookup(name, |idx| self.def_fqn(idx) == name);
        if !by_fqn.is_empty() {
            return by_fqn;
        }
        let by_name = self
            .indexes
            .by_name
            .lookup(name, |idx| self.def_name(idx) == name);
        if !by_name.is_empty() {
            return by_name;
        }
        // Qualified bare name (e.g. "Child.GrandChild"): resolve first
        // segment by name to find a def, use that def's fqn_sep as the
        // separator, then nested lookup for the remaining segment.
        // Try common separators to find the split point.
        for sep in &[".", "::"] {
            let segments: Vec<&str> = name.split(sep).collect();
            if segments.len() < 2 {
                continue;
            }
            let mut current = self
                .indexes
                .by_name
                .lookup(segments[0], |idx| self.def_name(idx) == segments[0]);
            if current.is_empty() {
                continue;
            }
            for &segment in &segments[1..] {
                let mut next = SmallVec::new();
                for &node in &current {
                    let fqn = self.def_fqn(node);
                    let mut found = Vec::new();
                    self.indexes.nested.lookup_into(
                        fqn,
                        segment,
                        |idx| self.def_name(idx) == segment,
                        &mut found,
                    );
                    next.extend(found);
                }
                current = next;
                if current.is_empty() {
                    break;
                }
            }
            if !current.is_empty() {
                return current;
            }
        }
        SmallVec::new()
    }

    /// Compute stable IDs for all nodes. Returns a dense Vec indexed by
    /// `NodeIndex::index()` — O(1) lookup, ~3x smaller than FxHashMap.
    pub fn assign_ids(&self, project_id: i64, branch: &str) -> Vec<i64> {
        let pid = project_id.to_string();
        let mut ids = vec![0i64; self.graph.node_count()];
        for idx in self.graph.node_indices() {
            ids[idx.index()] = match &self.graph[idx] {
                GraphNode::Directory(d) => compute_id(&[&pid, branch, "dir", &d.path]),
                GraphNode::File(f) => compute_id(&[&pid, branch, "file", &f.path]),
                GraphNode::Definition { file_path, id } => {
                    let def = &self.defs[id.0 as usize];
                    compute_id(&[&pid, branch, "def", file_path, self.strings.get(def.fqn)])
                }
                GraphNode::Import { file_path, id } => {
                    let import = &self.imports[id.0 as usize];
                    compute_id(&[
                        &pid,
                        branch,
                        "import",
                        file_path,
                        self.strings.get(import.path),
                        import.name.map(|id| self.strings.get(id)).unwrap_or("*"),
                    ])
                }
            };
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

/// Definition row for Arrow serialization. Needs the StringPool to
/// resolve StrIds.
pub struct DefinitionRow<'a> {
    pub file_path: &'a str,
    pub def: &'a GraphDef,
    pub pool: &'a StringPool,
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
        b.col("fqn")?.push_str(self.pool.get(self.def.fqn))?;
        b.col("name")?.push_str(self.pool.get(self.def.name))?;
        b.col("definition_type")?
            .push_str(self.def.definition_type)?;
        write_range(b, &self.def.range)?;
        Ok(())
    }
}

/// Import row for Arrow serialization.
pub struct ImportRow<'a> {
    pub file_path: &'a str,
    pub import: &'a GraphImport,
    pub pool: &'a StringPool,
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
        b.col("import_path")?
            .push_str(self.pool.get(self.import.path))?;
        b.col("identifier_name")?
            .push_opt_str(self.import.name.map(|id| self.pool.get(id)))?;
        b.col("identifier_alias")?
            .push_opt_str(self.import.alias.map(|id| self.pool.get(id)))?;
        write_range(b, &self.import.range)?;
        Ok(())
    }
}

pub struct EdgeRow<'a> {
    pub source_id: i64,
    pub target_id: i64,
    pub edge_kind: &'a str,
    pub source_node_kind: &'a str,
    pub target_node_kind: &'a str,
}

impl AsRecordBatch for EdgeRow<'_> {
    fn write_row(&self, b: &mut BatchBuilder, _ctx: &()) -> Result<(), arrow::error::ArrowError> {
        b.col("source_id")?.push_int(self.source_id)?;
        b.col("source_kind")?.push_str(self.source_node_kind)?;
        b.col("relationship_kind")?.push_str(self.edge_kind)?;
        b.col("target_id")?.push_int(self.target_id)?;
        b.col("target_kind")?.push_str(self.target_node_kind)?;
        Ok(())
    }
}

// ── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::v2::config::Language;
    use crate::v2::types::*;

    fn build_graph(file_path: &str, defs: Vec<CanonicalDefinition>) -> CodeGraph {
        build_graph_multi(vec![(file_path, defs)])
    }

    fn build_graph_multi(files: Vec<(&str, Vec<CanonicalDefinition>)>) -> CodeGraph {
        let mut cg = CodeGraph::new_with_root("/repo".to_string());
        for (path, defs) in &files {
            let ext = path.rsplit_once('.').map(|(_, e)| e).unwrap_or("");
            cg.add_file(path, ext, Language::Python, 100, defs, &[]);
        }
        let tracer = crate::v2::trace::Tracer::new(false);
        cg.finalize(&tracer);
        cg
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
        let cg = build_graph_multi(vec![
            ("/repo/src/main.py", vec![]),
            ("/repo/src/utils/helpers.py", vec![]),
        ]);

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
        let cg = build_graph("/repo/src/utils/helpers.py", vec![]);

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
        let cg = build_graph("/repo/src/main.py", vec![]);

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
        let cg = build_graph(
            "/repo/main.py",
            vec![make_def("Foo", &["Foo"], DefKind::Class)],
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
        let cg = build_graph(
            "/repo/main.py",
            vec![
                make_def("Foo", &["Foo"], DefKind::Class),
                make_def("bar", &["Foo", "bar"], DefKind::Method),
            ],
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
        let cg = build_graph_multi(vec![("/repo/src/a.py", vec![]), ("/repo/src/b.py", vec![])]);

        let src_count = cg.directories().filter(|(_, d)| d.path == "src").count();
        assert_eq!(src_count, 1);
    }

    #[test]
    fn resolution_indexes_populated() {
        let cg = build_graph(
            "/repo/main.py",
            vec![
                make_def("Foo", &["Foo"], DefKind::Class),
                make_def("bar", &["Foo", "bar"], DefKind::Method),
            ],
        );

        assert_eq!(
            cg.indexes
                .by_fqn
                .lookup("Foo", |idx| cg.def_fqn(idx) == "Foo")
                .len(),
            1
        );
        assert_eq!(
            cg.indexes
                .by_fqn
                .lookup("Foo.bar", |idx| cg.def_fqn(idx) == "Foo.bar")
                .len(),
            1
        );
        assert_eq!(
            cg.indexes
                .by_name
                .lookup("Foo", |idx| cg.def_name(idx) == "Foo")
                .len(),
            1
        );
        assert_eq!(
            cg.indexes
                .by_name
                .lookup("bar", |idx| cg.def_name(idx) == "bar")
                .len(),
            1
        );
        assert_eq!(
            cg.indexes
                .nested
                .lookup("Foo", "bar", |idx| cg.def_name(idx) == "bar")
                .len(),
            1
        );
    }
}
