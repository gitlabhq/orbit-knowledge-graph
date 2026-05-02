use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;

use crate::trace;
use crate::v2::config::Language;
use crate::v2::types::{
    CanonicalDefinition, CanonicalDirectory, CanonicalFile, CanonicalImport, EdgeKind, NodeKind,
    Range, Relationship, containment_relationship,
};
use gkg_utils::arrow::{AsRecordBatch, BatchBuilder, ColumnSpec, ColumnType};

fn common_prefix_len(a: &str, b: &str) -> usize {
    a.bytes().zip(b.bytes()).take_while(|(x, y)| x == y).count()
}
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::visit::{Bfs, EdgeFiltered};
use rustc_hash::{FxHashMap, FxHasher};
use smallvec::SmallVec;

use super::state::{DefinitionRangeIndex, GraphDef, GraphImport, GraphIndexes, StrId, StringPool};

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphOutput {
    Complete,
    ParsedOnly,
}

impl GraphOutput {
    pub fn writes_repository_structure(self) -> bool {
        matches!(self, Self::Complete)
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
    pub output: GraphOutput,
    /// Language-specific resolution rules (spec, separator, hooks, settings).
    /// Set once at construction via `with_rules()`.
    pub rules: Option<std::sync::Arc<super::rules::ResolutionRules>>,
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
            output: GraphOutput::Complete,
            rules: None,
        }
    }

    pub fn new_with_root(root_path: String) -> Self {
        Self {
            root_path,
            ..Self::new()
        }
    }

    pub fn with_rules(mut self, rules: std::sync::Arc<super::rules::ResolutionRules>) -> Self {
        self.rules = Some(rules);
        self
    }

    pub fn mark_parsed_only(&mut self) {
        self.output = GraphOutput::ParsedOnly;
    }

    /// FQN separator for this language. Falls back to `"."`.
    #[inline]
    pub fn sep(&self) -> &str {
        self.rules.as_ref().map(|r| r.fqn_separator).unwrap_or(".")
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
        self.add_file_with_language(
            path,
            extension,
            Some(language),
            file_size,
            definitions,
            imports,
        )
    }

    /// Add a file that should be represented in the graph but is not parsed by
    /// any language pipeline.
    pub fn add_unparsed_file(
        &mut self,
        path: &str,
        language: Option<Language>,
        file_size: u64,
    ) -> NodeIndex {
        let extension = Path::new(path)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or_default()
            .to_string();
        let (file_node, _, _) =
            self.add_file_with_language(path, &extension, language, file_size, &[], &[]);
        file_node
    }

    fn add_file_with_language(
        &mut self,
        path: &str,
        extension: &str,
        language: Option<Language>,
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
        let mut definition_ranges = Vec::with_capacity(definitions.len());

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
            definition_ranges.push((gdef.range, def_node));

            self.graph.add_edge(
                file_node,
                def_node,
                GraphEdge::structural(EdgeKind::Defines, NodeKind::File, NodeKind::Definition),
            );
        }

        self.indexes.definition_ranges.insert(
            relative_path.clone(),
            DefinitionRangeIndex::from_ranges(definition_ranges),
        );

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
        // Stabilize index order so lookups are deterministic regardless
        // of par_iter file processing order. Sort by FQN since NodeIndex
        // assignment is also insertion-order-dependent.
        let defs = &self.defs;
        let strings = &self.strings;
        let graph = &self.graph;
        let fqn_of = |idx: NodeIndex| -> String {
            match &graph[idx] {
                GraphNode::Definition { id, .. } => {
                    strings.get(defs[id.0 as usize].fqn).to_string()
                }
                _ => String::new(),
            }
        };
        self.indexes.by_name.sort_all(fqn_of);
        self.indexes.by_fqn.sort_all(fqn_of);
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
                trace!(
                    tracer,
                    AncestorChainBuilt {
                        fqn: fqn,
                        ancestors: ancestor_fqns,
                    }
                );
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
    ) -> FxHashMap<String, Vec<NodeIndex>> {
        let sep = self.sep();
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
        out: &mut Vec<NodeIndex>,
    ) {
        let candidates = self.indexes.by_name.lookup(member_name, |idx| {
            self.graph[idx]
                .def_id()
                .is_some_and(|d| self.str(self.defs[d.0 as usize].name) == member_name)
        });
        let bare_type = type_name
            .rsplit_once(self.sep())
            .map_or(type_name, |(_, t)| t);

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

    pub fn lookup_nested_from_node_with_hierarchy(
        &self,
        scope_node: NodeIndex,
        member_name: &str,
        out: &mut Vec<NodeIndex>,
    ) -> bool {
        let scope_fqn = self.def_fqn(scope_node);
        let verify_member = |idx: NodeIndex| self.def_name(idx) == member_name;

        if self
            .indexes
            .nested
            .lookup_into(scope_fqn, member_name, verify_member, out)
        {
            return true;
        }

        if let Some(chain) = self.indexes.ancestors.get(&scope_node) {
            for &ancestor in chain {
                let ancestor_fqn = self.def_fqn(ancestor);
                if self
                    .indexes
                    .nested
                    .lookup_into(ancestor_fqn, member_name, verify_member, out)
                {
                    return true;
                }
            }
        }

        false
    }

    pub fn def_in_file(&self, def_idx: NodeIndex, file_path: &str) -> bool {
        self.graph[def_idx].path() == file_path
    }

    pub fn file_node_for_path(&self, file_path: &str) -> Option<NodeIndex> {
        self.indexes.file_index.as_ref()?.get(file_path).copied()
    }

    pub fn enclosing_definition_for_range(
        &self,
        file_path: &str,
        start: u32,
        end: u32,
    ) -> Option<NodeIndex> {
        self.indexes
            .definition_ranges
            .get(file_path)?
            .find_enclosing(start, end)
    }

    pub fn definition_for_range(&self, file_path: &str, start: u32, end: u32) -> Option<NodeIndex> {
        self.indexes
            .definition_ranges
            .get(file_path)?
            .find_enclosing_or_overlapping(start, end)
    }

    pub fn add_call_edge(&mut self, source_node: NodeIndex, target_node: NodeIndex) {
        let (source_node_kind, source_def_kind) = self.graph[source_node]
            .def_id()
            .map(|id| (NodeKind::Definition, Some(self.defs[id.0 as usize].kind)))
            .unwrap_or((NodeKind::File, None));
        let target_def_kind = self.graph[target_node]
            .def_id()
            .map(|id| self.defs[id.0 as usize].kind);

        self.graph.add_edge(
            source_node,
            target_node,
            GraphEdge {
                relationship: Relationship {
                    edge_kind: EdgeKind::Calls,
                    source_node: source_node_kind,
                    target_node: NodeKind::Definition,
                    source_def_kind,
                    target_def_kind,
                },
            },
        );
    }

    pub fn try_def(&self, idx: NodeIndex) -> Option<&GraphDef> {
        match &self.graph[idx] {
            GraphNode::Definition { id, .. } => Some(&self.defs[id.0 as usize]),
            _ => None,
        }
    }

    pub fn def(&self, idx: NodeIndex) -> &GraphDef {
        self.try_def(idx).unwrap_or_else(|| {
            std::panic::panic_any(crate::v2::error::CodeGraphError::UnexpectedNodeType {
                expected: "Definition",
                got: format!("{:?} at {idx:?}", self.graph[idx]),
            })
        })
    }

    pub fn try_import(&self, idx: NodeIndex) -> Option<&GraphImport> {
        match &self.graph[idx] {
            GraphNode::Import { id, .. } => Some(&self.imports[id.0 as usize]),
            _ => None,
        }
    }

    pub fn import(&self, idx: NodeIndex) -> &GraphImport {
        self.try_import(idx).unwrap_or_else(|| {
            std::panic::panic_any(crate::v2::error::CodeGraphError::UnexpectedNodeType {
                expected: "Import",
                got: format!("{:?} at {idx:?}", self.graph[idx]),
            })
        })
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

    /// Returns the ancestor chain for a node, if any.
    #[inline]
    pub fn ancestors(&self, idx: NodeIndex) -> Option<&[NodeIndex]> {
        self.indexes.ancestors.get(&idx).map(|v| v.as_slice())
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
                    // Disambiguate: when multiple candidates share a name,
                    // prefer the one closest to the child's scope (longest
                    // common FQN prefix) that isn't nested under the child.
                    if targets.len() > 1 {
                        let child_prefix = format!("{}.", child_fqn);
                        targets.sort_by(|&a, &b| {
                            let a_fqn = self.def_fqn(a);
                            let b_fqn = self.def_fqn(b);
                            let a_nested = a_fqn.starts_with(&child_prefix);
                            let b_nested = b_fqn.starts_with(&child_prefix);
                            // Non-nested before nested
                            a_nested.cmp(&b_nested).then_with(|| {
                                // Among non-nested: longest common prefix with
                                // child_fqn wins (closer scope)
                                let a_common = common_prefix_len(a_fqn, &child_fqn);
                                let b_common = common_prefix_len(b_fqn, &child_fqn);
                                b_common.cmp(&a_common)
                            })
                        });
                        targets.truncate(1);
                    }
                    let resolved_fqns: Vec<String> = targets
                        .iter()
                        .filter_map(|&t| {
                            self.graph[t]
                                .def_id()
                                .map(|d| self.strings.get(self.defs[d.0 as usize].fqn).to_string())
                        })
                        .collect();
                    trace!(
                        tracer,
                        ExtendsLinked {
                            child_fqn: child_fqn.clone(),
                            super_type: super_name.to_string(),
                            resolved_to: resolved_fqns,
                        }
                    );
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
        let by_name = self.indexes.by_name.lookup(name, |idx| {
            self.def_name(idx) == name
                && self.graph[idx]
                    .def_id()
                    .is_some_and(|d| self.defs[d.0 as usize].kind.is_type_container())
        });
        if !by_name.is_empty() {
            return by_name;
        }
        // Qualified bare name (e.g. "Parent.Child"): resolve the first
        // segment by name to find candidate prefixes, then check by_fqn
        // for the full qualified name under each prefix. O(first_matches)
        // instead of O(matches^segments).
        for sep in &[".", "::"] {
            let segments: Vec<&str> = name.split(sep).collect();
            if segments.len() < 2 {
                continue;
            }
            let first_matches = self.indexes.by_name.lookup(segments[0], |idx| {
                self.def_name(idx) == segments[0]
                    && self.graph[idx]
                        .def_id()
                        .is_some_and(|d| self.defs[d.0 as usize].kind.is_type_container())
            });
            if first_matches.is_empty() {
                continue;
            }
            let rest = &segments[1..].join(sep);
            for &node in &first_matches {
                let prefix_fqn = self.def_fqn(node);
                let candidate = format!("{prefix_fqn}{sep}{rest}");
                let matches = self
                    .indexes
                    .by_fqn
                    .lookup(&candidate, |idx| self.def_fqn(idx) == candidate);
                if !matches.is_empty() {
                    return matches;
                }
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
                    let range = format!("{}:{}", def.range.byte_offset.0, def.range.byte_offset.1);
                    compute_id(&[
                        &pid,
                        branch,
                        "def",
                        file_path,
                        self.strings.get(def.fqn),
                        &range,
                    ])
                }
                GraphNode::Import { file_path, id } => {
                    let import = &self.imports[id.0 as usize];
                    let range = format!(
                        "{}:{}",
                        import.range.byte_offset.0, import.range.byte_offset.1
                    );
                    compute_id(&[
                        &pid,
                        branch,
                        "import",
                        file_path,
                        self.strings.get(import.path),
                        import.name.map(|id| self.strings.get(id)).unwrap_or("*"),
                        &range,
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
    // Mask clears the sign bit so the result is always a positive i64.
    (hasher.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
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

impl gkg_utils::arrow::RowEnvelope for RowContext<'_> {
    fn write_header(&self, b: &mut BatchBuilder, id: i64) -> Result<(), arrow::error::ArrowError> {
        b.col("id")?.push_int(id)?;
        b.col("project_id")?.push_int(self.project_id)?;
        b.col("branch")?.push_str(self.branch)?;
        b.col("commit_sha")?.push_str(self.commit_sha)?;
        Ok(())
    }

    fn header_specs(&self) -> Vec<ColumnSpec> {
        vec![
            ColumnSpec {
                name: "id".into(),
                col_type: ColumnType::Int,
                nullable: false,
            },
            ColumnSpec {
                name: "project_id".into(),
                col_type: ColumnType::Int,
                nullable: false,
            },
            ColumnSpec {
                name: "branch".into(),
                col_type: ColumnType::Str,
                nullable: false,
            },
            ColumnSpec {
                name: "commit_sha".into(),
                col_type: ColumnType::Str,
                nullable: false,
            },
        ]
    }
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
impl<C: gkg_utils::arrow::RowEnvelope> AsRecordBatch<C> for DirectoryRow<'_> {
    fn write_row(&self, b: &mut BatchBuilder, ctx: &C) -> Result<(), arrow::error::ArrowError> {
        ctx.write_header(b, self.id)?;
        b.col("path")?.push_str(&self.dir.path)?;
        b.col("name")?.push_str(&self.dir.name)?;
        Ok(())
    }
}

pub struct FileRow<'a> {
    pub file: &'a CanonicalFile,
    pub id: i64,
}
impl<C: gkg_utils::arrow::RowEnvelope> AsRecordBatch<C> for FileRow<'_> {
    fn write_row(&self, b: &mut BatchBuilder, ctx: &C) -> Result<(), arrow::error::ArrowError> {
        ctx.write_header(b, self.id)?;
        b.col("path")?.push_str(&self.file.path)?;
        b.col("name")?.push_str(&self.file.name)?;
        b.col("extension")?.push_str(&self.file.extension)?;
        b.col("language")?.push_str(self.file.language_name())?;
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
impl<C: gkg_utils::arrow::RowEnvelope> AsRecordBatch<C> for DefinitionRow<'_> {
    fn write_row(&self, b: &mut BatchBuilder, ctx: &C) -> Result<(), arrow::error::ArrowError> {
        ctx.write_header(b, self.id)?;
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
impl<C: gkg_utils::arrow::RowEnvelope> AsRecordBatch<C> for ImportRow<'_> {
    fn write_row(&self, b: &mut BatchBuilder, ctx: &C) -> Result<(), arrow::error::ArrowError> {
        ctx.write_header(b, self.id)?;
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

    fn make_def_at(
        name: &str,
        fqn_parts: &[&str],
        start: usize,
        end: usize,
    ) -> CanonicalDefinition {
        CanonicalDefinition {
            definition_type: "Method",
            kind: DefKind::Function,
            name: name.to_string(),
            fqn: Fqn::from_parts(fqn_parts, "."),
            range: Range::new(Position::new(0, 0), Position::new(10, 0), (start, end)),
            is_top_level: false,
            metadata: None,
        }
    }

    #[test]
    fn assign_ids_distinguishes_definitions_sharing_fqn_in_same_file() {
        // Regression: v2 Definition ids previously hashed only
        // (project_id, branch, file_path, fqn). Two methods that share
        // an fqn (e.g. Go methods on different receivers, or generated
        // protobuf methods with the same synthesized fqn) collapsed to
        // one id and dedup'd on insert.
        let cg = build_graph(
            "/repo/main.go",
            vec![
                make_def_at("Dup", &["main", "Dup"], 100, 120),
                make_def_at("Dup", &["main", "Dup"], 200, 220),
            ],
        );

        let ids = cg.assign_ids(42, "main");
        let def_ids: Vec<i64> = cg
            .definitions()
            .map(|(idx, _, _)| ids[idx.index()])
            .collect();

        assert_eq!(def_ids.len(), 2);
        assert_ne!(def_ids[0], def_ids[1]);
    }

    #[test]
    fn compute_id_is_always_non_negative() {
        // Inputs hand-picked because their unmasked FxHash output has
        // the high bit set, which used to produce negative i64 ids.
        let cases: &[&[&str]] = &[
            &[
                "1",
                "main",
                "def",
                "src/lib.rs",
                "lower_traversal_edge_only",
                "100:120",
            ],
            &[
                "42",
                "feature/x",
                "def",
                "internal/foo.go",
                "main.Dup",
                "200:220",
            ],
            &["7", "main", "def", "a.py", "pkg.A.method", "0:5"],
            &["999", "release/1", "branch", "main", "", "0:0"],
        ];
        for components in cases {
            let id = compute_id(components);
            assert!(id >= 0, "compute_id({components:?}) returned {id}");
        }
    }

    #[test]
    fn assign_ids_distinguishes_imports_sharing_path_in_same_file() {
        // Regression: v2 Import ids previously hashed only
        // (project_id, branch, file_path, import_path, name|"*"),
        // so repeated `use foo::bar` style imports in one file
        // (common in tonic-generated Rust) collapsed to one id.
        let import_a = CanonicalImport {
            import_type: "Use",
            binding_kind: ImportBindingKind::Namespace,
            mode: ImportMode::Declarative,
            path: "tonic::codegen".to_string(),
            name: None,
            alias: None,
            scope_fqn: None,
            range: Range::new(Position::new(0, 0), Position::new(0, 20), (100, 120)),
            is_type_only: false,
            wildcard: false,
        };
        let import_b = CanonicalImport {
            range: Range::new(Position::new(0, 0), Position::new(0, 20), (500, 520)),
            ..import_a.clone()
        };

        let mut cg = CodeGraph::new_with_root("/repo".to_string());
        cg.add_file(
            "/repo/gen.rs",
            "rs",
            Language::Python,
            100,
            &[],
            &[import_a, import_b],
        );
        let tracer = crate::v2::trace::Tracer::new(false);
        cg.finalize(&tracer);

        let ids = cg.assign_ids(42, "main");
        let import_ids: Vec<i64> = cg
            .imports_iter()
            .map(|(idx, _, _)| ids[idx.index()])
            .collect();

        assert_eq!(import_ids.len(), 2);
        assert_ne!(import_ids[0], import_ids[1]);
    }

    #[test]
    fn def_on_import_panics_with_typed_unexpected_node_type() {
        use crate::v2::error::CodeGraphError;
        let mut cg = CodeGraph::new_with_root("/repo".to_string());
        let import = CanonicalImport {
            import_type: "NamedImport",
            binding_kind: ImportBindingKind::Named,
            mode: ImportMode::Declarative,
            path: "std::fs".to_string(),
            name: Some("read".to_string()),
            alias: None,
            scope_fqn: None,
            range: Range::new(Position::new(0, 0), Position::new(0, 10), (0, 10)),
            is_type_only: false,
            wildcard: false,
        };
        let (_, _, import_nodes) =
            cg.add_file("/repo/x.rs", "rs", Language::Python, 1, &[], &[import]);
        let import_idx = import_nodes[0];

        let payload = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = cg.def(import_idx);
        }))
        .expect_err("def() on Import index must panic");

        let typed = payload
            .downcast::<CodeGraphError>()
            .expect("panic payload should be a CodeGraphError");
        assert!(matches!(
            *typed,
            CodeGraphError::UnexpectedNodeType {
                expected: "Definition",
                ..
            }
        ));
        assert_eq!(typed.stage(), "graph_node");
    }
}
