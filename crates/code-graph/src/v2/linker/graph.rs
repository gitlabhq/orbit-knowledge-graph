//! Concurrent, `&self`-insertable graph that replaces petgraph's `DiGraph`.
//!
//! All insert operations take `&self`, enabling parallel node/edge insertion
//! from multiple threads. Indexes are `DashMap` for concurrent read/write.

use dashmap::DashMap;
use gkg_utils::strings::{StrId, StringPool};
use rust_lapper::{Interval, Lapper};
use rustc_hash::FxHasher;
use smallvec::SmallVec;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU32, Ordering};

fn common_prefix_len(a: &str, b: &str) -> usize {
    a.bytes().zip(b.bytes()).take_while(|(x, y)| x == y).count()
}

use crate::v2::config::Language;
use crate::v2::error::FileReason;
use crate::v2::types::{
    DefKind, EdgeKind, NodeKind, Range, Relationship, containment_relationship,
};

// ── Ids ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NodeId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DefId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ImportId(pub u32);

// ── Node data ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum NodeData {
    Directory {
        path: String,
        name: String,
    },
    File {
        path: String,
        name: String,
        extension: String,
        language: Option<Language>,
        size: u64,
        reason: FileReason,
    },
    Definition {
        file_path: StrId,
        def_id: DefId,
    },
    Import {
        file_path: StrId,
        import_id: ImportId,
    },
}

impl NodeData {
    pub fn def_id(&self) -> Option<DefId> {
        match self {
            NodeData::Definition { def_id, .. } => Some(*def_id),
            _ => None,
        }
    }

    pub fn import_id(&self) -> Option<ImportId> {
        match self {
            NodeData::Import { import_id, .. } => Some(*import_id),
            _ => None,
        }
    }
}

// ── Edge data ────────────────────────────────────────────────────────────────

/// Flat edge. No pointers, no linked lists.
#[derive(Debug, Clone, Copy)]
pub struct Edge {
    pub source: NodeId,
    pub target: NodeId,
    pub relationship: Relationship,
}

// Re-export the shared definition/import types from state.rs.
// These are the same types the DSL engine produces.
pub use super::state::{GraphDef, GraphDefMeta, GraphImport};

// ── Definition range index ───────────────────────────────────────────────────

pub struct DefinitionRangeIndex {
    lapper: Lapper<u64, NodeId>,
}

impl DefinitionRangeIndex {
    pub fn from_ranges(ranges: impl IntoIterator<Item = (Range, NodeId)>) -> Self {
        let intervals = ranges
            .into_iter()
            .map(|(range, node)| Interval {
                start: range.byte_offset.0 as u64,
                stop: range.byte_offset.1 as u64,
                val: node,
            })
            .collect();
        Self {
            lapper: Lapper::new(intervals),
        }
    }

    pub fn find_enclosing(&self, start: u32, end: u32) -> Option<NodeId> {
        self.lapper
            .find(start as u64, end as u64)
            .filter(|iv| iv.start <= start as u64 && end as u64 <= iv.stop)
            .min_by_key(|iv| iv.stop.saturating_sub(iv.start))
            .map(|iv| iv.val)
    }

    pub fn find_enclosing_or_overlapping(&self, start: u32, end: u32) -> Option<NodeId> {
        self.find_enclosing(start, end).or_else(|| {
            self.lapper
                .find(start as u64, end as u64)
                .min_by_key(|iv| iv.stop.saturating_sub(iv.start))
                .map(|iv| iv.val)
        })
    }
}

// ── Hash helper ──────────────────────────────────────────────────────────────

fn compute_id(components: &[&str]) -> i64 {
    let mut h = FxHasher::default();
    components.hash(&mut h);
    (h.finish() & 0x7FFF_FFFF_FFFF_FFFF) as i64
}

#[inline]
pub fn hash_name(s: &str) -> u64 {
    let mut h = FxHasher::default();
    s.hash(&mut h);
    h.finish()
}

// ── The concurrent graph ─────────────────────────────────────────────────────

pub struct CodeGraph {
    // Stores
    pub nodes: boxcar::Vec<NodeData>,
    pub defs: boxcar::Vec<GraphDef>,
    pub imports: boxcar::Vec<GraphImport>,
    pub edges: boxcar::Vec<Edge>,
    pub strings: StringPool,

    // Indexes (concurrent read/write via DashMap)
    pub by_fqn: DashMap<u64, SmallVec<[NodeId; 8]>>,
    pub by_name: DashMap<u64, SmallVec<[NodeId; 8]>>,
    pub nested: DashMap<u64, DashMap<u64, SmallVec<[NodeId; 8]>>>,
    pub ancestors: DashMap<NodeId, SmallVec<[NodeId; 8]>>,
    pub definition_ranges: DashMap<String, DefinitionRangeIndex>,

    // Per-file adjacency (file NodeId -> its def/import NodeIds)
    pub file_defs: DashMap<NodeId, Vec<NodeId>>,
    pub file_imports: DashMap<NodeId, Vec<NodeId>>,

    // Dedup for ImportedSymbol edges (source, target, edge_kind)
    seen_import_edges: DashMap<(NodeId, NodeId, EdgeKind), ()>,

    // Construction-phase indexes (dropped after barrier 0)
    pub dir_index: DashMap<String, NodeId>,
    pub file_index: DashMap<String, NodeId>,

    /// Cross-file inferred return types. Written after Phase 2, read by
    /// Phase 3 resolvers. Keyed by DefId so chain resolution can fall back
    /// to this when `GraphDef.metadata.return_type` is None.
    pub global_return_types: DashMap<DefId, StrId>,

    // Counters
    node_count: AtomicU32,
    def_count: AtomicU32,
    import_count: AtomicU32,

    pub root_path: String,
    /// When true, only definitions/imports/call edges are emitted (no
    /// directory/file structure). Set by `emit_file_inventory_graph`.
    pub parsed_only: bool,
}

impl CodeGraph {
    pub fn new(root_path: String) -> Self {
        Self {
            nodes: boxcar::Vec::new(),
            defs: boxcar::Vec::new(),
            imports: boxcar::Vec::new(),
            edges: boxcar::Vec::new(),
            strings: StringPool::new(),
            by_fqn: DashMap::new(),
            by_name: DashMap::new(),
            nested: DashMap::new(),
            ancestors: DashMap::new(),
            definition_ranges: DashMap::new(),
            file_defs: DashMap::new(),
            file_imports: DashMap::new(),
            seen_import_edges: DashMap::new(),
            dir_index: DashMap::new(),
            file_index: DashMap::new(),
            global_return_types: DashMap::new(),
            node_count: AtomicU32::new(0),
            def_count: AtomicU32::new(0),
            import_count: AtomicU32::new(0),
            root_path,
            parsed_only: false,
        }
    }

    // ── Insert operations (all &self) ────────────────────────────────────

    pub fn push_node(&self, data: NodeData) -> NodeId {
        let idx = self.nodes.push(data);
        self.node_count.fetch_add(1, Ordering::Relaxed);
        NodeId(idx as u32)
    }

    pub fn push_def(&self, def: GraphDef) -> DefId {
        let idx = self.defs.push(def);
        self.def_count.fetch_add(1, Ordering::Relaxed);
        DefId(idx as u32)
    }

    pub fn push_import(&self, imp: GraphImport) -> ImportId {
        let idx = self.imports.push(imp);
        self.import_count.fetch_add(1, Ordering::Relaxed);
        ImportId(idx as u32)
    }

    pub fn push_edge(&self, edge: Edge) {
        if edge.relationship.target_node == NodeKind::ImportedSymbol {
            let key = (edge.source, edge.target, edge.relationship.edge_kind);
            if self.seen_import_edges.contains_key(&key) {
                return;
            }
            self.seen_import_edges.insert(key, ());
        }
        self.edges.push(edge);
    }

    /// Insert into the by_fqn index.
    pub fn index_fqn(&self, fqn: &str, node: NodeId) {
        self.by_fqn.entry(hash_name(fqn)).or_default().push(node);
    }

    /// Insert into the by_name index.
    pub fn index_name(&self, name: &str, node: NodeId) {
        self.by_name.entry(hash_name(name)).or_default().push(node);
    }

    /// Insert into the nested (scope -> member -> nodes) index.
    pub fn index_nested(&self, scope: &str, member: &str, node: NodeId) {
        self.nested
            .entry(hash_name(scope))
            .or_default()
            .entry(hash_name(member))
            .or_default()
            .push(node);
    }

    // ── File insertion (all &self) ──────────────────────────────────────

    /// Insert a parsed file and all its definitions/imports into the graph.
    /// Returns the node IDs assigned to the file, defs, and imports.
    /// Safe to call from multiple rayon workers concurrently.
    #[expect(
        clippy::too_many_arguments,
        reason = "file metadata is flat; a wrapper struct would just shuffle fields"
    )]
    pub fn add_file(
        &self,
        path: &str,
        extension: &str,
        language: Option<Language>,
        file_size: u64,
        definitions: Vec<GraphDef>,
        imports: Vec<GraphImport>,
        reason: FileReason,
    ) -> (NodeId, Vec<NodeId>, Vec<NodeId>) {
        let relative_path = self.relative_path(path);

        let file_name = std::path::Path::new(&relative_path)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();

        let file_node = self.push_node(NodeData::File {
            path: relative_path.clone(),
            name: file_name,
            extension: extension.to_string(),
            language,
            size: file_size,
            reason,
        });
        self.file_index.insert(relative_path.clone(), file_node);

        if let Some(dir_idx) = self.ensure_directory_chain(&relative_path) {
            self.push_edge(Edge {
                source: dir_idx,
                target: file_node,
                relationship: Relationship {
                    edge_kind: EdgeKind::Contains,
                    source_node: NodeKind::Directory,
                    target_node: NodeKind::File,
                    source_def_kind: None,
                    target_def_kind: None,
                },
            });
        }

        let mut def_nodes = Vec::with_capacity(definitions.len());
        let mut definition_ranges = Vec::with_capacity(definitions.len());

        for gdef in &definitions {
            let def_id = self.push_def(gdef.clone());
            let def_node = self.push_node(NodeData::Definition {
                file_path: self.strings.alloc(&relative_path),
                def_id,
            });
            def_nodes.push(def_node);

            let fqn_str = self.str(gdef.fqn);
            let name_str = self.str(gdef.name);
            self.index_fqn(fqn_str, def_node);
            self.index_name(name_str, def_node);

            if let Some(sep_pos) = fqn_str.rfind(gdef.fqn_sep) {
                let parent = &fqn_str[..sep_pos];
                self.index_nested(parent, name_str, def_node);
            }
            definition_ranges.push((gdef.range, def_node));

            self.push_edge(Edge {
                source: file_node,
                target: def_node,
                relationship: Relationship {
                    edge_kind: EdgeKind::Defines,
                    source_node: NodeKind::File,
                    target_node: NodeKind::Definition,
                    source_def_kind: None,
                    target_def_kind: None,
                },
            });
        }

        self.definition_ranges.insert(
            relative_path.clone(),
            DefinitionRangeIndex::from_ranges(definition_ranges),
        );

        // Intra-file containment edges (parent def → child def by FQN prefix).
        for (i, gdef) in definitions.iter().enumerate() {
            let fqn_str = self.str(gdef.fqn);
            let Some(sep_pos) = fqn_str.rfind(gdef.fqn_sep) else {
                continue;
            };
            let parent_fqn = &fqn_str[..sep_pos];
            for (j, parent_def) in definitions.iter().enumerate() {
                if j != i
                    && self.str(parent_def.fqn) == parent_fqn
                    && let Some(rel) = containment_relationship(parent_def.kind, gdef.kind)
                {
                    self.push_edge(Edge {
                        source: def_nodes[j],
                        target: def_nodes[i],
                        relationship: rel,
                    });
                    break;
                }
            }
        }

        self.file_defs.insert(file_node, def_nodes.clone());

        let mut import_nodes = Vec::with_capacity(imports.len());
        for gimp in imports {
            let import_id = self.push_import(gimp);
            let imp_node = self.push_node(NodeData::Import {
                file_path: self.strings.alloc(&relative_path),
                import_id,
            });
            import_nodes.push(imp_node);
            self.push_edge(Edge {
                source: file_node,
                target: imp_node,
                relationship: Relationship {
                    edge_kind: EdgeKind::Imports,
                    source_node: NodeKind::File,
                    target_node: NodeKind::ImportedSymbol,
                    source_def_kind: None,
                    target_def_kind: None,
                },
            });
        }

        self.file_imports.insert(file_node, import_nodes.clone());

        (file_node, def_nodes, import_nodes)
    }

    pub fn add_unparsed_file(
        &self,
        path: &str,
        language: Option<Language>,
        file_size: u64,
        reason: FileReason,
    ) -> NodeId {
        let (file_node, _, _) = self.add_file(
            path,
            std::path::Path::new(path)
                .extension()
                .and_then(|ext| ext.to_str())
                .unwrap_or(""),
            language,
            file_size,
            vec![],
            vec![],
            reason,
        );
        file_node
    }

    pub fn relative_path(&self, path: &str) -> String {
        path.strip_prefix(&self.root_path)
            .unwrap_or(path)
            .trim_start_matches('/')
            .to_string()
    }

    fn ensure_directory_chain(&self, relative_path: &str) -> Option<NodeId> {
        let parent = std::path::Path::new(relative_path).parent()?;
        let mut current: Option<NodeId> = None;

        // Root-level files have an empty parent. Create a "." root dir
        // so every file in the repo has a containing directory.
        let components: Vec<_> = parent.components().collect();
        if components.is_empty() {
            let dir_node = *self.dir_index.entry(".".to_string()).or_insert_with(|| {
                self.push_node(NodeData::Directory {
                    path: ".".to_string(),
                    name: ".".to_string(),
                })
            });
            return Some(dir_node);
        }

        let mut accumulated = String::new();
        for component in components {
            let name = component.as_os_str().to_string_lossy().to_string();
            if !accumulated.is_empty() {
                accumulated.push('/');
            }
            accumulated.push_str(&name);

            let dir_node = *self
                .dir_index
                .entry(accumulated.clone())
                .or_insert_with(|| {
                    self.push_node(NodeData::Directory {
                        path: accumulated.clone(),
                        name: name.clone(),
                    })
                });

            if let Some(parent_node) = current
                && parent_node != dir_node
            {
                self.push_edge(Edge {
                    source: parent_node,
                    target: dir_node,
                    relationship: Relationship {
                        edge_kind: EdgeKind::Contains,
                        source_node: NodeKind::Directory,
                        target_node: NodeKind::Directory,
                        source_def_kind: None,
                        target_def_kind: None,
                    },
                });
            }
            current = Some(dir_node);
        }

        current
    }

    // ── Read operations ──────────────────────────────────────────────────

    pub fn str(&self, id: StrId) -> &str {
        self.strings.get(id)
    }

    pub fn def(&self, id: DefId) -> &GraphDef {
        &self.defs[id.0 as usize]
    }

    pub fn import(&self, id: ImportId) -> &GraphImport {
        &self.imports[id.0 as usize]
    }

    pub fn node(&self, id: NodeId) -> &NodeData {
        &self.nodes[id.0 as usize]
    }

    pub fn def_fqn(&self, node_id: NodeId) -> &str {
        let node = self.node(node_id);
        match node.def_id() {
            Some(did) => self.str(self.def(did).fqn),
            None => "",
        }
    }

    pub fn def_name(&self, node_id: NodeId) -> &str {
        let node = self.node(node_id);
        match node.def_id() {
            Some(did) => self.str(self.def(did).name),
            None => "",
        }
    }

    pub fn def_kind(&self, node_id: NodeId) -> Option<DefKind> {
        self.node(node_id).def_id().map(|did| self.def(did).kind)
    }

    /// Return type for a definition, checking metadata first then the
    /// global inferred return type side-table.
    pub fn def_return_type(&self, did: DefId) -> Option<StrId> {
        let gdef = self.def(did);
        if let Some(meta) = &gdef.metadata
            && let Some(rt) = meta.return_type
        {
            return Some(rt);
        }
        self.global_return_types.get(&did).map(|v| *v)
    }

    /// Lookup by FQN with verification.
    pub fn lookup_fqn(&self, key: &str, verify: impl Fn(NodeId) -> bool) -> SmallVec<[NodeId; 8]> {
        match self.by_fqn.get(&hash_name(key)) {
            Some(candidates) => candidates
                .iter()
                .copied()
                .filter(|id| verify(*id))
                .collect(),
            None => SmallVec::new(),
        }
    }

    /// Lookup by bare name with verification.
    pub fn lookup_name(&self, key: &str, verify: impl Fn(NodeId) -> bool) -> SmallVec<[NodeId; 8]> {
        match self.by_name.get(&hash_name(key)) {
            Some(candidates) => candidates
                .iter()
                .copied()
                .filter(|id| verify(*id))
                .collect(),
            None => SmallVec::new(),
        }
    }

    pub fn name_exists(&self, key: &str) -> bool {
        self.by_name.contains_key(&hash_name(key))
    }

    /// Lookup nested member with verification.
    pub fn lookup_nested(
        &self,
        scope: &str,
        member: &str,
        verify: impl Fn(NodeId) -> bool,
    ) -> SmallVec<[NodeId; 8]> {
        let Some(inner) = self.nested.get(&hash_name(scope)) else {
            return SmallVec::new();
        };
        let Some(candidates) = inner.get(&hash_name(member)) else {
            return SmallVec::new();
        };
        candidates
            .iter()
            .copied()
            .filter(|id| verify(*id))
            .collect()
    }

    pub fn ancestors(&self, node: NodeId) -> Option<SmallVec<[NodeId; 8]>> {
        self.ancestors.get(&node).map(|v| v.clone())
    }

    pub fn enclosing_definition(&self, file_path: &str, start: u32, end: u32) -> Option<NodeId> {
        self.definition_ranges
            .get(file_path)
            .and_then(|idx| idx.find_enclosing(start, end))
    }

    pub fn add_call_edge(&self, source: NodeId, target: NodeId) {
        let (source_node_kind, source_def_kind) = self
            .node(source)
            .def_id()
            .map(|id| (NodeKind::Definition, Some(self.def(id).kind)))
            .unwrap_or((NodeKind::File, None));
        let target_def_kind = self.node(target).def_id().map(|id| self.def(id).kind);
        self.push_edge(Edge {
            source,
            target,
            relationship: Relationship {
                edge_kind: EdgeKind::Calls,
                source_node: source_node_kind,
                target_node: NodeKind::Definition,
                source_def_kind,
                target_def_kind,
            },
        });
    }

    pub fn definition_for_range(&self, file_path: &str, start: u32, end: u32) -> Option<NodeId> {
        self.definition_ranges
            .get(file_path)
            .and_then(|idx| idx.find_enclosing_or_overlapping(start, end))
    }

    /// File path for any node kind.
    pub fn node_path(&self, id: NodeId) -> &str {
        match self.node(id) {
            NodeData::Directory { path, .. } | NodeData::File { path, .. } => path,
            NodeData::Definition { file_path, .. } | NodeData::Import { file_path, .. } => {
                self.str(*file_path)
            }
        }
    }

    pub fn try_def_for_node(&self, id: NodeId) -> Option<&GraphDef> {
        self.node(id).def_id().map(|did| self.def(did))
    }

    pub fn try_import_for_node(&self, id: NodeId) -> Option<&GraphImport> {
        self.node(id).import_id().map(|iid| self.import(iid))
    }

    /// Get the import data for an import node. Panics if the node is not an import.
    pub fn import_for_node(&self, id: NodeId) -> &GraphImport {
        self.try_import_for_node(id)
            .unwrap_or_else(|| panic!("expected Import node at {id:?}, got {:?}", self.node(id)))
    }

    pub fn file_node_for_path(&self, file_path: &str) -> Option<NodeId> {
        self.file_index.get(file_path).map(|v| *v)
    }



    // ── Finalize (post barrier-0, pre barrier-1) ─────────────────────────

    /// Build Extends edges from super_types metadata, then compute transitive
    /// ancestor chains via BFS. Call after all files are inserted and before
    /// resolution.
    pub fn finalize(&self) {
        self.link_extends();
        self.build_ancestor_table();
    }

    fn link_extends(&self) {
        let count = self.nodes.count();
        let mut edges = Vec::new();
        for i in 0..count {
            let node = &self.nodes[i];
            let NodeData::Definition { def_id, .. } = node else {
                continue;
            };
            let gdef = self.def(*def_id);
            let Some(meta) = &gdef.metadata else {
                continue;
            };
            if meta.super_types.is_empty() {
                continue;
            }
            let child = NodeId(i as u32);
            let child_fqn = self.str(gdef.fqn).to_string();
            for &super_id in &meta.super_types {
                let super_name = self.str(super_id);
                let mut targets = super::resolver::resolve_scope_nodes(self, super_name);
                targets.retain(|t| *t != child);
                if targets.len() > 1 {
                    let child_prefix = format!("{}.", child_fqn);
                    targets.sort_by(|&a, &b| {
                        let a_fqn = self.def_fqn(a);
                        let b_fqn = self.def_fqn(b);
                        let a_nested = a_fqn.starts_with(&child_prefix);
                        let b_nested = b_fqn.starts_with(&child_prefix);
                        a_nested.cmp(&b_nested).then_with(|| {
                            let a_common = common_prefix_len(a_fqn, &child_fqn);
                            let b_common = common_prefix_len(b_fqn, &child_fqn);
                            b_common.cmp(&a_common)
                        })
                    });
                    targets.truncate(1);
                }
                for &target in &targets {
                    edges.push((child, target));
                }
            }
        }
        for (child, parent) in edges {
            self.push_edge(Edge {
                source: child,
                target: parent,
                relationship: Relationship {
                    edge_kind: EdgeKind::Extends,
                    source_node: NodeKind::Definition,
                    target_node: NodeKind::Definition,
                    source_def_kind: None,
                    target_def_kind: None,
                },
            });
        }
    }

    fn build_ancestor_table(&self) {
        // Build adjacency map for Extends edges only.
        let mut extends_adj: rustc_hash::FxHashMap<NodeId, Vec<NodeId>> =
            rustc_hash::FxHashMap::default();
        for i in 0..self.edges.count() {
            let edge = &self.edges[i];
            if edge.relationship.edge_kind == EdgeKind::Extends {
                extends_adj
                    .entry(edge.source)
                    .or_default()
                    .push(edge.target);
            }
        }
        // BFS from each node with outgoing Extends edges.
        for &start in extends_adj.keys() {
            let mut chain: SmallVec<[NodeId; 8]> = SmallVec::new();
            let mut visited = rustc_hash::FxHashSet::default();
            visited.insert(start);
            let mut queue = std::collections::VecDeque::new();
            if let Some(direct) = extends_adj.get(&start) {
                for &parent in direct {
                    if visited.insert(parent) {
                        queue.push_back(parent);
                        chain.push(parent);
                    }
                }
            }
            while let Some(current) = queue.pop_front() {
                if let Some(parents) = extends_adj.get(&current) {
                    for &parent in parents {
                        if visited.insert(parent) {
                            queue.push_back(parent);
                            chain.push(parent);
                        }
                    }
                }
            }
            if !chain.is_empty() {
                self.ancestors.insert(start, chain);
            }
        }
    }

    // ── Iteration helpers ────────────────────────────────────────────────

    /// Iterate all definition nodes: (NodeId, file_path, &GraphDef).
    pub fn iter_definitions(&self) -> impl Iterator<Item = (NodeId, &str, &GraphDef)> + '_ {
        (0..self.nodes.count()).filter_map(move |i| {
            let node = &self.nodes[i];
            if let NodeData::Definition { file_path, def_id } = node {
                Some((NodeId(i as u32), self.str(*file_path), self.def(*def_id)))
            } else {
                None
            }
        })
    }

    /// Iterate all import nodes: (NodeId, file_path, &GraphImport).
    pub fn iter_imports(&self) -> impl Iterator<Item = (NodeId, &str, &GraphImport)> + '_ {
        (0..self.nodes.count()).filter_map(move |i| {
            let node = &self.nodes[i];
            if let NodeData::Import {
                file_path,
                import_id,
            } = node
            {
                Some((
                    NodeId(i as u32),
                    self.str(*file_path),
                    self.import(*import_id),
                ))
            } else {
                None
            }
        })
    }

    /// Iterate all file nodes: (NodeId, path, name, extension, language, size, reason).
    pub fn iter_files(
        &self,
    ) -> impl Iterator<Item = (NodeId, &str, &str, &str, Option<Language>, u64, &FileReason)> + '_
    {
        (0..self.nodes.count()).filter_map(move |i| {
            let node = &self.nodes[i];
            if let NodeData::File {
                path,
                name,
                extension,
                language,
                size,
                reason,
            } = node
            {
                Some((
                    NodeId(i as u32),
                    path.as_str(),
                    name.as_str(),
                    extension.as_str(),
                    *language,
                    *size,
                    reason,
                ))
            } else {
                None
            }
        })
    }

    /// Iterate all directory nodes: (NodeId, path, name).
    pub fn iter_directories(&self) -> impl Iterator<Item = (NodeId, &str, &str)> + '_ {
        (0..self.nodes.count()).filter_map(move |i| {
            let node = &self.nodes[i];
            if let NodeData::Directory { path, name } = node {
                Some((NodeId(i as u32), path.as_str(), name.as_str()))
            } else {
                None
            }
        })
    }

    /// Iterate all edges.
    pub fn iter_edges(&self) -> impl Iterator<Item = &Edge> + '_ {
        (0..self.edges.count()).map(move |i| &self.edges[i])
    }

    // ── Counts ───────────────────────────────────────────────────────────

    pub fn node_count(&self) -> u32 {
        self.node_count.load(Ordering::Relaxed)
    }

    pub fn def_count(&self) -> u32 {
        self.def_count.load(Ordering::Relaxed)
    }

    pub fn import_count(&self) -> u32 {
        self.import_count.load(Ordering::Relaxed)
    }

    pub fn edge_count(&self) -> usize {
        self.edges.count()
    }

    pub fn drop_construction_indexes(&self) {
        self.dir_index.clear();
        self.file_index.clear();
        self.seen_import_edges.clear();
    }
}

// ── Serialization helpers (free functions) ───────────────────────────────────

fn node_property(graph: &CodeGraph, id: NodeId, property: &str) -> Option<String> {
    let value = match graph.node(id) {
        NodeData::File {
            extension,
            language,
            reason,
            ..
        } => match property {
            "extension" => Some(extension.clone()),
            "language" => Some(language.map_or("unknown", |l| l.names()[0]).to_string()),
            "reason" => Some(reason.to_string()),
            _ => None,
        },
        NodeData::Definition { def_id, .. } => match property {
            "definition_type" => Some(graph.def(*def_id).definition_type.to_string()),
            _ => None,
        },
        NodeData::Import { import_id, .. } => match property {
            "import_type" => Some(graph.import(*import_id).import_type.to_string()),
            _ => None,
        },
        NodeData::Directory { .. } => None,
    };
    value.filter(|v| !v.is_empty())
}

pub fn build_node_tags(
    graph: &CodeGraph,
    tag_properties: &std::collections::HashMap<String, Vec<(String, String)>>,
) -> Vec<Vec<String>> {
    let count = graph.nodes.count();
    let mut tags = Vec::with_capacity(count);
    for i in 0..count {
        let node = &graph.nodes[i];
        let kind_name = match node {
            NodeData::File { .. } => "File",
            NodeData::Definition { .. } => "Definition",
            NodeData::Import { .. } => "ImportedSymbol",
            NodeData::Directory { .. } => {
                tags.push(Vec::new());
                continue;
            }
        };
        let Some(props) = tag_properties.get(kind_name) else {
            tags.push(Vec::new());
            continue;
        };
        tags.push(
            props
                .iter()
                .filter_map(|(tag_key, prop_name)| {
                    let val = node_property(graph, NodeId(i as u32), prop_name)?;
                    Some(format!("{tag_key}:{val}"))
                })
                .collect(),
        );
    }
    tags
}

#[expect(
    clippy::needless_range_loop,
    reason = "indexes into both ids[i] and boxcar::Vec nodes[i] which has no zip-friendly iterator"
)]
pub fn assign_ids(graph: &CodeGraph, project_id: i64, branch: &str) -> Vec<i64> {
    use std::fmt::Write as _;
    let pid = project_id.to_string();
    let count = graph.nodes.count();
    let mut ids = vec![0i64; count];
    let mut range_buf = String::new();
    for i in 0..count {
        ids[i] = match &graph.nodes[i] {
            NodeData::Directory { path, .. } => compute_id(&[&pid, branch, "dir", path]),
            NodeData::File { path, .. } => compute_id(&[&pid, branch, "file", path]),
            NodeData::Definition { file_path, def_id } => {
                let def = graph.def(*def_id);
                range_buf.clear();
                let _ = write!(
                    range_buf,
                    "{}:{}",
                    def.range.byte_offset.0, def.range.byte_offset.1
                );
                compute_id(&[
                    &pid,
                    branch,
                    "def",
                    graph.str(*file_path),
                    graph.str(def.fqn),
                    &range_buf,
                ])
            }
            NodeData::Import {
                file_path,
                import_id,
            } => {
                let imp = graph.import(*import_id);
                range_buf.clear();
                let _ = write!(
                    range_buf,
                    "{}:{}",
                    imp.range.byte_offset.0, imp.range.byte_offset.1
                );
                compute_id(&[
                    &pid,
                    branch,
                    "import",
                    graph.str(*file_path),
                    graph.str(imp.path),
                    imp.name.map(|id| graph.str(id)).unwrap_or("*"),
                    &range_buf,
                ])
            }
        };
    }
    ids
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_read_def() {
        let g = CodeGraph::new("/repo".into());
        let name = g.strings.alloc("MyClass");
        let fqn = g.strings.alloc("com.example.MyClass");
        let file = g.strings.alloc("src/main.rs");

        let def_id = g.push_def(GraphDef {
            definition_type: "class",
            kind: DefKind::Class,
            name,
            fqn,
            fqn_sep: ".",
            range: Range::empty(),
            is_top_level: true,
            metadata: None,
        });

        let node_id = g.push_node(NodeData::Definition {
            file_path: file,
            def_id,
        });

        g.index_fqn("com.example.MyClass", node_id);
        g.index_name("MyClass", node_id);

        assert_eq!(g.def_fqn(node_id), "com.example.MyClass");
        assert_eq!(g.def_name(node_id), "MyClass");

        let found = g.lookup_fqn("com.example.MyClass", |_| true);
        assert_eq!(found.len(), 1);
        assert_eq!(found[0], node_id);
    }

    #[test]
    fn concurrent_insert() {
        use std::sync::Arc;
        use std::thread;

        let g = Arc::new(CodeGraph::new("/repo".into()));
        let threads: Vec<_> = (0..8)
            .map(|t| {
                let g = Arc::clone(&g);
                thread::spawn(move || {
                    for i in 0..1000 {
                        let name_s = format!("def_{t}_{i}");
                        let name = g.strings.alloc(&name_s);
                        let fqn = g.strings.alloc(&format!("pkg.{name_s}"));
                        let did = g.push_def(GraphDef {
                            definition_type: "function",
                            kind: DefKind::Function,
                            name,
                            fqn,
                            fqn_sep: ".",
                            range: Range::empty(),
                            is_top_level: true,
                            metadata: None,
                        });
                        let nid = g.push_node(NodeData::Definition {
                            file_path: name,
                            def_id: did,
                        });
                        g.index_name(&name_s, nid);
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(g.node_count(), 8000);
        assert_eq!(g.def_count(), 8000);
    }

    #[test]
    fn add_file_concurrent() {
        use std::sync::Arc;
        use std::thread;

        let g = Arc::new(CodeGraph::new("/repo".into()));
        let threads: Vec<_> = (0..4)
            .map(|t| {
                let g = Arc::clone(&g);
                thread::spawn(move || {
                    for i in 0..100 {
                        let file_path = format!("/repo/src/t{t}/file{i}.rs");
                        let def_name = format!("Def_{t}_{i}");
                        let def_fqn = format!("pkg.t{t}.{def_name}");
                        let name_id = g.strings.alloc(&def_name);
                        let fqn_id = g.strings.alloc(&def_fqn);

                        let defs = vec![GraphDef {
                            definition_type: "class",
                            kind: DefKind::Class,
                            name: name_id,
                            fqn: fqn_id,
                            fqn_sep: ".",
                            range: Range::empty(),
                            is_top_level: true,
                            metadata: None,
                        }];

                        let (file_node, def_nodes, _) = g.add_file(
                            &file_path,
                            "rs",
                            None,
                            100,
                            defs,
                            vec![],
                            crate::v2::error::FileReason::None,
                        );

                        assert_eq!(def_nodes.len(), 1);
                        assert_eq!(g.def_fqn(def_nodes[0]), def_fqn);
                        assert!(!g.lookup_fqn(&def_fqn, |_| true).is_empty());
                        assert!(g.file_defs.contains_key(&file_node));
                    }
                })
            })
            .collect();

        for t in threads {
            t.join().unwrap();
        }

        assert_eq!(g.def_count(), 400);
    }

    fn make_graph_with_hierarchy() -> CodeGraph {
        let g = CodeGraph::new("/repo".into());

        // Animal (class)
        let animal_name = g.strings.alloc("Animal");
        let animal_fqn = g.strings.alloc("pkg.Animal");
        let animal_fp = g.strings.alloc("src/animal.py");
        let animal_did = g.push_def(GraphDef {
            definition_type: "class",
            kind: DefKind::Class,
            name: animal_name,
            fqn: animal_fqn,
            fqn_sep: ".",
            range: Range::empty(),
            is_top_level: true,
            metadata: None,
        });
        let animal_node = g.push_node(NodeData::Definition {
            file_path: animal_fp,
            def_id: animal_did,
        });
        g.index_fqn("pkg.Animal", animal_node);
        g.index_name("Animal", animal_node);

        // Animal.speak (method)
        let speak_name = g.strings.alloc("speak");
        let speak_fqn = g.strings.alloc("pkg.Animal.speak");
        let speak_did = g.push_def(GraphDef {
            definition_type: "method",
            kind: DefKind::Method,
            name: speak_name,
            fqn: speak_fqn,
            fqn_sep: ".",
            range: Range::empty(),
            is_top_level: false,
            metadata: None,
        });
        let speak_node = g.push_node(NodeData::Definition {
            file_path: animal_fp,
            def_id: speak_did,
        });
        g.index_fqn("pkg.Animal.speak", speak_node);
        g.index_name("speak", speak_node);
        g.index_nested("pkg.Animal", "speak", speak_node);

        // Dog extends Animal
        let super_type = g.strings.alloc("pkg.Animal");
        let dog_name = g.strings.alloc("Dog");
        let dog_fqn = g.strings.alloc("pkg.Dog");
        let dog_did = g.push_def(GraphDef {
            definition_type: "class",
            kind: DefKind::Class,
            name: dog_name,
            fqn: dog_fqn,
            fqn_sep: ".",
            range: Range::empty(),
            is_top_level: true,
            metadata: Some(Box::new(GraphDefMeta {
                super_types: smallvec::smallvec![super_type],
                ..GraphDefMeta::default()
            })),
        });
        let dog_node = g.push_node(NodeData::Definition {
            file_path: animal_fp,
            def_id: dog_did,
        });
        g.index_fqn("pkg.Dog", dog_node);
        g.index_name("Dog", dog_node);

        // Dog.fetch (method)
        let fetch_name = g.strings.alloc("fetch");
        let fetch_fqn = g.strings.alloc("pkg.Dog.fetch");
        let fetch_did = g.push_def(GraphDef {
            definition_type: "method",
            kind: DefKind::Method,
            name: fetch_name,
            fqn: fetch_fqn,
            fqn_sep: ".",
            range: Range::empty(),
            is_top_level: false,
            metadata: None,
        });
        let fetch_node = g.push_node(NodeData::Definition {
            file_path: animal_fp,
            def_id: fetch_did,
        });
        g.index_fqn("pkg.Dog.fetch", fetch_node);
        g.index_name("fetch", fetch_node);
        g.index_nested("pkg.Dog", "fetch", fetch_node);

        g.finalize();
        g
    }

    #[test]
    fn finalize_builds_extends_edges() {
        let g = make_graph_with_hierarchy();
        let has_extends = (0..g.edges.count()).any(|i| {
            let e = &g.edges[i];
            e.relationship.edge_kind == EdgeKind::Extends
        });
        assert!(has_extends);
    }

    #[test]
    fn finalize_builds_ancestor_table() {
        let g = make_graph_with_hierarchy();
        let dog_nodes = crate::v2::linker::resolver::resolve_scope_nodes(&g, "pkg.Dog");
        assert_eq!(dog_nodes.len(), 1);
        let ancestors = g.ancestors(dog_nodes[0]);
        assert!(ancestors.is_some());
        let chain = ancestors.unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(g.def_fqn(chain[0]), "pkg.Animal");
    }

    #[test]
    fn iter_definitions_and_edges() {
        let g = make_graph_with_hierarchy();
        let def_count = g.iter_definitions().count();
        assert!(def_count >= 4);
        let edge_count = g.iter_edges().count();
        assert!(edge_count > 0);
    }
}

use gkg_utils::arrow::{AsRecordBatch, BatchBuilder, ColumnSpec, ColumnType};

// ── IncludeIndex ─────────────────────────────────────────────────────────────

/// Precomputed include-graph lookup tables for C/C++ resolution.
/// Built once before Phase 2 resolve, shared across all file resolvers.
#[derive(Default)]
pub struct IncludeIndex {
    /// file_path -> list of normalized include paths for that file
    pub include_map: rustc_hash::FxHashMap<String, Vec<String>>,
    /// suffix -> list of file NodeId values whose path ends with that suffix
    pub suffix_map: rustc_hash::FxHashMap<String, Vec<NodeId>>,
    /// file NodeId -> file path
    pub path_by_idx: rustc_hash::FxHashMap<NodeId, String>,
}

impl IncludeIndex {
    pub fn build(graph: &CodeGraph) -> Self {
        let mut idx = Self::default();

        for (_node_id, file_path, imp) in graph.iter_imports() {
            let raw = graph.str(imp.path);
            let cleaned = raw.trim_matches('"').trim_matches('<').trim_matches('>');
            let normalized = cleaned.trim_start_matches("../").trim_start_matches("./");
            idx.include_map
                .entry(file_path.to_string())
                .or_default()
                .push(normalized.to_string());
        }

        for (file_id, path, _name, _ext, _lang, _size, _reason) in graph.iter_files() {
            idx.path_by_idx.insert(file_id, path.to_string());
            let mut start = 0;
            loop {
                idx.suffix_map
                    .entry(path[start..].to_string())
                    .or_default()
                    .push(file_id);
                match path[start..].find('/') {
                    Some(pos) => start += pos + 1,
                    None => break,
                }
            }
        }

        idx
    }

    pub fn is_empty(&self) -> bool {
        self.include_map.is_empty()
    }
}

// ── ReexportIndex ────────────────────────────────────────────────────────────

/// Language-agnostic re-export lookup; a language hook decides what counts as a
/// re-export. `named`: `module -> { bound_name -> (target_module, target_name) }`.
/// `wildcard`: `module -> [starred source modules]`.
#[derive(Default)]
pub struct ReexportIndex {
    named: rustc_hash::FxHashMap<String, rustc_hash::FxHashMap<String, (String, String)>>,
    wildcard: rustc_hash::FxHashMap<String, Vec<String>>,
}

impl ReexportIndex {
    pub fn add_named(
        &mut self,
        module: String,
        bound: String,
        target_module: String,
        target_name: String,
    ) {
        self.named
            .entry(module)
            .or_default()
            .insert(bound, (target_module, target_name));
    }

    pub fn add_wildcard(&mut self, module: String, source_module: String) {
        self.wildcard.entry(module).or_default().push(source_module);
    }

    pub fn named(&self, module: &str, name: &str) -> Option<(&str, &str)> {
        self.named
            .get(module)
            .and_then(|names| names.get(name))
            .map(|(module, name)| (module.as_str(), name.as_str()))
    }

    pub fn wildcard_sources(&self, module: &str) -> &[String] {
        self.wildcard
            .get(module)
            .map(|v| v.as_slice())
            .unwrap_or(&[])
    }
}

// ── Arrow Row types ──────────────────────────────────────────────────────────

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
        b.col("traversal_path")?.push_str("")?;
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
            ColumnSpec {
                name: "traversal_path".into(),
                col_type: ColumnType::Str,
                nullable: false,
            },
        ]
    }
}

fn write_range(b: &mut BatchBuilder, range: &Range) -> Result<(), arrow::error::ArrowError> {
    b.col("start_line")?.push_int(range.start.line as i64 + 1)?;
    b.col("end_line")?.push_int(range.end.line as i64 + 1)?;
    b.col("start_byte")?.push_int(range.byte_offset.0 as i64)?;
    b.col("end_byte")?.push_int(range.byte_offset.1 as i64)?;
    b.col("start_char")?
        .push_int(range.start.column as i64 + 1)?;
    b.col("end_char")?.push_int(range.end.column as i64 + 1)?;
    Ok(())
}

pub struct DirectoryRow<'a> {
    pub path: &'a str,
    pub name: &'a str,
    pub id: i64,
}
impl<C: gkg_utils::arrow::RowEnvelope> AsRecordBatch<C> for DirectoryRow<'_> {
    fn write_row(&self, b: &mut BatchBuilder, ctx: &C) -> Result<(), arrow::error::ArrowError> {
        ctx.write_header(b, self.id)?;
        b.col("path")?.push_str(self.path)?;
        b.col("name")?.push_str(self.name)?;
        Ok(())
    }
}

pub struct FileRow<'a> {
    pub path: &'a str,
    pub name: &'a str,
    pub extension: &'a str,
    pub language: &'a str,
    pub size: u64,
    pub reason: &'a str,
    pub id: i64,
}
impl<C: gkg_utils::arrow::RowEnvelope> AsRecordBatch<C> for FileRow<'_> {
    fn write_row(&self, b: &mut BatchBuilder, ctx: &C) -> Result<(), arrow::error::ArrowError> {
        ctx.write_header(b, self.id)?;
        b.col("path")?.push_str(self.path)?;
        b.col("name")?.push_str(self.name)?;
        b.col("extension")?.push_str(self.extension)?;
        b.col("language")?.push_str(self.language)?;
        b.col("size_bytes")?.push_int(self.size as i64)?;
        b.col("reason")?.push_str(self.reason)?;
        Ok(())
    }
}

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
            .push_str(self.import.name.map(|id| self.pool.get(id)).unwrap_or(""))?;
        b.col("identifier_alias")?
            .push_str(self.import.alias.map(|id| self.pool.get(id)).unwrap_or(""))?;
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
        b.col("traversal_path")?.push_str("")?;
        Ok(())
    }
}
