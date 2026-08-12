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

pub struct ConcurrentGraph {
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

impl ConcurrentGraph {
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

    pub fn def_in_file(&self, node_id: NodeId, file_path: &str) -> bool {
        self.node_path(node_id) == file_path
    }

    pub fn file_node_for_path(&self, file_path: &str) -> Option<NodeId> {
        self.file_index.get(file_path).map(|v| *v)
    }

    /// Like `lookup_nested` but appends matches to `out` and returns whether
    /// any were found.
    pub fn lookup_nested_into(
        &self,
        scope: &str,
        member: &str,
        verify: impl Fn(NodeId) -> bool,
        out: &mut Vec<NodeId>,
    ) -> bool {
        let found = self.lookup_nested(scope, member, verify);
        if found.is_empty() {
            return false;
        }
        out.extend_from_slice(&found);
        true
    }

    /// Resolve a scope name to definition nodes. Tries by_fqn first, then
    /// by_name filtered to type containers, then segmented qualified-name walk.
    pub fn resolve_scope_nodes(&self, name: &str) -> SmallVec<[NodeId; 8]> {
        let by_fqn = self.lookup_fqn(name, |idx| self.def_fqn(idx) == name);
        if !by_fqn.is_empty() {
            return by_fqn;
        }
        let by_name = self.lookup_name(name, |idx| {
            self.def_name(idx) == name
                && self
                    .node(idx)
                    .def_id()
                    .is_some_and(|d| self.def(d).kind.is_type_container())
        });
        if !by_name.is_empty() {
            return by_name;
        }
        for sep in &[".", "::"] {
            let segments: Vec<&str> = name.split(sep).collect();
            if segments.len() < 2 {
                continue;
            }
            let first_matches = self.lookup_name(segments[0], |idx| {
                self.def_name(idx) == segments[0]
                    && self
                        .node(idx)
                        .def_id()
                        .is_some_and(|d| self.def(d).kind.is_type_container())
            });
            if first_matches.is_empty() {
                continue;
            }
            let rest = &segments[1..].join(sep);
            for &node in &first_matches {
                let prefix_fqn = self.def_fqn(node);
                let candidate = format!("{prefix_fqn}{sep}{rest}");
                let matches = self.lookup_fqn(&candidate, |idx| self.def_fqn(idx) == candidate);
                if !matches.is_empty() {
                    return matches;
                }
            }
        }
        SmallVec::new()
    }

    /// Resolve nested member with hierarchy (ancestor chain) walk.
    pub fn lookup_nested_with_hierarchy(
        &self,
        scope_fqn: &str,
        member_name: &str,
        out: &mut Vec<NodeId>,
    ) -> bool {
        let start_nodes = self.resolve_scope_nodes(scope_fqn);
        if start_nodes.is_empty() {
            return false;
        }
        let verify_member = |idx: NodeId| self.def_name(idx) == member_name;
        for &start in &start_nodes {
            let actual_fqn = self.def_fqn(start);
            if self.lookup_nested_into(actual_fqn, member_name, verify_member, out) {
                return true;
            }
            if let Some(chain) = self.ancestors(start) {
                for &ancestor in &chain {
                    let ancestor_fqn = self.def_fqn(ancestor);
                    if self.lookup_nested_into(ancestor_fqn, member_name, verify_member, out) {
                        return true;
                    }
                }
            }
        }
        false
    }

    pub fn lookup_nested_from_node_with_hierarchy(
        &self,
        scope_node: NodeId,
        member_name: &str,
        out: &mut Vec<NodeId>,
    ) -> bool {
        let scope_fqn = self.def_fqn(scope_node);
        let verify_member = |idx: NodeId| self.def_name(idx) == member_name;
        if self.lookup_nested_into(scope_fqn, member_name, verify_member, out) {
            return true;
        }
        if let Some(chain) = self.ancestors(scope_node) {
            for &ancestor in &chain {
                let ancestor_fqn = self.def_fqn(ancestor);
                if self.lookup_nested_into(ancestor_fqn, member_name, verify_member, out) {
                    return true;
                }
            }
        }
        false
    }

    /// Find methods whose `receiver_type` metadata matches `type_name`.
    pub fn lookup_by_receiver_type(
        &self,
        type_name: &str,
        member_name: &str,
        out: &mut Vec<NodeId>,
    ) {
        let candidates = self.lookup_name(member_name, |idx| {
            self.node(idx)
                .def_id()
                .is_some_and(|d| self.str(self.def(d).name) == member_name)
        });
        let bare_type = type_name.rsplit_once('.').map_or(type_name, |(_, t)| t);
        for idx in candidates {
            if let Some(did) = self.node(idx).def_id()
                && let Some(meta) = &self.def(did).metadata
                && let Some(rt) = meta.receiver_type
            {
                let rt_str = self.str(rt);
                if rt_str == type_name || rt_str == bare_type {
                    out.push(idx);
                }
            }
        }
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
                let mut targets = self.resolve_scope_nodes(super_name);
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

    /// Compute stable IDs for all nodes. Returns a dense Vec indexed by
    /// `NodeId.0` for O(1) lookup.
    /// Returns a vec indexed by `NodeId.0`, mapping each node to its
    /// denormalized tag strings. `tag_properties` maps node kind name
    /// (e.g. `"File"`) to `(tag_key, property_name)` pairs.
    fn node_property(&self, id: NodeId, property: &str) -> Option<String> {
        let value = match self.node(id) {
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
                "definition_type" => Some(self.def(*def_id).definition_type.to_string()),
                _ => None,
            },
            NodeData::Import { import_id, .. } => match property {
                "import_type" => Some(self.import(*import_id).import_type.to_string()),
                _ => None,
            },
            NodeData::Directory { .. } => None,
        };
        value.filter(|v| !v.is_empty())
    }

    pub fn build_node_tags(
        &self,
        tag_properties: &std::collections::HashMap<String, Vec<(String, String)>>,
    ) -> Vec<Vec<String>> {
        let count = self.nodes.count();
        let mut tags = Vec::with_capacity(count);
        for i in 0..count {
            let node = &self.nodes[i];
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
                        let val = self.node_property(NodeId(i as u32), prop_name)?;
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
    pub fn assign_ids(&self, project_id: i64, branch: &str) -> Vec<i64> {
        use std::fmt::Write as _;
        let pid = project_id.to_string();
        let count = self.nodes.count();
        let mut ids = vec![0i64; count];
        let mut range_buf = String::new();
        for i in 0..count {
            ids[i] = match &self.nodes[i] {
                NodeData::Directory { path, .. } => compute_id(&[&pid, branch, "dir", path]),
                NodeData::File { path, .. } => compute_id(&[&pid, branch, "file", path]),
                NodeData::Definition { file_path, def_id } => {
                    let def = self.def(*def_id);
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
                        self.str(*file_path),
                        self.str(def.fqn),
                        &range_buf,
                    ])
                }
                NodeData::Import {
                    file_path,
                    import_id,
                } => {
                    let imp = self.import(*import_id);
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
                        self.str(*file_path),
                        self.str(imp.path),
                        imp.name.map(|id| self.str(id)).unwrap_or("*"),
                        &range_buf,
                    ])
                }
            };
        }
        ids
    }

    pub fn drop_construction_indexes(&self) {
        self.dir_index.clear();
        self.file_index.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_read_def() {
        let g = ConcurrentGraph::new("/repo".into());
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

        let g = Arc::new(ConcurrentGraph::new("/repo".into()));
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

        let g = Arc::new(ConcurrentGraph::new("/repo".into()));
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

    fn make_graph_with_hierarchy() -> ConcurrentGraph {
        let g = ConcurrentGraph::new("/repo".into());

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
    fn resolve_scope_nodes_by_fqn() {
        let g = make_graph_with_hierarchy();
        let found = g.resolve_scope_nodes("pkg.Animal");
        assert_eq!(found.len(), 1);
        assert_eq!(g.def_fqn(found[0]), "pkg.Animal");
    }

    #[test]
    fn resolve_scope_nodes_by_name_fallback() {
        let g = make_graph_with_hierarchy();
        let found = g.resolve_scope_nodes("Animal");
        assert_eq!(found.len(), 1);
        assert_eq!(g.def_fqn(found[0]), "pkg.Animal");
    }

    #[test]
    fn lookup_nested_with_hierarchy_inherited() {
        let g = make_graph_with_hierarchy();
        let mut out = Vec::new();
        // Dog inherits speak from Animal
        let found = g.lookup_nested_with_hierarchy("pkg.Dog", "speak", &mut out);
        assert!(found);
        assert_eq!(out.len(), 1);
        assert_eq!(g.def_fqn(out[0]), "pkg.Animal.speak");
    }

    #[test]
    fn lookup_nested_with_hierarchy_own_member() {
        let g = make_graph_with_hierarchy();
        let mut out = Vec::new();
        let found = g.lookup_nested_with_hierarchy("pkg.Dog", "fetch", &mut out);
        assert!(found);
        assert_eq!(out.len(), 1);
        assert_eq!(g.def_fqn(out[0]), "pkg.Dog.fetch");
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
        let dog_nodes = g.resolve_scope_nodes("pkg.Dog");
        assert_eq!(dog_nodes.len(), 1);
        let ancestors = g.ancestors(dog_nodes[0]);
        assert!(ancestors.is_some());
        let chain = ancestors.unwrap();
        assert_eq!(chain.len(), 1);
        assert_eq!(g.def_fqn(chain[0]), "pkg.Animal");
    }

    #[test]
    fn lookup_by_receiver_type_finds_methods() {
        let g = ConcurrentGraph::new("/repo".into());
        let fp = g.strings.alloc("src/main.go");
        let recv = g.strings.alloc("Service");
        let method_name = g.strings.alloc("Run");
        let method_fqn = g.strings.alloc("main.Run");
        let did = g.push_def(GraphDef {
            definition_type: "method",
            kind: DefKind::Method,
            name: method_name,
            fqn: method_fqn,
            fqn_sep: ".",
            range: Range::empty(),
            is_top_level: false,
            metadata: Some(Box::new(GraphDefMeta {
                receiver_type: Some(recv),
                ..GraphDefMeta::default()
            })),
        });
        let node = g.push_node(NodeData::Definition {
            file_path: fp,
            def_id: did,
        });
        g.index_name("Run", node);

        let mut out = Vec::new();
        g.lookup_by_receiver_type("Service", "Run", &mut out);
        assert_eq!(out.len(), 1);
        assert_eq!(g.def_fqn(out[0]), "main.Run");
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
