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

use crate::v2::config::Language;
use crate::v2::error::FileReason;
use crate::v2::types::{
    DefKind, EdgeKind, ImportBindingKind, ImportMode, NodeKind, Range, Relationship,
    containment_relationship,
};

// ── Ids ──────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

// ── Graph definition metadata (pool-backed) ──────────────────────────────────

#[derive(Debug, Clone)]
pub struct GraphDef {
    pub definition_type: &'static str,
    pub kind: DefKind,
    pub name: StrId,
    pub fqn: StrId,
    pub fqn_sep: &'static str,
    pub range: Range,
    pub is_top_level: bool,
    pub metadata: Option<Box<GraphDefMeta>>,
}

#[derive(Debug, Clone, Default)]
pub struct GraphDefMeta {
    pub super_types: SmallVec<[StrId; 2]>,
    pub return_type: Option<StrId>,
    pub type_annotation: Option<StrId>,
    pub receiver_type: Option<StrId>,
    pub decorators: SmallVec<[StrId; 2]>,
    pub companion_of: Option<StrId>,
    pub is_exported: bool,
}

/// Pool-backed import.
#[derive(Debug, Clone)]
pub struct GraphImport {
    pub import_type: &'static str,
    pub binding_kind: ImportBindingKind,
    pub mode: ImportMode,
    pub path: StrId,
    pub name: Option<StrId>,
    pub alias: Option<StrId>,
    pub range: Range,
    pub is_type_only: bool,
    pub wildcard: bool,
}

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

    // Counters
    node_count: AtomicU32,
    def_count: AtomicU32,
    import_count: AtomicU32,

    pub root_path: String,
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
            node_count: AtomicU32::new(0),
            def_count: AtomicU32::new(0),
            import_count: AtomicU32::new(0),
            root_path,
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

    fn relative_path(&self, path: &str) -> String {
        path.strip_prefix(&self.root_path)
            .unwrap_or(path)
            .trim_start_matches('/')
            .to_string()
    }

    fn ensure_directory_chain(&self, relative_path: &str) -> Option<NodeId> {
        let parent = std::path::Path::new(relative_path).parent()?;
        let mut current: Option<NodeId> = None;
        let mut accumulated = String::new();

        for component in parent.components() {
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

            if let Some(parent_node) = current {
                // Only add the edge if we just created this dir node
                // (it might already be linked from a previous file).
                // DashMap entry returns the existing value, so we
                // skip the edge if the dir already existed. We rely
                // on push_edge being idempotent-ish (duplicate structural
                // edges are harmless and filtered at conversion).
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
}
