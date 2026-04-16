//! Centralized linker state with collision-safe verified lookups and
//! arena-backed string storage.
//!
//! ## Verified lookups
//!
//! All hash-keyed index maps are wrapped in [`VerifiedMap`] / [`NestedMap`]
//! which force callers to provide the original string key and a verifier
//! function. There is no API to get raw unverified results — the collision
//! bug class is structurally impossible.
//!
//! ## Arena allocation
//!
//! Two arena scopes eliminate per-string heap allocation and Drop overhead:
//!
//! - **[`GraphArena`]** (`'arena`): per-language, lives as long as the graph.
//!   Holds all definition names, FQN strings, import paths, metadata strings.
//!   Allocated during Phase 1 under the Mutex, read-only in Phase 2.
//!
//! - **[`FileArena`]** (`'file`): per-file, thread-local. Holds all scratch
//!   strings for walker caches, scope names, constructed FQNs. Created at
//!   Phase 2 file start, dropped wholesale when `FusedWalkResult` is returned.
//!   Output (`Vec<(NodeIndex, NodeIndex, GraphEdge)>`) contains no arena refs,
//!   so `'file` never escapes the walk.
//!
//! This module is designed to be adopted incrementally: existing code in
//! `graph.rs` and `walker.rs` can migrate to these types one map at a time.

use std::hash::{Hash, Hasher};

use bumpalo::Bump;
use code_graph_types::{DefKind, Range};
use petgraph::graph::NodeIndex;
use rustc_hash::{FxHashMap, FxHasher};
use smallvec::SmallVec;

use bumpalo::collections::String as BumpString;

// ── Hash key ────────────────────────────────────────────────────

/// Hash a string for use as an index key. FxHash for speed.
///
/// Used internally by VerifiedMap/NestedMap. Also public for
/// `ssa_names: FxHashSet<u64>` in the walker, which uses conservative
/// hash-based membership checks (collision = extra work, never wrong edges).
#[inline]
pub fn hash_name(s: &str) -> u64 {
    let mut h = FxHasher::default();
    s.hash(&mut h);
    h.finish()
}

// ── VerifiedMap ─────────────────────────────────────────────────

/// A hash-keyed index map that forces verification on every lookup.
///
/// Stores `FxHashMap<u64, SmallVec<[NodeIndex; N]>>` internally. The u64
/// keys avoid string pointer chases during HashMap probing, but hash
/// collisions (~10⁻⁹ per lookup) can return wrong entries. VerifiedMap
/// makes it structurally impossible to consume unverified results.
///
/// # API
///
/// - [`insert`]: add an entry (hashes the key internally)
/// - [`lookup`]: get entries, filtered through a caller-provided verifier
/// - [`lookup_into`]: same but appends to an existing `Vec` (avoids alloc)
/// - [`contains`]: conservative existence check (collision = false positive = extra work)
/// - [`is_empty`]: check if the map has any entries at all
pub struct VerifiedMap<const N: usize = 8> {
    inner: FxHashMap<u64, SmallVec<[NodeIndex; N]>>,
}

impl<const N: usize> VerifiedMap<N> {
    pub fn new() -> Self {
        Self {
            inner: FxHashMap::default(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            inner: FxHashMap::with_capacity_and_hasher(cap, Default::default()),
        }
    }

    /// Insert a value under the given string key.
    pub fn insert(&mut self, key: &str, value: NodeIndex) {
        self.inner.entry(hash_name(key)).or_default().push(value);
    }

    /// Look up entries for `key`, returning only those that pass `verify`.
    ///
    /// The verifier receives each candidate `NodeIndex` and must check that
    /// the actual stored data matches `key` (e.g. `|idx| graph.def(idx).name == key`).
    pub fn lookup(
        &self,
        key: &str,
        verify: impl Fn(NodeIndex) -> bool,
    ) -> SmallVec<[NodeIndex; N]> {
        match self.inner.get(&hash_name(key)) {
            Some(candidates) => candidates
                .iter()
                .copied()
                .filter(|idx| verify(*idx))
                .collect(),
            None => SmallVec::new(),
        }
    }

    /// Like [`lookup`] but appends to `out` instead of allocating.
    /// Returns `true` if any verified entries were found.
    pub fn lookup_into(
        &self,
        key: &str,
        verify: impl Fn(NodeIndex) -> bool,
        out: &mut Vec<NodeIndex>,
    ) -> bool {
        let Some(candidates) = self.inner.get(&hash_name(key)) else {
            return false;
        };
        let before = out.len();
        for &idx in candidates {
            if verify(idx) {
                out.push(idx);
            }
        }
        out.len() > before
    }

    /// Conservative existence check. A hash collision can produce a false
    /// positive (name absent but hash matches another entry), which causes
    /// extra work but never wrong edges — callers use this for early-skip
    /// decisions where "maybe yes" is safe.
    pub fn contains(&self, key: &str) -> bool {
        self.inner.contains_key(&hash_name(key))
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }
}

impl<const N: usize> Default for VerifiedMap<N> {
    fn default() -> Self {
        Self::new()
    }
}

// ── NestedMap ───────────────────────────────────────────────────

/// Two-level hash-keyed index map for scope → member lookups.
///
/// `nested_defs[hash(scope_fqn)][hash(member_name)]` → `SmallVec<[NodeIndex; 8]>`
///
/// Both levels are verified on lookup: the outer key (scope) and inner key
/// (member) are checked against actual graph data. No raw access.
pub struct NestedMap {
    inner: FxHashMap<u64, FxHashMap<u64, SmallVec<[NodeIndex; 8]>>>,
}

impl NestedMap {
    pub fn new() -> Self {
        Self {
            inner: FxHashMap::default(),
        }
    }

    /// Insert a member under a scope.
    pub fn insert(&mut self, scope: &str, member: &str, value: NodeIndex) {
        self.inner
            .entry(hash_name(scope))
            .or_default()
            .entry(hash_name(member))
            .or_default()
            .push(value);
    }

    /// Look up members of a scope, verifying both scope and member keys.
    ///
    /// `verify_member` checks the candidate's name against `member`.
    /// Scope verification is implicit: callers pass a scope string that was
    /// already verified against the graph (e.g. from `def_fqn(start_node)`).
    /// If two scope FQNs hash-collide, entries from the wrong scope appear
    /// in the inner map, but `verify_member` filters them as long as the
    /// member names don't also collide (independent events, ~10⁻¹⁸).
    pub fn lookup(
        &self,
        scope: &str,
        member: &str,
        verify_member: impl Fn(NodeIndex) -> bool,
    ) -> SmallVec<[NodeIndex; 8]> {
        let Some(inner) = self.inner.get(&hash_name(scope)) else {
            return SmallVec::new();
        };
        let Some(candidates) = inner.get(&hash_name(member)) else {
            return SmallVec::new();
        };
        candidates
            .iter()
            .copied()
            .filter(|idx| verify_member(*idx))
            .collect()
    }

    /// Like [`lookup`] but appends to `out`. Returns `true` if any found.
    pub fn lookup_into(
        &self,
        scope: &str,
        member: &str,
        verify_member: impl Fn(NodeIndex) -> bool,
        out: &mut Vec<NodeIndex>,
    ) -> bool {
        let Some(inner) = self.inner.get(&hash_name(scope)) else {
            return false;
        };
        let Some(candidates) = inner.get(&hash_name(member)) else {
            return false;
        };
        let before = out.len();
        for &idx in candidates {
            if verify_member(idx) {
                out.push(idx);
            }
        }
        out.len() > before
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
}

impl Default for NestedMap {
    fn default() -> Self {
        Self::new()
    }
}

// ── GraphIndexes ────────────────────────────────────────────────

/// All resolution indexes for a CodeGraph, bundled together.
///
/// Replaces the scattered `def_by_fqn`, `def_by_name`, `nested_defs` fields
/// on CodeGraph. Every lookup goes through VerifiedMap/NestedMap — no raw
/// hash access possible.
///
/// Construction-only indexes (`dir_index`, `file_index`) are held as
/// `Option` and dropped after `finalize()`.
pub struct GraphIndexes {
    /// FQN → definition nodes. Verified by `fqn.as_str() == key`.
    pub by_fqn: VerifiedMap,
    /// Bare name → definition nodes. Verified by `def.name == key`.
    pub by_name: VerifiedMap,
    /// Parent FQN → member name → definition nodes. Both levels verified.
    pub nested: NestedMap,
    /// Pre-computed ancestor chains from Extends edges (no hash keys).
    pub ancestors: FxHashMap<NodeIndex, SmallVec<[NodeIndex; 8]>>,

    /// Directory path → node index. Only used during Phase 1 construction.
    pub dir_index: Option<FxHashMap<String, NodeIndex>>,
    /// File path → node index. Only used during Phase 1 construction.
    pub file_index: Option<FxHashMap<String, NodeIndex>>,
}

impl GraphIndexes {
    pub fn new() -> Self {
        Self {
            by_fqn: VerifiedMap::new(),
            by_name: VerifiedMap::new(),
            nested: NestedMap::new(),
            ancestors: FxHashMap::default(),
            dir_index: Some(FxHashMap::default()),
            file_index: Some(FxHashMap::default()),
        }
    }

    /// Drop construction-only indexes after finalize.
    pub fn drop_construction_indexes(&mut self) {
        self.dir_index = None;
        self.file_index = None;
    }
}

impl Default for GraphIndexes {
    fn default() -> Self {
        Self::new()
    }
}

// ── String pool ─────────────────────────────────────────────────

/// Index into [`StringPool`]. 4 bytes, Copy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct StrId(u32);

/// Pool of strings for graph-level storage.
///
/// Strings are packed contiguously in a single `Vec<u8>` buffer. An index
/// `Vec` stores `(offset, len)` pairs for O(1) retrieval. One large
/// allocation instead of ~500K individual `Box<str>` heap allocs.
///
/// Owned by `CodeGraph`. No lifetime parameter, no global lock, no memory
/// leak, no unsafe. Dropped in bulk when the graph is dropped.
pub struct StringPool {
    /// Contiguous UTF-8 byte buffer. All strings packed end-to-end.
    buf: Vec<u8>,
    /// (byte_offset, byte_len) into `buf` for each StrId.
    index: Vec<(u32, u32)>,
}

impl StringPool {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            index: Vec::new(),
        }
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            buf: Vec::with_capacity(cap * 32),
            index: Vec::with_capacity(cap),
        }
    }

    /// Append a string to the pool. Returns an ID for later retrieval.
    pub fn alloc(&mut self, s: &str) -> StrId {
        let id = StrId(self.index.len() as u32);
        let offset = self.buf.len() as u32;
        self.buf.extend_from_slice(s.as_bytes());
        self.index.push((offset, s.len() as u32));
        id
    }

    /// Retrieve a string by ID.
    #[inline]
    pub fn get(&self, id: StrId) -> &str {
        let (offset, len) = self.index[id.0 as usize];
        let bytes = &self.buf[offset as usize..(offset + len) as usize];
        // alloc() only accepts &str (valid UTF-8), and buf is append-only.
        std::str::from_utf8(bytes).expect("StringPool: invalid UTF-8")
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    pub fn is_empty(&self) -> bool {
        self.index.is_empty()
    }
}

// ── Pool-backed graph types ─────────────────────────────────────

/// Pool-backed definition. Stored in `CodeGraph.defs`.
///
/// Replaces `CanonicalDefinition` for graph storage. All strings are
/// [`StrId`] referencing the graph's [`StringPool`].
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

/// Pool-backed definition metadata.
#[derive(Debug, Clone, Default)]
pub struct GraphDefMeta {
    pub super_types: SmallVec<[StrId; 2]>,
    pub return_type: Option<StrId>,
    pub type_annotation: Option<StrId>,
    pub receiver_type: Option<StrId>,
    pub decorators: SmallVec<[StrId; 2]>,
    pub companion_of: Option<StrId>,
}

/// Pool-backed import. Stored in `CodeGraph.imports`.
#[derive(Debug, Clone)]
pub struct GraphImport {
    pub import_type: &'static str,
    pub path: StrId,
    pub name: Option<StrId>,
    pub alias: Option<StrId>,
    pub range: Range,
    pub wildcard: bool,
}

// ── Conversion from parser types ────────────────────────────────

impl GraphDef {
    /// Convert from parser's `CanonicalDefinition`, allocating strings into pool.
    pub fn from_canonical(
        def: &code_graph_types::CanonicalDefinition,
        pool: &mut StringPool,
    ) -> Self {
        let metadata = def.metadata.as_ref().map(|m| {
            Box::new(GraphDefMeta {
                super_types: m.super_types.iter().map(|s| pool.alloc(s)).collect(),
                return_type: m.return_type.as_deref().map(|s| pool.alloc(s)),
                type_annotation: m.type_annotation.as_deref().map(|s| pool.alloc(s)),
                receiver_type: m.receiver_type.as_deref().map(|s| pool.alloc(s)),
                decorators: m.decorators.iter().map(|s| pool.alloc(s)).collect(),
                companion_of: m.companion_of.as_deref().map(|s| pool.alloc(s)),
            })
        });
        Self {
            definition_type: def.definition_type,
            kind: def.kind,
            name: pool.alloc(&def.name),
            fqn: pool.alloc(&def.fqn.to_string()),
            fqn_sep: def.fqn.separator(),
            range: def.range,
            is_top_level: def.is_top_level,
            metadata,
        }
    }
}

impl GraphImport {
    /// Convert from parser's `CanonicalImport`, allocating strings into pool.
    pub fn from_canonical(imp: &code_graph_types::CanonicalImport, pool: &mut StringPool) -> Self {
        Self {
            import_type: imp.import_type,
            path: pool.alloc(&imp.path),
            name: imp.name.as_deref().map(|s| pool.alloc(s)),
            alias: imp.alias.as_deref().map(|s| pool.alloc(s)),
            range: imp.range,
            wildcard: imp.wildcard,
        }
    }
}

// ── SSA types ───────────────────────────────────────────────────

/// Identifier for a basic block in the SSA graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct BlockId(pub usize);

impl std::fmt::Display for BlockId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "b{}", self.0)
    }
}

/// Identifier for a phi node in the SSA graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PhiId(pub usize);

/// A value in the SSA graph — what a variable resolves to.
///
/// All string data (`Type`, `Alias`) is backed by [`FileArena`] — no
/// `Intern<str>`, no global RwLock, no memory leak.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Value<'a> {
    /// A definition node in the graph.
    Def(NodeIndex),
    /// An import node in the graph.
    Import(NodeIndex),
    /// A type name (for type-flow: resolve members on this type).
    Type(&'a str),
    /// Deferred name resolution: "I'm whatever this name resolves to."
    Alias(&'a str),
    /// Dead end — parameter, literal, or otherwise unresolvable.
    Opaque,
    /// Internal: cycle-detection sentinel for the marker algorithm.
    Marker,
    /// Internal: a phi node (resolved to concrete values).
    Phi(PhiId),
}

impl<'a> Value<'a> {
    /// Create a Type value from a string slice (arena-backed).
    pub fn type_of(s: &'a str) -> Self {
        Self::Type(s)
    }
}

/// The concrete values a variable resolves to at a given program point.
#[derive(Debug, Clone, Default)]
pub struct ReachingDefs<'a> {
    pub values: SmallVec<[Value<'a>; 2]>,
}

impl ReachingDefs<'_> {
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

// ── Arenas ──────────────────────────────────────────────────────

/// Per-language arena for graph-lifetime strings.
///
/// Wraps a [`bumpalo::Bump`] allocator. All strings allocated here live
/// as long as the `CodeGraph` that references them. Allocated during Phase 1
/// (under Mutex), read-only during Phase 2.
///
/// # What goes here
///
/// - `CanonicalDefinition.name` → `&'arena str`
/// - `CanonicalDefinition.fqn` parts → `&'arena str`
/// - `CanonicalImport.path`, `.name`, `.alias` → `&'arena str`
/// - `DefinitionMetadata.super_types`, `.return_type`, `.type_annotation` → `&'arena str`
/// - Directory/file path strings in `GraphNode` variants
///
/// # Lifecycle
///
/// ```text
/// let arena = GraphArena::new();           // pipeline start
/// let graph = LinkerGraph::new(&arena);    // graph borrows 'arena
/// // Phase 1: arena.alloc_str() under Mutex
/// // Phase 2: &arena.0 is Send+Sync, read-only
/// // pipeline end: drop graph, then drop arena
/// ```
pub struct GraphArena(Bump);

impl GraphArena {
    pub fn new() -> Self {
        Self(Bump::new())
    }

    /// Allocate with a capacity hint (bytes). Reduces early reallocations
    /// for large repos. Rule of thumb: ~50 bytes per file for names/paths.
    pub fn with_capacity(bytes: usize) -> Self {
        Self(Bump::with_capacity(bytes))
    }

    /// Copy a string into the arena, returning a reference that lives as
    /// long as the arena.
    #[inline]
    pub fn alloc_str(&self, s: &str) -> &str {
        self.0.alloc_str(s)
    }

    /// Allocate a string by formatting into the arena. Avoids a temporary
    /// `String` allocation for constructed FQNs like `"{parent}.{name}"`.
    pub fn alloc_fmt(&self, args: std::fmt::Arguments<'_>) -> &str {
        use std::fmt::Write;
        let mut w = BumpString::new_in(&self.0);
        w.write_fmt(args).expect("fmt into bump");
        w.into_bump_str()
    }

    /// Total bytes allocated by this arena (including waste from alignment).
    pub fn allocated_bytes(&self) -> usize {
        self.0.allocated_bytes()
    }
}

impl Default for GraphArena {
    fn default() -> Self {
        Self::new()
    }
}

/// Per-file arena for walker scratch strings.
///
/// Wraps a [`bumpalo::Bump`] allocator. Thread-local, created at Phase 2
/// file start, dropped wholesale when the walk completes. Output from the
/// walk (`Vec<(NodeIndex, NodeIndex, GraphEdge)>`) contains no arena refs,
/// so `'file` never escapes.
///
/// # What goes here
///
/// - `import_name_map` keys and values → `&'file str`
/// - `import_map` keys → `&'file str`
/// - `nested_cache` keys → `&'file str`
/// - `scope_stack[].name` → `&'file str`
/// - Constructed FQN candidates during resolution → `&'file str`
/// - `Value::Type` / `Value::Alias` name strings → `&'file str`
///
/// # Lifecycle
///
/// ```text
/// // Inside par_iter (one per rayon thread):
/// let file_arena = FileArena::new();
/// let walker = FileWalker::new(&graph, &file_arena);
/// walker.walk(&root);
/// let result = walker.into_result();  // no &'file refs
/// drop(file_arena);                   // one free(), all strings gone
/// ```
pub struct FileArena(Bump);

impl FileArena {
    pub fn new() -> Self {
        Self(Bump::new())
    }

    /// Allocate with a capacity hint. Rule of thumb: ~4KB per file covers
    /// scope names, cache keys, and constructed FQNs for typical files.
    pub fn with_capacity(bytes: usize) -> Self {
        Self(Bump::with_capacity(bytes))
    }

    /// Copy a string into the arena.
    #[inline]
    pub fn alloc_str(&self, s: &str) -> &str {
        self.0.alloc_str(s)
    }

    /// Allocate a string by formatting into the arena.
    pub fn alloc_fmt(&self, args: std::fmt::Arguments<'_>) -> &str {
        use std::fmt::Write;
        let mut w = BumpString::new_in(&self.0);
        w.write_fmt(args).expect("fmt into bump");
        w.into_bump_str()
    }

    /// Total bytes allocated by this arena.
    pub fn allocated_bytes(&self) -> usize {
        self.0.allocated_bytes()
    }

    /// Reset the arena for reuse (e.g. processing another file on the same
    /// thread without reallocating the backing storage). All references
    /// previously returned by `alloc_str` / `alloc_fmt` become invalid —
    /// the caller must ensure nothing borrows them.
    pub fn reset(&mut self) {
        self.0.reset();
    }
}

impl Default for FileArena {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ───────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // ── VerifiedMap ─────────────────────────────────────────

    #[test]
    fn verified_map_insert_and_lookup() {
        let mut map = VerifiedMap::<8>::new();
        let n0 = NodeIndex::new(0);
        let n1 = NodeIndex::new(1);

        map.insert("foo", n0);
        map.insert("bar", n1);

        let result = map.lookup("foo", |idx| idx == n0);
        assert_eq!(result.as_slice(), &[n0]);

        let result = map.lookup("bar", |idx| idx == n1);
        assert_eq!(result.as_slice(), &[n1]);
    }

    #[test]
    fn verified_map_multiple_values_same_key() {
        let mut map = VerifiedMap::<8>::new();
        let n0 = NodeIndex::new(0);
        let n1 = NodeIndex::new(1);

        map.insert("foo", n0);
        map.insert("foo", n1);

        // Verifier accepts both
        let result = map.lookup("foo", |_| true);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&n0));
        assert!(result.contains(&n1));

        // Verifier filters
        let result = map.lookup("foo", |idx| idx == n1);
        assert_eq!(result.as_slice(), &[n1]);
    }

    #[test]
    fn verified_map_miss_returns_empty() {
        let map = VerifiedMap::<8>::new();
        let result = map.lookup("missing", |_| true);
        assert!(result.is_empty());
    }

    #[test]
    fn verified_map_contains_is_conservative() {
        let mut map = VerifiedMap::<8>::new();
        map.insert("foo", NodeIndex::new(0));

        assert!(map.contains("foo"));
        assert!(!map.contains("bar"));
    }

    #[test]
    fn verified_map_lookup_into_appends() {
        let mut map = VerifiedMap::<8>::new();
        let n0 = NodeIndex::new(0);
        let n1 = NodeIndex::new(1);
        map.insert("foo", n0);
        map.insert("foo", n1);

        let mut out = vec![NodeIndex::new(99)]; // pre-existing
        let found = map.lookup_into("foo", |_| true, &mut out);
        assert!(found);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], NodeIndex::new(99));
        assert!(out.contains(&n0));
        assert!(out.contains(&n1));
    }

    #[test]
    fn verified_map_lookup_into_returns_false_on_miss() {
        let map = VerifiedMap::<8>::new();
        let mut out = Vec::new();
        let found = map.lookup_into("missing", |_| true, &mut out);
        assert!(!found);
        assert!(out.is_empty());
    }

    #[test]
    fn verified_map_verifier_rejects_all() {
        let mut map = VerifiedMap::<8>::new();
        map.insert("foo", NodeIndex::new(0));
        map.insert("foo", NodeIndex::new(1));

        let result = map.lookup("foo", |_| false);
        assert!(result.is_empty());

        let mut out = Vec::new();
        let found = map.lookup_into("foo", |_| false, &mut out);
        assert!(!found);
    }

    // ── NestedMap ───────────────────────────────────────────

    #[test]
    fn nested_map_insert_and_lookup() {
        let mut map = NestedMap::new();
        let n0 = NodeIndex::new(0);

        map.insert("Foo", "bar", n0);

        let result = map.lookup("Foo", "bar", |idx| idx == n0);
        assert_eq!(result.as_slice(), &[n0]);
    }

    #[test]
    fn nested_map_different_scopes() {
        let mut map = NestedMap::new();
        let n0 = NodeIndex::new(0);
        let n1 = NodeIndex::new(1);

        map.insert("Foo", "method", n0);
        map.insert("Bar", "method", n1);

        let result = map.lookup("Foo", "method", |idx| idx == n0);
        assert_eq!(result.as_slice(), &[n0]);

        let result = map.lookup("Bar", "method", |idx| idx == n1);
        assert_eq!(result.as_slice(), &[n1]);
    }

    #[test]
    fn nested_map_miss_scope() {
        let mut map = NestedMap::new();
        map.insert("Foo", "bar", NodeIndex::new(0));

        let result = map.lookup("Missing", "bar", |_| true);
        assert!(result.is_empty());
    }

    #[test]
    fn nested_map_miss_member() {
        let mut map = NestedMap::new();
        map.insert("Foo", "bar", NodeIndex::new(0));

        let result = map.lookup("Foo", "missing", |_| true);
        assert!(result.is_empty());
    }

    #[test]
    fn nested_map_lookup_into_appends() {
        let mut map = NestedMap::new();
        let n0 = NodeIndex::new(0);
        map.insert("Foo", "bar", n0);

        let mut out = vec![NodeIndex::new(99)];
        let found = map.lookup_into("Foo", "bar", |_| true, &mut out);
        assert!(found);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], NodeIndex::new(99));
        assert_eq!(out[1], n0);
    }

    #[test]
    fn nested_map_verifier_filters() {
        let mut map = NestedMap::new();
        let n0 = NodeIndex::new(0);
        let n1 = NodeIndex::new(1);
        map.insert("Foo", "bar", n0);
        map.insert("Foo", "bar", n1);

        let result = map.lookup("Foo", "bar", |idx| idx == n1);
        assert_eq!(result.as_slice(), &[n1]);
    }

    // ── GraphArena ───────────────────────────────────────────

    #[test]
    fn graph_arena_alloc_str() {
        let arena = GraphArena::new();
        let s1 = arena.alloc_str("UserService");
        let s2 = arena.alloc_str("get_user");
        assert_eq!(s1, "UserService");
        assert_eq!(s2, "get_user");
        // Different allocations, different pointers
        assert!(!std::ptr::eq(s1.as_ptr(), s2.as_ptr()));
    }

    #[test]
    fn graph_arena_alloc_str_deduplicates_nothing() {
        // Arena does NOT deduplicate — two allocs of the same string
        // produce independent copies. This is intentional: dedup would
        // require a lookup table (IStr's problem). Arena trades memory
        // for zero-lock, zero-fragmentation allocation.
        let arena = GraphArena::new();
        let s1 = arena.alloc_str("Foo");
        let s2 = arena.alloc_str("Foo");
        assert_eq!(s1, s2);
        assert!(!std::ptr::eq(s1.as_ptr(), s2.as_ptr()));
    }

    #[test]
    fn graph_arena_alloc_fmt() {
        let arena = GraphArena::new();
        let parent = "com.example";
        let name = "UserService";
        let fqn = arena.alloc_fmt(format_args!("{}.{}", parent, name));
        assert_eq!(fqn, "com.example.UserService");
    }

    #[test]
    fn graph_arena_allocated_bytes_grows() {
        let arena = GraphArena::new();
        let before = arena.allocated_bytes();
        arena.alloc_str("a]reasonably long string that forces allocation");
        let after = arena.allocated_bytes();
        assert!(after > before);
    }

    #[test]
    fn graph_arena_with_capacity() {
        let arena = GraphArena::with_capacity(1024 * 1024); // 1MB
        // Should not panic, pre-allocates backing storage
        let s = arena.alloc_str("test");
        assert_eq!(s, "test");
    }

    // ── FileArena ───────────────────────────────────────────

    #[test]
    fn file_arena_basic() {
        let arena = FileArena::new();
        let s = arena.alloc_str("scope_name");
        assert_eq!(s, "scope_name");
    }

    #[test]
    fn file_arena_alloc_fmt() {
        let arena = FileArena::new();
        let key = arena.alloc_fmt(format_args!("{}::{}", "Foo", "bar"));
        assert_eq!(key, "Foo::bar");
    }

    #[test]
    fn file_arena_reset() {
        let mut arena = FileArena::new();
        arena.alloc_str("first file strings");
        let bytes_before = arena.allocated_bytes();
        assert!(bytes_before > 0);

        arena.reset();
        // After reset, backing storage is retained but contents are gone.
        // New allocations reuse the same memory.
        let s = arena.alloc_str("second file");
        assert_eq!(s, "second file");
    }

    #[test]
    fn file_arena_many_small_allocs() {
        let arena = FileArena::new();
        let mut refs = Vec::new();
        for i in 0..1000 {
            let s = arena.alloc_fmt(format_args!("name_{}", i));
            refs.push(s);
        }
        assert_eq!(refs[0], "name_0");
        assert_eq!(refs[999], "name_999");
        assert_eq!(refs.len(), 1000);
    }

    // ── GraphIndexes ────────────────────────────────────────

    #[test]
    fn graph_indexes_construction_lifecycle() {
        let mut indexes = GraphIndexes::new();

        assert!(indexes.dir_index.is_some());
        assert!(indexes.file_index.is_some());

        indexes
            .dir_index
            .as_mut()
            .unwrap()
            .insert("src".to_string(), NodeIndex::new(0));
        indexes
            .file_index
            .as_mut()
            .unwrap()
            .insert("src/main.py".to_string(), NodeIndex::new(1));

        indexes.drop_construction_indexes();
        assert!(indexes.dir_index.is_none());
        assert!(indexes.file_index.is_none());
    }

    #[test]
    fn graph_indexes_all_maps_independent() {
        let mut indexes = GraphIndexes::new();
        let n0 = NodeIndex::new(0);
        let n1 = NodeIndex::new(1);
        let n2 = NodeIndex::new(2);

        indexes.by_fqn.insert("com.Foo", n0);
        indexes.by_name.insert("Foo", n1);
        indexes.nested.insert("com.Foo", "bar", n2);

        assert_eq!(indexes.by_fqn.lookup("com.Foo", |_| true).len(), 1);
        assert_eq!(indexes.by_name.lookup("Foo", |_| true).len(), 1);
        assert_eq!(indexes.nested.lookup("com.Foo", "bar", |_| true).len(), 1);

        // Cross-check: different maps don't interfere
        assert!(indexes.by_fqn.lookup("Foo", |_| true).is_empty());
        assert!(indexes.by_name.lookup("com.Foo", |_| true).is_empty());
    }

    // ── StringPool ───────────────────────────────────────────

    #[test]
    fn string_pool_alloc_and_get() {
        let mut pool = StringPool::new();
        let id = pool.alloc("hello");
        assert_eq!(pool.get(id), "hello");
    }

    #[test]
    fn string_pool_multiple() {
        let mut pool = StringPool::new();
        let a = pool.alloc("foo");
        let b = pool.alloc("bar");
        let c = pool.alloc("baz");
        assert_eq!(pool.get(a), "foo");
        assert_eq!(pool.get(b), "bar");
        assert_eq!(pool.get(c), "baz");
        assert_eq!(pool.len(), 3);
    }

    #[test]
    fn string_pool_duplicates_not_deduped() {
        let mut pool = StringPool::new();
        let a = pool.alloc("same");
        let b = pool.alloc("same");
        assert_ne!(a, b);
        assert_eq!(pool.get(a), pool.get(b));
    }

    // ── GraphDef / GraphImport ──────────────────────────────

    #[test]
    fn graph_def_from_canonical() {
        use code_graph_types::*;

        let mut pool = StringPool::new();
        let cdef = CanonicalDefinition {
            definition_type: "Class",
            kind: DefKind::Class,
            name: "UserService".to_string(),
            fqn: Fqn::from_parts(&["com", "example", "UserService"], "."),
            range: Range::new(Position::new(1, 0), Position::new(50, 0), (0, 1000)),
            is_top_level: true,
            metadata: Some(Box::new(DefinitionMetadata {
                super_types: vec!["BaseService".to_string()],
                return_type: None,
                type_annotation: None,
                receiver_type: None,
                decorators: vec![],
                companion_of: None,
            })),
        };
        let gdef = GraphDef::from_canonical(&cdef, &mut pool);

        assert_eq!(pool.get(gdef.name), "UserService");
        assert_eq!(pool.get(gdef.fqn), "com.example.UserService");
        assert_eq!(gdef.kind, DefKind::Class);
        assert!(gdef.is_top_level);
        let meta = gdef.metadata.as_ref().unwrap();
        assert_eq!(pool.get(meta.super_types[0]), "BaseService");
    }

    #[test]
    fn graph_import_from_canonical() {
        use code_graph_types::*;

        let mut pool = StringPool::new();
        let cimp = CanonicalImport {
            import_type: "FromImport",
            path: "app.services".to_string(),
            name: Some("AuthService".to_string()),
            alias: Some("Auth".to_string()),
            scope_fqn: None,
            range: Range::new(Position::new(1, 0), Position::new(1, 30), (0, 30)),
            wildcard: false,
        };
        let gimp = GraphImport::from_canonical(&cimp, &mut pool);

        assert_eq!(pool.get(gimp.path), "app.services");
        assert_eq!(pool.get(gimp.name.unwrap()), "AuthService");
        assert_eq!(pool.get(gimp.alias.unwrap()), "Auth");
        assert!(!gimp.wildcard);
    }
}
