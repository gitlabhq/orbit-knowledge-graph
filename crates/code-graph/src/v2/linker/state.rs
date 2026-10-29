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
//! ## String storage
//!
//! Three tiers eliminate per-string heap allocation and Drop overhead:
//!
//! - **[`StringPool`]**: graph-level, contiguous `Vec<u8>` buffer. Owns all
//!   definition names, FQN strings, import paths, metadata strings. Accessed
//!   via [`StrId`]. Dropped with the graph.
//!
//! - **[`FileArena`]** (`'file`): per-file, thread-local bump allocator.
//!   Holds walker scratch strings (scope names, cache keys, constructed FQNs).
//!   Created at Phase 2 file start, dropped wholesale when `FusedWalkResult`
//!   is returned. Output contains no arena refs, so `'file` never escapes.
//!
//! - **[`ScratchBuf`]**: reusable heap `String` for transient lookup keys.
//!   Allocated once per walker, reused via `clear()` + `write!()`. For
//!   strings that are used once for a lookup and immediately discarded.

use std::hash::{Hash, Hasher};

use crate::v2::linker::graph::DefId;
use crate::v2::types::{
    DefKind, DefScope, DefinitionMetadata, ImportBindingKind, ImportMode, Modifiers, Range,
    Visibility,
};
use bumpalo::Bump;
use petgraph::graph::NodeIndex;
use rust_lapper::{Interval, Lapper};
use rustc_hash::{FxHashMap, FxHasher};
use smallvec::SmallVec;

use bumpalo::collections::String as BumpString;

/// Hashes a string into a `u64` index key (FxHash, for speed). Also public for
/// `ssa_names` membership checks in the walker, where a collision only costs
/// extra work, never wrong edges.
#[inline]
pub fn hash_name(s: &str) -> u64 {
    let mut h = FxHasher::default();
    s.hash(&mut h);
    h.finish()
}

/// Intern-keyed index map. Keys are `StrId` from the graph's `StringPool`,
/// so lookups are integer-keyed HashMap probes with no string comparison.
pub struct VerifiedMap<const N: usize = 8> {
    inner: FxHashMap<StrId, SmallVec<[NodeIndex; N]>>,
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

    pub fn insert(&mut self, key: StrId, value: NodeIndex) {
        self.inner.entry(key).or_default().push(value);
    }

    pub fn lookup(&self, key: StrId) -> &[NodeIndex] {
        match self.inner.get(&key) {
            Some(candidates) => candidates.as_slice(),
            None => &[],
        }
    }

    pub fn sort_all(&mut self, key_fn: impl Fn(NodeIndex) -> String) {
        for entries in self.inner.values_mut() {
            entries.sort_by_cached_key(|idx| key_fn(*idx));
        }
    }

    pub fn lookup_into(&self, key: StrId, out: &mut Vec<NodeIndex>) -> bool {
        let Some(candidates) = self.inner.get(&key) else {
            return false;
        };
        if candidates.is_empty() {
            return false;
        }
        out.extend_from_slice(candidates);
        true
    }

    pub fn contains(&self, key: StrId) -> bool {
        self.inner.contains_key(&key)
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

/// Two-level intern-keyed index map for scope → member lookups.
pub struct NestedMap {
    inner: FxHashMap<StrId, FxHashMap<StrId, SmallVec<[NodeIndex; 8]>>>,
}

impl NestedMap {
    pub fn new() -> Self {
        Self {
            inner: FxHashMap::default(),
        }
    }

    pub fn insert(&mut self, scope: StrId, member: StrId, value: NodeIndex) {
        self.inner
            .entry(scope)
            .or_default()
            .entry(member)
            .or_default()
            .push(value);
    }

    pub fn lookup(&self, scope: StrId, member: StrId) -> &[NodeIndex] {
        let Some(inner) = self.inner.get(&scope) else {
            return &[];
        };
        let Some(candidates) = inner.get(&member) else {
            return &[];
        };
        candidates.as_slice()
    }

    pub fn contains_scope(&self, scope: StrId) -> bool {
        self.inner.contains_key(&scope)
    }

    pub fn lookup_into(&self, scope: StrId, member: StrId, out: &mut Vec<NodeIndex>) -> bool {
        let Some(inner) = self.inner.get(&scope) else {
            return false;
        };
        let Some(candidates) = inner.get(&member) else {
            return false;
        };
        if candidates.is_empty() {
            return false;
        }
        out.extend_from_slice(candidates);
        true
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

pub struct DefinitionRangeIndex {
    lapper: Lapper<u64, NodeIndex>,
}

impl DefinitionRangeIndex {
    pub fn new() -> Self {
        Self {
            lapper: Lapper::new(Vec::new()),
        }
    }

    pub fn from_ranges(ranges: impl IntoIterator<Item = (Range, NodeIndex)>) -> Self {
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

    pub fn find_enclosing(&self, start: u32, end: u32) -> Option<NodeIndex> {
        self.lapper
            .find(start as u64, end as u64)
            .filter(|interval| interval.start <= start as u64 && end as u64 <= interval.stop)
            .min_by_key(|interval| interval.stop.saturating_sub(interval.start))
            .map(|interval| interval.val)
    }

    pub fn find_enclosing_or_overlapping(&self, start: u32, end: u32) -> Option<NodeIndex> {
        self.find_enclosing(start, end).or_else(|| {
            self.lapper
                .find(start as u64, end as u64)
                .min_by_key(|interval| interval.stop.saturating_sub(interval.start))
                .map(|interval| interval.val)
        })
    }
}

impl Default for DefinitionRangeIndex {
    fn default() -> Self {
        Self::new()
    }
}

/// All resolution indexes for a CodeGraph, bundled together.
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
    /// File path → definition range index for source/target lookup.
    pub definition_ranges: FxHashMap<String, DefinitionRangeIndex>,
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
            definition_ranges: FxHashMap::default(),
        }
    }

    pub fn drop_construction_indexes(&mut self) {
        self.dir_index = None;
        self.file_index = None;
    }

    /// Free the resolution-only indexes; the Arrow converter never reads them.
    pub fn drop_resolution_indexes(&mut self) {
        self.by_fqn = VerifiedMap::new();
        self.by_name = VerifiedMap::new();
        self.nested = NestedMap::new();
        self.ancestors = FxHashMap::default();
        self.definition_ranges = FxHashMap::default();
    }
}

impl Default for GraphIndexes {
    fn default() -> Self {
        Self::new()
    }
}

pub use orbit_utils::strings::{StrId, StringPool};

/// Pool-backed definition. All strings are [`StrId`] referencing the graph's
/// [`StringPool`].
#[derive(Debug, Clone)]
pub struct GraphDef {
    pub definition_type: &'static str,
    pub kind: DefKind,
    pub name: StrId,
    pub fqn: StrId,
    pub fqn_sep: &'static str,
    pub range: Range,
    pub is_top_level: bool,
    pub scope: DefScope,
    pub metadata: Option<Box<GraphDefMeta>>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct GraphDefMeta {
    pub super_types: SmallVec<[StrId; 2]>,
    pub return_type: Option<StrId>,
    pub type_annotation: Option<StrId>,
    pub receiver_type: Option<StrId>,
    pub decorators: SmallVec<[StrId; 2]>,
    pub companion_of: Option<StrId>,
    pub is_exported: bool,
}

#[derive(Debug, Default)]
pub struct DisplayTable {
    text: String,
    ends: Vec<u32>,
    rows: Vec<DisplayRow>,
}

#[derive(Debug, Clone, Copy)]
struct DisplayRow {
    first: u32,
    parameters: u8,
    present: u8,
    modifiers: Modifiers,
    visibility: Option<Visibility>,
}

#[derive(Clone, Copy)]
pub struct DefDisplay<'a> {
    table: &'a DisplayTable,
    row: DisplayRow,
}

const SIGNATURE: u32 = 0;
const RETURN_TYPE: u32 = 1;
const DECLARED_TYPE: u32 = 2;
const DOC_COMMENT: u32 = 3;
const GENERATED_BY: u32 = 4;
const FIELDS: u32 = 5;

impl DisplayTable {
    pub fn push(&mut self, meta: Option<&DefinitionMetadata>) {
        let first = self.ends.len() as u32;
        let mut present = 0;
        let fields = meta.map(|m| {
            [
                &m.signature,
                &m.declared_return_type,
                &m.declared_type,
                &m.doc_comment,
                &m.generated_by,
            ]
        });
        for (bit, field) in fields.into_iter().flatten().enumerate() {
            if let Some(text) = field {
                present |= 1 << bit;
                self.text.push_str(text);
            }
            self.ends.push(self.text.len() as u32);
        }
        let parameters = meta.map_or(&[][..], |m| &m.parameters[..]);
        for parameter in parameters {
            self.text.push_str(parameter);
            self.ends.push(self.text.len() as u32);
        }
        self.rows.push(DisplayRow {
            first,
            parameters: parameters.len() as u8,
            present,
            modifiers: meta.map(|m| m.modifiers).unwrap_or_default(),
            visibility: meta.and_then(|m| m.visibility),
        });
    }

    pub fn get(&self, id: DefId) -> DefDisplay<'_> {
        DefDisplay {
            table: self,
            row: self.rows[id.0 as usize],
        }
    }

    fn span(&self, index: u32) -> &str {
        let start = index.checked_sub(1).map_or(0, |i| self.ends[i as usize]);
        &self.text[start as usize..self.ends[index as usize] as usize]
    }
}

impl<'a> DefDisplay<'a> {
    fn field(&self, field: u32) -> Option<&'a str> {
        (self.row.present & (1 << field) != 0).then(|| self.table.span(self.row.first + field))
    }

    pub fn signature(&self) -> Option<&'a str> {
        self.field(SIGNATURE)
    }

    pub fn return_type(&self) -> Option<&'a str> {
        self.field(RETURN_TYPE)
    }

    pub fn declared_type(&self) -> Option<&'a str> {
        self.field(DECLARED_TYPE)
    }

    pub fn doc_comment(&self) -> Option<&'a str> {
        self.field(DOC_COMMENT)
    }

    pub fn generated_by(&self) -> Option<&'a str> {
        self.field(GENERATED_BY)
    }

    pub fn parameters(&self) -> impl Iterator<Item = &'a str> {
        let first = self.row.first + FIELDS;
        let table = self.table;
        (first..first + self.row.parameters as u32).map(move |i| table.span(i))
    }

    pub fn modifiers(&self) -> Modifiers {
        self.row.modifiers
    }

    pub fn visibility(&self) -> Option<Visibility> {
        self.row.visibility
    }
}

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
    pub is_external: bool,
}

impl GraphDef {
    pub fn from_canonical(
        def: &crate::v2::types::CanonicalDefinition,
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
                is_exported: m.is_exported,
            })
        });
        let metadata = metadata.filter(|m| **m != GraphDefMeta::default());
        Self {
            definition_type: def.definition_type,
            kind: def.kind,
            name: pool.alloc(&def.name),
            fqn: pool.alloc(def.fqn.as_str()),
            fqn_sep: def.fqn.separator(),
            range: def.range,
            is_top_level: def.is_top_level,
            scope: DefScope::TopLevel,
            metadata,
        }
    }
}

impl GraphImport {
    pub fn target_fqn(&self, pool: &StringPool, sep: &str) -> String {
        let path = pool.get(self.path);
        let name = self.name.map(|n| pool.get(n));
        match (self.import_kind(path.is_empty()), name) {
            ("symbol", Some(name)) if !path.is_empty() && !path_names(path, name) => {
                format!("{path}{sep}{name}")
            }
            (_, Some(name)) if path.is_empty() => name.to_string(),
            _ => path.to_string(),
        }
    }

    pub fn import_kind(&self, path_is_empty: bool) -> &'static str {
        match self.binding_kind {
            _ if self.wildcard => "wildcard",
            ImportBindingKind::SideEffect => "side_effect",
            ImportBindingKind::Namespace => "module",
            ImportBindingKind::Primary => "symbol",
            ImportBindingKind::Named if self.name.is_none() || path_is_empty => "module",
            ImportBindingKind::Named => "symbol",
        }
    }

    pub fn from_canonical(imp: &crate::v2::types::CanonicalImport, pool: &mut StringPool) -> Self {
        Self {
            import_type: imp.import_type,
            binding_kind: imp.binding_kind,
            mode: imp.mode,
            path: pool.alloc(&imp.path),
            name: imp.name.as_deref().map(|s| pool.alloc(s)),
            alias: imp.alias.as_deref().map(|s| pool.alloc(s)),
            range: imp.range,
            is_type_only: imp.is_type_only,
            wildcard: imp.wildcard,
            is_external: false,
        }
    }
}

fn path_names(path: &str, name: &str) -> bool {
    path.strip_suffix(name).is_some_and(|head| {
        head.is_empty() || head.ends_with(|c: char| !c.is_alphanumeric() && c != '_')
    })
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

    #[inline]
    pub fn alloc_str(&self, s: &str) -> &str {
        self.0.alloc_str(s)
    }

    pub fn alloc_fmt(&self, args: std::fmt::Arguments<'_>) -> &str {
        use std::fmt::Write;
        let mut w = BumpString::new_in(&self.0);
        w.write_fmt(args).expect("fmt into bump");
        w.into_bump_str()
    }

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

pub use orbit_utils::strings::ScratchBuf;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn verified_map_insert_and_lookup() {
        let mut pool = StringPool::new();
        let foo = pool.alloc("foo");
        let bar = pool.alloc("bar");
        let mut map = VerifiedMap::<8>::new();
        let n0 = NodeIndex::new(0);
        let n1 = NodeIndex::new(1);
        map.insert(foo, n0);
        map.insert(bar, n1);

        assert_eq!(map.lookup(foo), &[n0]);
        assert_eq!(map.lookup(bar), &[n1]);
    }

    #[test]
    fn verified_map_multiple_values_same_key() {
        let mut pool = StringPool::new();
        let foo = pool.alloc("foo");
        let mut map = VerifiedMap::<8>::new();
        let n0 = NodeIndex::new(0);
        let n1 = NodeIndex::new(1);
        map.insert(foo, n0);
        map.insert(foo, n1);

        let result = map.lookup(foo);
        assert_eq!(result.len(), 2);
        assert!(result.contains(&n0));
        assert!(result.contains(&n1));
    }

    #[test]
    fn verified_map_miss_returns_empty() {
        let mut pool = StringPool::new();
        let missing = pool.alloc("missing");
        let map = VerifiedMap::<8>::new();
        assert!(map.lookup(missing).is_empty());
    }

    #[test]
    fn verified_map_contains() {
        let mut pool = StringPool::new();
        let foo = pool.alloc("foo");
        let bar = pool.alloc("bar");
        let mut map = VerifiedMap::<8>::new();
        map.insert(foo, NodeIndex::new(0));

        assert!(map.contains(foo));
        assert!(!map.contains(bar));
    }

    #[test]
    fn verified_map_lookup_into_appends() {
        let mut pool = StringPool::new();
        let foo = pool.alloc("foo");
        let mut map = VerifiedMap::<8>::new();
        let n0 = NodeIndex::new(0);
        let n1 = NodeIndex::new(1);
        map.insert(foo, n0);
        map.insert(foo, n1);

        let mut out = vec![NodeIndex::new(99)];
        let found = map.lookup_into(foo, &mut out);
        assert!(found);
        assert_eq!(out.len(), 3);
        assert_eq!(out[0], NodeIndex::new(99));
        assert!(out.contains(&n0));
        assert!(out.contains(&n1));
    }

    #[test]
    fn verified_map_lookup_into_returns_false_on_miss() {
        let mut pool = StringPool::new();
        let missing = pool.alloc("missing");
        let map = VerifiedMap::<8>::new();
        let mut out = Vec::new();
        let found = map.lookup_into(missing, &mut out);
        assert!(!found);
        assert!(out.is_empty());
    }

    #[test]
    fn nested_map_insert_and_lookup() {
        let mut pool = StringPool::new();
        let foo = pool.alloc("Foo");
        let bar = pool.alloc("bar");
        let mut map = NestedMap::new();
        let n0 = NodeIndex::new(0);
        map.insert(foo, bar, n0);
        assert_eq!(map.lookup(foo, bar), &[n0]);
    }

    #[test]
    fn nested_map_different_scopes() {
        let mut pool = StringPool::new();
        let foo = pool.alloc("Foo");
        let bar_scope = pool.alloc("Bar");
        let method = pool.alloc("method");
        let mut map = NestedMap::new();
        let n0 = NodeIndex::new(0);
        let n1 = NodeIndex::new(1);
        map.insert(foo, method, n0);
        map.insert(bar_scope, method, n1);

        assert_eq!(map.lookup(foo, method), &[n0]);
        assert_eq!(map.lookup(bar_scope, method), &[n1]);
    }

    #[test]
    fn nested_map_miss_scope() {
        let mut pool = StringPool::new();
        let foo = pool.alloc("Foo");
        let bar = pool.alloc("bar");
        let missing = pool.alloc("Missing");
        let mut map = NestedMap::new();
        map.insert(foo, bar, NodeIndex::new(0));
        assert!(map.lookup(missing, bar).is_empty());
    }

    #[test]
    fn nested_map_miss_member() {
        let mut pool = StringPool::new();
        let foo = pool.alloc("Foo");
        let bar = pool.alloc("bar");
        let missing = pool.alloc("missing");
        let mut map = NestedMap::new();
        map.insert(foo, bar, NodeIndex::new(0));
        assert!(map.lookup(foo, missing).is_empty());
    }

    #[test]
    fn nested_map_lookup_into_appends() {
        let mut pool = StringPool::new();
        let foo = pool.alloc("Foo");
        let bar = pool.alloc("bar");
        let mut map = NestedMap::new();
        let n0 = NodeIndex::new(0);
        map.insert(foo, bar, n0);

        let mut out = vec![NodeIndex::new(99)];
        let found = map.lookup_into(foo, bar, &mut out);
        assert!(found);
        assert_eq!(out.len(), 2);
        assert_eq!(out[0], NodeIndex::new(99));
        assert_eq!(out[1], n0);
    }

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

        let mut pool = StringPool::new();
        let com_foo = pool.alloc("com.Foo");
        let foo = pool.alloc("Foo");
        let bar = pool.alloc("bar");

        indexes.by_fqn.insert(com_foo, n0);
        indexes.by_name.insert(foo, n1);
        indexes.nested.insert(com_foo, bar, n2);

        assert_eq!(indexes.by_fqn.lookup(com_foo).len(), 1);
        assert_eq!(indexes.by_name.lookup(foo).len(), 1);
        assert_eq!(indexes.nested.lookup(com_foo, bar).len(), 1);

        assert!(indexes.by_fqn.lookup(foo).is_empty());
        assert!(indexes.by_name.lookup(com_foo).is_empty());
    }

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
    fn string_pool_deduplicates() {
        let mut pool = StringPool::new();
        let a = pool.alloc("same");
        let b = pool.alloc("same");
        assert_eq!(a, b);
    }

    #[test]
    fn graph_def_from_canonical() {
        use crate::v2::types::*;

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
                is_exported: true,
                ..Default::default()
            })),
        };
        let gdef = GraphDef::from_canonical(&cdef, &mut pool);

        assert_eq!(pool.get(gdef.name), "UserService");
        assert_eq!(pool.get(gdef.fqn), "com.example.UserService");
        assert_eq!(gdef.kind, DefKind::Class);
        assert!(gdef.is_top_level);
        let meta = gdef.metadata.as_ref().unwrap();
        assert_eq!(pool.get(meta.super_types[0]), "BaseService");
        assert!(meta.is_exported);
    }

    #[test]
    fn graph_import_from_canonical() {
        use crate::v2::types::*;

        let mut pool = StringPool::new();
        let cimp = CanonicalImport {
            import_type: "FromImport",
            binding_kind: ImportBindingKind::Named,
            mode: ImportMode::Declarative,
            path: "app.services".to_string(),
            name: Some("AuthService".to_string()),
            alias: Some("Auth".to_string()),
            scope_fqn: None,
            range: Range::new(Position::new(1, 0), Position::new(1, 30), (0, 30)),
            is_type_only: true,
            wildcard: false,
        };
        let gimp = GraphImport::from_canonical(&cimp, &mut pool);

        assert_eq!(pool.get(gimp.path), "app.services");
        assert_eq!(pool.get(gimp.name.unwrap()), "AuthService");
        assert_eq!(pool.get(gimp.alias.unwrap()), "Auth");
        assert_eq!(gimp.binding_kind, ImportBindingKind::Named);
        assert_eq!(gimp.mode, ImportMode::Declarative);
        assert!(gimp.is_type_only);
        assert!(!gimp.wildcard);
    }
}
