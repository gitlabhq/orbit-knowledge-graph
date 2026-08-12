//! Graph data types and per-file arena allocator.
//!
//! ## String storage
//!
//! - **[`StringPool`]**: graph-level, contiguous buffer. Owns all definition
//!   names, FQN strings, import paths, metadata strings. Accessed via [`StrId`].
//!
//! - **[`FileArena`]** (`'file`): per-file, thread-local bump allocator.
//!   Holds walker scratch strings (scope names, cache keys, constructed FQNs).
//!
//! - **[`ScratchBuf`]**: reusable heap `String` for transient lookup keys.

use crate::v2::types::{DefKind, ImportBindingKind, ImportMode, Range};
use bumpalo::Bump;
use smallvec::SmallVec;

use bumpalo::collections::String as BumpString;

pub use gkg_utils::strings::{ScratchBuf, StrId, StringPool};

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

/// Per-file arena for walker scratch strings.
///
/// Wraps a [`bumpalo::Bump`] allocator. Thread-local, created at Phase 2
/// file start, dropped wholesale when the walk completes.
pub struct FileArena(Bump);

impl FileArena {
    pub fn new() -> Self {
        Self(Bump::new())
    }

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

    pub fn reset(&mut self) {
        self.0.reset();
    }
}

impl Default for FileArena {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn string_pool_alloc_and_get() {
        let pool = StringPool::new();
        let id = pool.alloc("hello");
        assert_eq!(pool.get(id), "hello");
    }

    #[test]
    fn string_pool_multiple() {
        let pool = StringPool::new();
        let a = pool.alloc("foo");
        let b = pool.alloc("bar");
        let c = pool.alloc("baz");
        assert_eq!(pool.get(a), "foo");
        assert_eq!(pool.get(b), "bar");
        assert_eq!(pool.get(c), "baz");
        assert_eq!(pool.len(), 3);
    }

    #[test]
    fn graph_def_direct_construction() {
        use crate::v2::types::*;

        let pool = StringPool::new();
        let gdef = GraphDef {
            definition_type: "Class",
            kind: DefKind::Class,
            name: pool.alloc("UserService"),
            fqn: pool.alloc("com.example.UserService"),
            fqn_sep: ".",
            range: Range::new(Position::new(1, 0), Position::new(50, 0), (0, 1000)),
            is_top_level: true,
            metadata: Some(Box::new(GraphDefMeta {
                super_types: smallvec::smallvec![pool.alloc("BaseService")],
                is_exported: true,
                ..Default::default()
            })),
        };

        assert_eq!(pool.get(gdef.name), "UserService");
        assert_eq!(pool.get(gdef.fqn), "com.example.UserService");
        assert_eq!(gdef.kind, DefKind::Class);
        assert!(gdef.is_top_level);
        let meta = gdef.metadata.as_ref().unwrap();
        assert_eq!(pool.get(meta.super_types[0]), "BaseService");
        assert!(meta.is_exported);
    }

    #[test]
    fn graph_import_direct_construction() {
        use crate::v2::types::*;

        let pool = StringPool::new();
        let gimp = GraphImport {
            import_type: "FromImport",
            binding_kind: ImportBindingKind::Named,
            mode: ImportMode::Declarative,
            path: pool.alloc("app.services"),
            name: Some(pool.alloc("AuthService")),
            alias: Some(pool.alloc("Auth")),
            range: Range::new(Position::new(1, 0), Position::new(1, 30), (0, 30)),
            is_type_only: true,
            wildcard: false,
        };

        assert_eq!(pool.get(gimp.path), "app.services");
        assert_eq!(pool.get(gimp.name.unwrap()), "AuthService");
        assert_eq!(pool.get(gimp.alias.unwrap()), "Auth");
        assert_eq!(gimp.binding_kind, ImportBindingKind::Named);
        assert_eq!(gimp.mode, ImportMode::Declarative);
        assert!(gimp.is_type_only);
        assert!(!gimp.wildcard);
    }
}
