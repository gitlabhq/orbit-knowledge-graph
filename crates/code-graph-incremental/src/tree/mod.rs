mod cursor;
mod node;
#[cfg(feature = "pretty-print")]
mod print;
mod semantic;
pub use semantic::CLASS_LIKE;
mod storage;

use indextree::Arena;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

pub use crate::edge::{CallResolution, Edge, EdgeKind};
pub use cursor::{Cursor, Step, Walk};
pub use node::{Node, Tag};
#[cfg(feature = "pretty-print")]
pub use print::pretty_print;
pub use semantic::{
    Role, find_member_in, infer_return_type, is_name_text, members_by_level, members_named,
    reachable,
};
pub(crate) use storage::Entry as CompactNode;
pub use storage::{Compact, Mutable, Storage};

#[derive(Clone)]
pub struct Tree<S: Storage = Mutable> {
    pub storage: S,
    pub(crate) root: S::Id,
    pub label: String,
    pub tags: FxHashMap<u32, SmallVec<[Tag; 2]>>,
    pub source: std::sync::Arc<str>,
    pub(crate) indexes: IndexCache,
}

/// One declared index's entries in one tree: the matching nodes by
/// (anchor node, key text). The anchor is the scope node for a scope index
/// and 0 otherwise.
pub type Piece = FxHashMap<(u32, u32), Vec<u32>>;

/// This tree's piece of each declared index, built the first time a lookup
/// needs it and dropped when the tree changes.
#[derive(Clone, Default)]
pub struct IndexCache(std::sync::OnceLock<Box<[std::sync::OnceLock<Piece>]>>);

impl IndexCache {
    pub fn piece(&self, count: usize, id: usize, build: impl FnOnce() -> Piece) -> &Piece {
        self.0
            .get_or_init(|| (0..count).map(|_| std::sync::OnceLock::new()).collect())[id]
            .get_or_init(build)
    }

    pub fn clear(&mut self) {
        *self = Self::default();
    }
}

impl<N> Tree<Mutable<N>> {
    pub fn with_capacity(capacity: usize, root_node: N) -> Self {
        let mut arena = Arena::with_capacity(capacity);
        let root = arena.new_node(root_node);
        Self {
            storage: Mutable(arena),
            root,
            label: String::new(),
            tags: FxHashMap::default(),
            source: std::sync::Arc::from(""),
            indexes: Default::default(),
        }
    }

    pub fn new(root_node: N) -> Self {
        Self::with_capacity(1, root_node)
    }
}

impl<S: Storage> Tree<S> {
    pub fn set_tag(&mut self, node: u32, key: u32, val: u32) {
        self.indexes.clear();
        let tags = self.tags.entry(node).or_default();
        if let Some(tag) = tags.iter_mut().find(|tag| tag.key == key) {
            tag.val = val;
        } else {
            tags.push(Tag { key, val });
        }
    }

    pub fn clear_tags(&mut self, node: u32, keys: &[u32]) {
        self.indexes.clear();
        if let Some(tags) = self.tags.get_mut(&node) {
            tags.retain(|tag| !keys.contains(&tag.key));
        }
    }

    pub fn get_tag(&self, node: u32, key: u32) -> Option<u32> {
        self.tags
            .get(&node)
            .and_then(|tags| tags.iter().find(|tag| tag.key == key))
            .map(|tag| tag.val)
    }
}
