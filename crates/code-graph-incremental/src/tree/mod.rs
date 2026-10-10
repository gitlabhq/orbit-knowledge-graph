mod cursor;
mod node;
mod print;
mod semantic;
mod storage;

use indextree::Arena;
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

pub use crate::edge::{CallResolution, Edge, EdgeKind};
pub use cursor::{Cursor, Step, Walk};
pub use node::{Node, Tag};
pub use print::pretty_print;
pub use semantic::{find_method_in, infer_return_type, members_by_level, reachable};
pub(crate) use storage::Entry as CompactNode;
pub use storage::{Compact, Mutable, Storage};

#[derive(Clone)]
pub struct Tree<S: Storage = Mutable> {
    pub storage: S,
    pub(crate) root: S::Id,
    pub label: String,
    pub tags: FxHashMap<u32, SmallVec<[Tag; 2]>>,
    pub source: std::sync::Arc<str>,
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
        }
    }

    pub fn new(root_node: N) -> Self {
        Self::with_capacity(1, root_node)
    }
}

impl<S: Storage> Tree<S> {
    pub fn set_tag(&mut self, node: u32, key: u32, val: u32) {
        let tags = self.tags.entry(node).or_default();
        if let Some(tag) = tags.iter_mut().find(|tag| tag.key == key) {
            tag.val = val;
        } else {
            tags.push(Tag { key, val });
        }
    }

    pub fn clear_tags(&mut self, node: u32, keys: &[u32]) {
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
