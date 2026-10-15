use indextree::{Arena, NodeId};

use super::Storage;
use crate::tree::{Node, Tree};

#[derive(Clone)]
pub struct Mutable<N = Node>(pub(crate) Arena<N>);

impl<N> Mutable<N> {
    pub(crate) fn index(id: NodeId) -> u32 {
        usize::from(id) as u32 - 1
    }
}

impl<N> Tree<Mutable<N>> {
    pub fn append(&mut self, parent: NodeId, node: N) -> NodeId {
        parent.append_value(node, &mut self.storage.0)
    }

    pub(crate) fn preorder(&self) -> Vec<NodeId> {
        self.root.descendants(&self.storage.0).collect()
    }
}

impl Tree {
    pub(crate) fn replace(&mut self, target: NodeId, replacements: Vec<NodeId>) {
        let field = self.node(target).field;
        if target == self.root && replacements.len() != 1 {
            for node in replacements {
                node.remove_subtree(&mut self.storage.0);
            }
            return;
        }
        if let Some(&first) = replacements.first() {
            self.node_mut(first).field = field;
        }
        if target == self.root {
            self.root = replacements[0];
        } else {
            for node in replacements {
                target.insert_before(node, &mut self.storage.0);
            }
        }
        target.remove_subtree(&mut self.storage.0);
    }

    pub fn compact(&mut self) {
        let tree = std::mem::replace(self, Self::new(Node::default()));
        *self = tree.compact_and_remap().into();
    }
}

impl<N> Storage for Mutable<N> {
    type Node = N;
    type Id = NodeId;

    fn index(id: NodeId) -> u32 {
        Self::index(id)
    }

    fn id(&self, index: u32) -> NodeId {
        let index = std::num::NonZeroUsize::new(index as usize + 1).expect("raw + 1 is nonzero");
        self.0
            .get_node_id_at(index)
            .expect("raw ids come from this arena")
    }

    fn is_removed(&self, id: NodeId) -> bool {
        id.is_removed(&self.0)
    }
    fn node(&self, id: u32) -> &N {
        self.0[self.id(id)].get()
    }
    fn node_mut(&mut self, id: u32) -> &mut N {
        let id = self.id(id);
        self.0[id].get_mut()
    }
    fn parent(&self, id: u32) -> Option<u32> {
        self.0[self.id(id)].parent().map(Self::index)
    }
    fn first_child(&self, id: u32) -> Option<u32> {
        self.0[self.id(id)].first_child().map(Self::index)
    }
    fn last_child(&self, id: u32) -> Option<u32> {
        self.0[self.id(id)].last_child().map(Self::index)
    }
    fn next_sibling(&self, id: u32) -> Option<u32> {
        self.0[self.id(id)].next_sibling().map(Self::index)
    }
    fn previous_sibling(&self, id: u32) -> Option<u32> {
        self.0[self.id(id)].previous_sibling().map(Self::index)
    }
}
