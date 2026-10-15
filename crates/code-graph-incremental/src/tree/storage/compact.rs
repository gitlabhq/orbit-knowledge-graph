use indextree::Arena;

use super::{Mutable, NONE, Storage};
use crate::tree::{Node, Tree};

#[derive(Clone)]
pub struct Compact<N = Node>(pub(crate) Vec<Entry<N>>);

#[derive(Clone)]
pub(crate) struct Entry<N> {
    pub(crate) node: N,
    // A self-parent marks a removed slot without changing other node IDs.
    parent: u32,
    first_child: u32,
    last_child: u32,
    next_sibling: u32,
    previous_sibling: u32,
}

impl<N> Entry<N> {
    pub(crate) fn new(node: N, parent: u32) -> Self {
        Self {
            node,
            parent,
            first_child: NONE,
            last_child: NONE,
            next_sibling: NONE,
            previous_sibling: NONE,
        }
    }

    pub(crate) fn link(nodes: &mut [Self], parent: u32, child: u32) {
        let previous = nodes[parent as usize].last_child;
        nodes[child as usize].previous_sibling = previous;
        if previous == NONE {
            nodes[parent as usize].first_child = child;
        } else {
            nodes[previous as usize].next_sibling = child;
        }
        nodes[parent as usize].last_child = child;
    }

    fn from_mutable(entry: &indextree::Node<N>, index: u32, map: impl Fn(u32) -> u32) -> Self
    where
        N: Clone + Default,
    {
        let link =
            |id: Option<indextree::NodeId>| id.map_or(NONE, |id| map(Mutable::<N>::index(id)));
        Self {
            node: if entry.is_removed() {
                N::default()
            } else {
                entry.get().clone()
            },
            parent: if entry.is_removed() {
                index
            } else {
                link(entry.parent())
            },
            first_child: link(entry.first_child()),
            last_child: link(entry.last_child()),
            next_sibling: link(entry.next_sibling()),
            previous_sibling: link(entry.previous_sibling()),
        }
    }
}

impl<N> Storage for Compact<N> {
    type Node = N;
    type Id = u32;

    fn index(id: u32) -> u32 {
        id
    }
    fn id(&self, index: u32) -> u32 {
        index
    }
    fn is_removed(&self, id: u32) -> bool {
        self.0[id as usize].parent == id
    }
    fn node(&self, id: u32) -> &N {
        &self.0[id as usize].node
    }
    fn node_mut(&mut self, id: u32) -> &mut N {
        &mut self.0[id as usize].node
    }
    fn parent(&self, id: u32) -> Option<u32> {
        let link = self.0[id as usize].parent;
        (link != NONE && link != id).then_some(link)
    }
    fn first_child(&self, id: u32) -> Option<u32> {
        let link = self.0[id as usize].first_child;
        (link != NONE).then_some(link)
    }
    fn last_child(&self, id: u32) -> Option<u32> {
        let link = self.0[id as usize].last_child;
        (link != NONE).then_some(link)
    }
    fn next_sibling(&self, id: u32) -> Option<u32> {
        let link = self.0[id as usize].next_sibling;
        (link != NONE).then_some(link)
    }
    fn previous_sibling(&self, id: u32) -> Option<u32> {
        let link = self.0[id as usize].previous_sibling;
        (link != NONE).then_some(link)
    }
}

impl<N: Clone + Default> From<Tree<Mutable<N>>> for Tree<Compact<N>> {
    fn from(tree: Tree<Mutable<N>>) -> Self {
        let nodes = tree
            .storage
            .0
            .iter()
            .enumerate()
            .map(|(id, entry)| Entry::from_mutable(entry, id as u32, |id| id))
            .collect();
        Tree {
            storage: Compact(nodes),
            root: Mutable::<N>::index(tree.root),
            label: tree.label,
            tags: tree.tags,
            source: tree.source,
        }
    }
}

impl<N: Clone> From<Tree<Compact<N>>> for Tree<Mutable<N>> {
    fn from(tree: Tree<Compact<N>>) -> Self {
        let nodes = tree.storage.0;
        let mut arena = Arena::with_capacity(nodes.len());
        let ids: Vec<_> = nodes
            .iter()
            .map(|entry| arena.new_node(entry.node.clone()))
            .collect();
        for (index, entry) in nodes.iter().enumerate() {
            let mut child = entry.first_child;
            while child != NONE {
                ids[index].append(ids[child as usize], &mut arena);
                child = nodes[child as usize].next_sibling;
            }
        }
        for (index, entry) in nodes.iter().enumerate() {
            if entry.parent == index as u32 {
                ids[index].remove(&mut arena);
            }
        }
        Tree {
            storage: Mutable(arena),
            root: ids[tree.root as usize],
            label: tree.label,
            tags: tree.tags,
            source: tree.source,
        }
    }
}

impl<N: Clone + Default> Tree<Mutable<N>> {
    pub fn compact_and_remap(self) -> Tree<Compact<N>> {
        let arena = &self.storage.0;
        let mut mapping = vec![NONE; arena.len()];
        let mut count = 0;
        for (index, id) in self.root.descendants(arena).enumerate() {
            mapping[Mutable::<N>::index(id) as usize] = index as u32;
            count = index + 1;
        }
        let mut nodes = Vec::with_capacity(count);
        nodes.extend(self.root.descendants(arena).map(|id| {
            Entry::from_mutable(
                &arena[id],
                mapping[Mutable::<N>::index(id) as usize],
                |id| mapping[id as usize],
            )
        }));
        let tags = self
            .tags
            .into_iter()
            .filter_map(|(id, tags)| {
                let id = mapping[id as usize];
                (id != NONE).then_some((id, tags))
            })
            .collect();
        Tree {
            storage: Compact(nodes),
            root: 0,
            label: self.label,
            tags,
            source: self.source,
        }
    }
}
