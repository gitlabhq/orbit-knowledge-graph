use indextree::{Arena, NodeId};

use super::types::{Node, Tree};

pub const NONE: u32 = u32::MAX;

pub trait Storage<N> {
    type Nodes: Clone;
    type Id: Copy + Eq;

    fn index(id: Self::Id) -> u32;
    fn id(nodes: &Self::Nodes, index: u32) -> Self::Id;
    fn is_removed(nodes: &Self::Nodes, id: Self::Id) -> bool;
    fn node(nodes: &Self::Nodes, id: u32) -> &N;
    fn parent(nodes: &Self::Nodes, id: u32) -> Option<u32>;
    fn first_child(nodes: &Self::Nodes, id: u32) -> Option<u32>;
    fn last_child(nodes: &Self::Nodes, id: u32) -> Option<u32>;
    fn next_sibling(nodes: &Self::Nodes, id: u32) -> Option<u32>;
    fn previous_sibling(nodes: &Self::Nodes, id: u32) -> Option<u32>;
}

#[derive(Clone, Copy)]
pub struct Mutable;

#[derive(Clone, Copy)]
pub struct Compact;

impl Mutable {
    pub(crate) fn index(id: NodeId) -> u32 {
        usize::from(id) as u32 - 1
    }

    pub(crate) fn id<N>(nodes: &Arena<N>, id: u32) -> NodeId {
        let index = std::num::NonZeroUsize::new(id as usize + 1).expect("raw + 1 is nonzero");
        nodes
            .get_node_id_at(index)
            .expect("raw ids come from this arena")
    }
}

#[derive(Clone)]
pub struct CompactNode<N> {
    pub(crate) node: N,
    // A self-parent marks a removed slot without changing other node IDs.
    parent: u32,
    first_child: u32,
    last_child: u32,
    next_sibling: u32,
    previous_sibling: u32,
}

impl<N> CompactNode<N> {
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
}

impl<N: Clone> Storage<N> for Mutable {
    type Nodes = Arena<N>;
    type Id = NodeId;

    fn id(nodes: &Arena<N>, index: u32) -> NodeId {
        Self::id(nodes, index)
    }
    fn is_removed(nodes: &Arena<N>, id: NodeId) -> bool {
        id.is_removed(nodes)
    }

    fn index(id: NodeId) -> u32 {
        Self::index(id)
    }

    fn node(nodes: &Arena<N>, id: u32) -> &N {
        nodes[Self::id(nodes, id)].get()
    }

    fn parent(nodes: &Arena<N>, id: u32) -> Option<u32> {
        nodes[Self::id(nodes, id)].parent().map(Self::index)
    }

    fn first_child(nodes: &Arena<N>, id: u32) -> Option<u32> {
        nodes[Self::id(nodes, id)].first_child().map(Self::index)
    }

    fn last_child(nodes: &Arena<N>, id: u32) -> Option<u32> {
        nodes[Self::id(nodes, id)].last_child().map(Self::index)
    }

    fn next_sibling(nodes: &Arena<N>, id: u32) -> Option<u32> {
        nodes[Self::id(nodes, id)].next_sibling().map(Self::index)
    }

    fn previous_sibling(nodes: &Arena<N>, id: u32) -> Option<u32> {
        nodes[Self::id(nodes, id)]
            .previous_sibling()
            .map(Self::index)
    }
}

impl<N: Clone> Storage<N> for Compact {
    type Nodes = Vec<CompactNode<N>>;
    type Id = u32;

    fn id(_: &Self::Nodes, index: u32) -> u32 {
        index
    }
    fn is_removed(nodes: &Self::Nodes, id: u32) -> bool {
        nodes[id as usize].parent == id
    }

    fn index(id: u32) -> u32 {
        id
    }
    fn node(nodes: &Self::Nodes, id: u32) -> &N {
        &nodes[id as usize].node
    }
    fn parent(nodes: &Self::Nodes, id: u32) -> Option<u32> {
        let link = nodes[id as usize].parent;
        (link != NONE && link != id).then_some(link)
    }
    fn first_child(nodes: &Self::Nodes, id: u32) -> Option<u32> {
        let link = nodes[id as usize].first_child;
        (link != NONE).then_some(link)
    }
    fn last_child(nodes: &Self::Nodes, id: u32) -> Option<u32> {
        let link = nodes[id as usize].last_child;
        (link != NONE).then_some(link)
    }
    fn next_sibling(nodes: &Self::Nodes, id: u32) -> Option<u32> {
        let link = nodes[id as usize].next_sibling;
        (link != NONE).then_some(link)
    }
    fn previous_sibling(nodes: &Self::Nodes, id: u32) -> Option<u32> {
        let link = nodes[id as usize].previous_sibling;
        (link != NONE).then_some(link)
    }
}

impl<N: Clone + Default> From<Tree<Mutable, N>> for Tree<Compact, N> {
    fn from(tree: Tree<Mutable, N>) -> Self {
        let arena = tree
            .arena
            .iter()
            .enumerate()
            .map(|(index, entry)| CompactNode {
                node: if entry.is_removed() {
                    N::default()
                } else {
                    entry.get().clone()
                },
                parent: if entry.is_removed() {
                    index as u32
                } else {
                    entry.parent().map_or(NONE, Mutable::index)
                },
                first_child: entry.first_child().map_or(NONE, Mutable::index),
                last_child: entry.last_child().map_or(NONE, Mutable::index),
                next_sibling: entry.next_sibling().map_or(NONE, Mutable::index),
                previous_sibling: entry.previous_sibling().map_or(NONE, Mutable::index),
            })
            .collect();
        Self {
            arena,
            root: Mutable::index(tree.root),
            label: tree.label,
            tags: tree.tags,
            source: tree.source,
        }
    }
}

impl<N: Clone> From<Tree<Compact, N>> for Tree<Mutable, N> {
    fn from(tree: Tree<Compact, N>) -> Self {
        let mut arena = Arena::with_capacity(tree.arena.len());
        let ids: Vec<_> = tree
            .arena
            .iter()
            .map(|entry| arena.new_node(entry.node.clone()))
            .collect();
        for (index, entry) in tree.arena.iter().enumerate() {
            let mut child = entry.first_child;
            while child != NONE {
                ids[index].append(ids[child as usize], &mut arena);
                child = tree.arena[child as usize].next_sibling;
            }
        }
        for (index, entry) in tree.arena.iter().enumerate() {
            if entry.parent == index as u32 {
                ids[index].remove(&mut arena);
            }
        }
        Self {
            arena,
            root: ids[tree.root as usize],
            label: tree.label,
            tags: tree.tags,
            source: tree.source,
        }
    }
}

impl Tree<Compact> {
    pub(crate) fn node_mut(&mut self, id: u32) -> &mut Node {
        &mut self.arena[id as usize].node
    }
}

impl<N: Clone> Tree<Mutable, N> {
    pub fn into_compact(self) -> Tree<Compact, N> {
        let mut mapping = vec![NONE; self.arena.len()];
        for (index, id) in self.root.descendants(&self.arena).enumerate() {
            mapping[Mutable::index(id) as usize] = index as u32;
        }
        let remap = |id: Option<NodeId>| id.map_or(NONE, |id| mapping[Mutable::index(id) as usize]);
        let arena = self
            .root
            .descendants(&self.arena)
            .map(|id| {
                let entry = &self.arena[id];
                CompactNode {
                    node: entry.get().clone(),
                    parent: remap(entry.parent()),
                    first_child: remap(entry.first_child()),
                    last_child: remap(entry.last_child()),
                    next_sibling: remap(entry.next_sibling()),
                    previous_sibling: remap(entry.previous_sibling()),
                }
            })
            .collect();
        let tags = self
            .tags
            .into_iter()
            .filter_map(|(id, tags)| {
                let id = mapping[id as usize];
                (id != NONE).then_some((id, tags))
            })
            .collect();
        Tree {
            arena,
            root: 0,
            label: self.label,
            tags,
            source: self.source,
        }
    }
}
