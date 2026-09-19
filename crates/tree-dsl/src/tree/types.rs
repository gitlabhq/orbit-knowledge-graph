use indextree::{Arena, NodeEdge, NodeId};

use crate::canonical;

pub(crate) const NONE: u32 = u32::MAX;

#[repr(u16)]
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    strum::Display,
    strum::EnumString,
    strum::IntoStaticStr,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub enum EdgeKind {
    Calls = 1,
    Defines = 2,
    Imports = 3,
    Extends = 4,
}

impl EdgeKind {
    pub fn name(self) -> &'static str {
        self.into()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Node {
    pub(crate) kind: u16,
    pub(crate) field: u16,
    pub(crate) sym: u32,
    pub(crate) start: u32,
    pub(crate) end: u32,
    pub(crate) start_row: u32,
    pub(crate) start_col: u32,
    pub(crate) end_row: u32,
    pub(crate) end_col: u32,
    pub(crate) synth: bool,
    pub(crate) named: bool,
}

#[derive(Clone, Copy, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct Edge {
    pub from_tree: u32,
    pub from_node: u32,
    pub to_tree: u32,
    pub to_node: u32,
    pub kind: EdgeKind,
}

impl Edge {
    pub fn new(from_tree: u32, from_node: u32, to_tree: u32, to_node: u32, kind: EdgeKind) -> Self {
        Self {
            from_tree,
            from_node,
            to_tree,
            to_node,
            kind,
        }
    }
    pub fn from_fi(&self) -> usize {
        self.from_tree as usize
    }

    pub fn to_fi(&self) -> usize {
        self.to_tree as usize
    }

    pub fn local(from: u32, to: u32, kind: EdgeKind) -> Self {
        Self {
            from_tree: 0,
            from_node: from,
            to_tree: 0,
            to_node: to,
            kind,
        }
    }
}

#[derive(Clone)]
pub struct Tree {
    pub(crate) arena: Arena<Node>,
    pub(crate) root: NodeId,
    pub label: String,
}

impl Tree {
    pub fn with_capacity(capacity: usize, root_node: Node) -> Self {
        let mut arena = Arena::with_capacity(capacity);
        let root = arena.new_node(root_node);
        Self {
            arena,
            root,
            label: String::new(),
        }
    }

    pub fn new(root_node: Node) -> Self {
        Self::with_capacity(1, root_node)
    }

    #[inline]
    pub(crate) fn to_id(&self, raw: u32) -> NodeId {
        self.arena
            .get_node_id_at(std::num::NonZeroUsize::new(raw as usize + 1).unwrap())
            .unwrap()
    }

    #[inline]
    pub(crate) fn to_raw(id: NodeId) -> u32 {
        usize::from(id) as u32 - 1
    }

    pub(crate) fn node(&self, id: NodeId) -> &Node {
        self.arena[id].get()
    }

    pub(crate) fn node_mut(&mut self, id: NodeId) -> &mut Node {
        self.arena[id].get_mut()
    }

    pub(crate) fn append(&mut self, parent: NodeId, child: Node) -> NodeId {
        parent.append_value(child, &mut self.arena)
    }

    pub(crate) fn replace(&mut self, target: NodeId, replacements: Vec<NodeId>) {
        let field = self.node(target).field;
        if target == self.root && replacements.len() != 1 {
            for r in replacements {
                r.remove_subtree(&mut self.arena);
            }
            return;
        }
        if let Some(&first) = replacements.first() {
            self.arena[first].get_mut().field = field;
        }
        if target == self.root {
            self.root = replacements[0];
        } else {
            for r in replacements {
                target.insert_before(r, &mut self.arena);
            }
        }
        target.remove_subtree(&mut self.arena);
    }

    pub(crate) fn postorder(&self) -> Vec<NodeId> {
        self.root
            .reverse_traverse(&self.arena)
            .filter_map(|edge| match edge {
                NodeEdge::Start(id) => Some(id),
                NodeEdge::End(_) => None,
            })
            .collect()
    }

    pub(crate) fn preorder(&self) -> Vec<NodeId> {
        self.root.descendants(&self.arena).collect()
    }

    pub fn prune(&mut self) {
        let ids: Vec<NodeId> = self.root.descendants(&self.arena).skip(1).collect();
        for id in ids {
            if !canonical::is_canonical(self.arena[id].get().kind) {
                let children: Vec<NodeId> = id.children(&self.arena).collect();
                for child in children {
                    child.detach(&mut self.arena);
                    id.insert_before(child, &mut self.arena);
                }
                id.remove(&mut self.arena);
            } else {
                self.arena[id].get_mut().field = 0;
            }
        }
    }

    pub fn len(&self) -> u32 {
        self.root.descendants(&self.arena).count() as u32
    }

    pub fn compact(&mut self) {
        let mut new_arena = Arena::with_capacity(self.root.descendants(&self.arena).count());
        let mut id_map = rustc_hash::FxHashMap::default();
        for id in self.root.descendants(&self.arena) {
            let parent: Option<NodeId> =
                id.parent(&self.arena).and_then(|p| id_map.get(&p).copied());
            let new_id = match parent {
                Some(p) => p.append_value(*self.arena[id].get(), &mut new_arena),
                None => new_arena.new_node(*self.arena[id].get()),
            };
            id_map.insert(id, new_id);
        }
        self.root = id_map[&self.root];
        self.arena = new_arena;
    }
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct SnapshotNode {
    pub kind: u16,
    pub field: u16,
    pub sym: u32,
    pub start: u32,
    pub end: u32,
    pub start_row: u32,
    pub start_col: u32,
    pub end_row: u32,
    pub end_col: u32,
    pub size: u32,
    pub synth: bool,
    pub named: bool,
    pub parent: u32,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct TreeSnapshot {
    pub nodes: Vec<SnapshotNode>,
    pub label: String,
}

impl From<&Tree> for TreeSnapshot {
    fn from(tree: &Tree) -> Self {
        let ids: Vec<NodeId> = tree.root.descendants(&tree.arena).collect();
        let id_to_pos: rustc_hash::FxHashMap<NodeId, u32> = ids
            .iter()
            .enumerate()
            .map(|(i, &id)| (id, i as u32))
            .collect();
        let mut nodes = Vec::with_capacity(ids.len());
        for &id in &ids {
            let n = tree.arena[id].get();
            let parent = id.parent(&tree.arena).map_or(NONE, |p| id_to_pos[&p]);
            let size = id.descendants(&tree.arena).count() as u32;
            nodes.push(SnapshotNode {
                kind: n.kind,
                field: n.field,
                sym: n.sym,
                start: n.start,
                end: n.end,
                start_row: n.start_row,
                start_col: n.start_col,
                end_row: n.end_row,
                end_col: n.end_col,
                size,
                synth: n.synth,
                named: n.named,
                parent,
            });
        }
        Self {
            nodes,
            label: tree.label.clone(),
        }
    }
}

impl From<TreeSnapshot> for Tree {
    fn from(snap: TreeSnapshot) -> Self {
        if snap.nodes.is_empty() {
            return Tree::new(Node::default());
        }
        let first = &snap.nodes[0];
        let mut tree = Tree::with_capacity(
            snap.nodes.len(),
            Node {
                kind: first.kind,
                field: first.field,
                sym: first.sym,
                start: first.start,
                end: first.end,
                start_row: first.start_row,
                start_col: first.start_col,
                end_row: first.end_row,
                end_col: first.end_col,
                synth: first.synth,
                named: first.named,
            },
        );
        let mut id_map = vec![tree.root];
        for sn in &snap.nodes[1..] {
            let parent = id_map[sn.parent as usize];
            let id = tree.append(
                parent,
                Node {
                    kind: sn.kind,
                    field: sn.field,
                    sym: sn.sym,
                    start: sn.start,
                    end: sn.end,
                    start_row: sn.start_row,
                    start_col: sn.start_col,
                    end_row: sn.end_row,
                    end_col: sn.end_col,
                    synth: sn.synth,
                    named: sn.named,
                },
            );
            id_map.push(id);
        }
        tree.label = snap.label;
        tree
    }
}
