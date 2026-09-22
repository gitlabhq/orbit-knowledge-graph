use indextree::{Arena, NodeEdge, NodeId};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

use crate::canonical;

#[derive(Clone, Copy, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct Tag {
    pub key: u32,
    pub val: u32,
}

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
    TypeFlow = 5,
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
    pub site: Option<u32>,
}

impl Edge {
    pub fn new(from_tree: u32, from_node: u32, to_tree: u32, to_node: u32, kind: EdgeKind) -> Self {
        Self {
            from_tree,
            from_node,
            to_tree,
            to_node,
            kind,
            site: None,
        }
    }
    pub fn from_fi(&self) -> usize {
        self.from_tree as usize
    }

    pub fn to_fi(&self) -> usize {
        self.to_tree as usize
    }

    pub fn from(&self) -> (u32, u32) {
        (self.from_tree, self.from_node)
    }

    pub fn to(&self) -> (u32, u32) {
        (self.to_tree, self.to_node)
    }

    pub fn local(from: u32, to: u32, kind: EdgeKind) -> Self {
        Self {
            from_tree: 0,
            from_node: from,
            to_tree: 0,
            to_node: to,
            kind,
            site: None,
        }
    }
}

#[derive(Clone)]
pub struct Tree {
    pub(crate) arena: Arena<Node>,
    pub(crate) root: NodeId,
    pub label: String,
    pub tags: FxHashMap<u32, SmallVec<[Tag; 2]>>,
    /// The file text. Nodes with named children carry no interned sym; their
    /// text is sliced from here when a rule asks for it.
    pub source: std::sync::Arc<str>,
}

impl Tree {
    pub fn with_capacity(capacity: usize, root_node: Node) -> Self {
        let mut arena = Arena::with_capacity(capacity);
        let root = arena.new_node(root_node);
        Self {
            arena,
            root,
            label: String::new(),
            tags: FxHashMap::default(),
            source: std::sync::Arc::from(""),
        }
    }

    pub fn new(root_node: Node) -> Self {
        Self::with_capacity(1, root_node)
    }

    /// A node's text: the interned sym when it has one, else its source span.
    pub fn text<'a>(&'a self, id: NodeId, lang: &'a crate::intern::Lang) -> &'a str {
        let n = self.node(id);
        if n.sym != 0 {
            return lang.syms.resolve(n.sym);
        }
        if n.synth || !n.named {
            return "";
        }
        self.source
            .get(n.start as usize..n.end as usize)
            .unwrap_or("")
    }

    /// A node's sym, interning its source text on first use.
    pub fn sym_of(&self, id: NodeId, lang: &crate::intern::Lang) -> u32 {
        let n = self.node(id);
        if n.sym != 0 {
            return n.sym;
        }
        match self.text(id, lang) {
            "" => 0,
            s => lang.syms.intern(s),
        }
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

    pub fn set_tag(&mut self, node: u32, key: u32, val: u32) {
        let entry = self.tags.entry(node).or_default();
        if let Some(t) = entry.iter_mut().find(|t| t.key == key) {
            t.val = val;
        } else {
            entry.push(Tag { key, val });
        }
    }

    pub fn get_tag(&self, node: u32, key: u32) -> Option<u32> {
        self.tags
            .get(&node)
            .and_then(|tags| tags.iter().find(|t| t.key == key))
            .map(|t| t.val)
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

    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
        let raw_map: FxHashMap<u32, u32> = id_map
            .iter()
            .map(|(&old_id, &new_id)| (Self::to_raw(old_id), Self::to_raw(new_id)))
            .collect();
        self.tags = std::mem::take(&mut self.tags)
            .into_iter()
            .filter_map(|(old_raw, tags)| Some((*raw_map.get(&old_raw)?, tags)))
            .collect();
        self.root = id_map[&self.root];
        self.arena = new_arena;
    }
}
