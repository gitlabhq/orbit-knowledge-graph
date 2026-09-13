use std::cell::RefCell;

pub const NONE: u32 = u32::MAX;

#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeKind {
    Calls = 1,
    Defines = 2,
    Imports = 3,
}

impl EdgeKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Calls => "Calls",
            Self::Defines => "Defines",
            Self::Imports => "Imports",
        }
    }
}

impl std::fmt::Display for EdgeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Node {
    pub id: u32,
    pub kind: u16,
    pub field: u16,
    pub parent: u32,
    pub sym: u32,
    pub start: u32,
    pub end: u32,
    pub size: u32,
    pub synth: bool,
    pub dead: bool,
    pub named: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct NodeRef {
    pub tree: u32,
    pub node: u32,
}

impl NodeRef {
    pub fn local(node: u32) -> Self {
        Self { tree: 0, node }
    }
    pub fn new(tree: usize, node: u32) -> Self {
        Self {
            tree: tree as u32,
            node,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Edge {
    pub from: NodeRef,
    pub to: NodeRef,
    pub kind: EdgeKind,
}

impl Edge {
    pub fn new(
        from_tree: usize,
        from_node: u32,
        to_tree: usize,
        to_node: u32,
        kind: EdgeKind,
    ) -> Self {
        Self {
            from: NodeRef::new(from_tree, from_node),
            to: NodeRef::new(to_tree, to_node),
            kind,
        }
    }
    pub fn local(from: u32, to: u32, kind: EdgeKind) -> Self {
        Self {
            from: NodeRef::local(from),
            to: NodeRef::local(to),
            kind,
        }
    }
}

#[derive(Default)]
pub struct Tree {
    pub nodes: Vec<Node>,
    pub(crate) edges_cell: RefCell<Vec<Edge>>,
    pub label: String,
    pub(crate) next_id: u32,
    pub(crate) spare: Vec<Node>,
    pub(crate) appends: RefCell<Vec<(u32, Node)>>,
    pub(crate) inserts: RefCell<Vec<(u32, u32, u32)>>,
    pub(crate) insert_buf: RefCell<Vec<Node>>,
}

impl Tree {
    pub fn from_nodes(nodes: Vec<Node>) -> Self {
        Self {
            nodes,
            ..Default::default()
        }
    }

    #[inline]
    pub fn node(&self, i: u32) -> &Node {
        &self.nodes[i as usize]
    }
    #[inline]
    pub fn kind(&self, i: u32) -> u16 {
        self.nodes[i as usize].kind
    }
    #[inline]
    pub fn sym(&self, i: u32) -> u32 {
        self.nodes[i as usize].sym
    }
    #[inline]
    pub fn field_of(&self, i: u32) -> u16 {
        self.nodes[i as usize].field
    }
    #[inline]
    pub fn hop(&self, i: u32) -> u32 {
        i + self.nodes[i as usize].size
    }
    #[inline]
    pub fn text<'a>(&self, lang: &'a crate::lang::Lang, i: u32) -> &'a str {
        lang.syms.resolve(self.nodes[i as usize].sym)
    }

    pub fn parent(&self, i: u32) -> Option<u32> {
        let p = self.nodes[i as usize].parent;
        (p != NONE).then_some(p)
    }

    pub fn children(&self, i: u32) -> impl Iterator<Item = u32> + '_ {
        let (end, mut c) = (self.hop(i), i + 1);
        std::iter::from_fn(move || {
            c = live(self, c, end);
            (c < end).then(|| {
                let r = c;
                c = self.hop(c);
                r
            })
        })
    }

    pub fn child_by_field(&self, i: u32, f: u16) -> Option<u32> {
        self.children(i).find(|&c| self.field_of(c) == f)
    }

    pub fn descendants(&self, i: u32) -> impl Iterator<Item = u32> + '_ {
        let end = self.hop(i);
        let mut c = live(self, i + 1, end);
        std::iter::from_fn(move || {
            if c >= end {
                return None;
            }
            let r = c;
            c = live(self, c + 1, end);
            Some(r)
        })
    }

    pub fn add_edge(&self, from: u32, to: u32, kind: EdgeKind) {
        let mut edges = self.edges_cell.borrow_mut();
        if !edges
            .iter()
            .any(|e| e.from.node == from && e.to.node == to && e.kind == kind)
        {
            edges.push(Edge::local(from, to, kind));
        }
    }

    pub fn edges(&self) -> std::cell::Ref<'_, Vec<Edge>> {
        self.edges_cell.borrow()
    }

    pub fn edges_mut(&mut self) -> &mut Vec<Edge> {
        self.edges_cell.get_mut()
    }

    /// Remap all sym IDs using the given table. Used after merging per-thread interners.
    pub fn remap_syms(&mut self, remap: &[u32]) {
        for n in &mut self.nodes {
            if n.sym != 0 && (n.sym as usize) < remap.len() {
                n.sym = remap[n.sym as usize];
            }
        }
    }

    pub fn prune(&mut self) {
        for i in 1..self.nodes.len() {
            self.nodes[i].field = 0;
            if self.nodes[i].dead {
                continue;
            }
            if !crate::canonical::is_canonical(self.nodes[i].kind) {
                self.nodes[i].dead = true;
                self.nodes[i].size = 1;
            }
        }
    }
}

#[inline]
pub fn live(t: &Tree, mut c: u32, end: u32) -> u32 {
    while c < end && t.nodes[c as usize].dead {
        c = t.hop(c);
    }
    c
}

pub fn elems<'a>(
    t: &'a Tree,
    (a, b): (u32, u32),
    kinds: &'a [u16],
) -> impl Iterator<Item = u32> + 'a {
    let mut c = live(t, a, b);
    std::iter::from_fn(move || {
        while c < b {
            let r = c;
            c = live(t, t.hop(c), b);
            if kinds.is_empty() || kinds.contains(&t.kind(r)) {
                return Some(r);
            }
        }
        None
    })
}

pub fn copy_subtree(t: &Tree, i: u32, out: &mut Vec<Node>, parent: u32) {
    let at = out.len();
    out.push(Node {
        parent,
        id: 0,
        ..t.nodes[i as usize]
    });
    for c in t.children(i) {
        copy_subtree(t, c, out, at as u32);
    }
    out[at].size = (out.len() - at) as u32;
}
