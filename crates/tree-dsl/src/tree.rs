use std::cell::RefCell;

pub const NONE: u32 = u32::MAX;

#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeKind {
    Calls = 1,
    Defines = 2,
    Imports = 3,
    TypeRef = 4,
}

impl EdgeKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Calls => "Calls",
            Self::Defines => "Defines",
            Self::Imports => "Imports",
            Self::TypeRef => "TypeRef",
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

#[derive(Clone, Copy, Debug)]
pub struct Edge {
    pub from: NodeRef,
    pub to: NodeRef,
    pub kind: EdgeKind,
}

#[derive(Default)]
pub struct Tree {
    pub nodes: Vec<Node>,
    pub edges: Vec<Edge>,
    pub label: String,
    next_id: u32,
    spare: Vec<Node>,
    appends: RefCell<Vec<(u32, Node)>>,
    inserts: RefCell<Vec<(u32, u32, u32)>>,
    insert_buf: RefCell<Vec<Node>>,
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
    pub fn parent_kind(&self, i: u32) -> Option<u16> {
        let p = self.nodes[i as usize].parent;
        (p != NONE).then(|| self.kind(p))
    }
    #[inline]
    pub fn hop(&self, i: u32) -> u32 {
        i + self.nodes[i as usize].size
    }
    #[inline]
    pub fn text<'a>(&self, lang: &'a crate::lang::Lang, i: u32) -> &'a str {
        lang.syms.resolve(self.nodes[i as usize].sym)
    }

    /// Read text from the source by the node's byte span. Use this for
    /// non-leaf nodes (e.g. dotted_name) whose sym is 0.
    pub fn span_text<'a>(&self, source: &'a str, i: u32) -> &'a str {
        let n = &self.nodes[i as usize];
        &source[n.start as usize..n.end as usize]
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

    pub fn children_of_kind(&self, i: u32, k: u16) -> impl Iterator<Item = u32> + '_ {
        self.children(i).filter(move |&c| self.kind(c) == k)
    }

    pub fn has_child(&self, i: u32, k: u16) -> bool {
        self.children_of_kind(i, k).next().is_some()
    }

    pub fn parent(&self, i: u32) -> Option<u32> {
        let p = self.nodes[i as usize].parent;
        (p != NONE).then_some(p)
    }

    pub fn parent_chain(&self, i: u32) -> impl Iterator<Item = u32> + '_ {
        let mut cur = self.parent(i);
        std::iter::from_fn(move || {
            let n = cur?;
            cur = self.parent(n);
            Some(n)
        })
    }

    pub fn prev_sibling(&self, i: u32) -> Option<u32> {
        let parent = self.parent(i)?;
        let mut prev = None;
        for c in self.children(parent) {
            if c == i {
                return prev;
            }
            prev = Some(c);
        }
        None
    }

    pub fn next_sibling(&self, i: u32) -> Option<u32> {
        let parent = self.parent(i)?;
        let mut found = false;
        for c in self.children(parent) {
            if found {
                return Some(c);
            }
            if c == i {
                found = true;
            }
        }
        None
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

    pub fn add_edge(&mut self, from: u32, to: u32, kind: EdgeKind) {
        if kind == EdgeKind::Calls
            || !self
                .edges
                .iter()
                .any(|e| e.from.node == from && e.to.node == to && e.kind == kind)
        {
            self.edges.push(Edge {
                from: NodeRef {
                    tree: 0,
                    node: from,
                },
                to: NodeRef { tree: 0, node: to },
                kind,
            });
        }
    }

    pub fn edges_from(&self, node: u32) -> impl Iterator<Item = &Edge> {
        self.edges.iter().filter(move |e| e.from.node == node)
    }

    pub fn edges_to(&self, node: u32) -> impl Iterator<Item = &Edge> {
        self.edges.iter().filter(move |e| e.to.node == node)
    }

    pub fn remove(&mut self, i: u32) {
        self.nodes[i as usize].dead = true;
    }
    pub fn set_kind(&mut self, i: u32, k: u16) {
        self.nodes[i as usize].kind = k;
    }
    pub fn set_field(&mut self, i: u32, f: u16) {
        self.nodes[i as usize].field = f;
    }
    pub fn set_text(&mut self, i: u32, sym: u32) {
        self.nodes[i as usize].sym = sym;
    }
    pub fn flatten(&mut self, first: u32, last: u32, kind: u16, sym: u32) {
        let (end, end_span) = (self.hop(last), self.nodes[last as usize].end);
        let n = &mut self.nodes[first as usize];
        let old = n.size;
        n.kind = kind;
        n.sym = sym;
        n.end = end_span;
        n.size = end - first;
        let mut j = first + old;
        while j < end {
            self.nodes[j as usize].parent = first;
            j = self.hop(j);
        }
    }

    pub fn replace(&mut self, i: u32, sub: &[Node]) {
        let old = self.nodes[i as usize].size as usize;
        if sub.len() > old {
            let field = self.nodes[i as usize].field;
            self.remove(i);
            let start = self.insert_buf.borrow().len();
            self.insert_before(i, sub);
            self.insert_buf.borrow_mut()[start].field = field;
            return;
        }
        let (parent, field) = (self.nodes[i as usize].parent, self.nodes[i as usize].field);
        for (k, mut n) in sub.iter().copied().enumerate() {
            n.parent = if n.parent == NONE {
                parent
            } else {
                n.parent + i
            };
            if k == 0 {
                n.field = field;
            }
            self.nodes[i as usize + k] = n;
        }
        if sub.len() < old {
            let d = &mut self.nodes[i as usize + sub.len()];
            d.dead = true;
            d.size = (old - sub.len()) as u32;
        }
    }

    pub fn append(&self, parent: u32, leaf: Node) {
        self.appends.borrow_mut().push((parent, leaf));
    }

    pub fn insert_before(&self, i: u32, sub: &[Node]) {
        let mut buf = self.insert_buf.borrow_mut();
        let base = buf.len() as u32;
        self.inserts.borrow_mut().push((i, base, sub.len() as u32));
        for n in sub {
            buf.push(Node {
                parent: if n.parent == NONE {
                    NONE
                } else {
                    n.parent + base
                },
                ..*n
            });
        }
    }

    pub fn compact(&mut self) -> Vec<u32> {
        let mut appends = self.appends.take();
        let mut inserts = self.inserts.take();
        let insert_buf = self.insert_buf.take();
        appends.sort_by_key(|a| a.0);
        inserts.sort_by_key(|x| x.0);
        let old = std::mem::take(&mut self.nodes);
        let mut new = std::mem::take(&mut self.spare);
        new.clear();
        let mut remap = vec![NONE; old.len()];
        let mut open: Vec<(u32, u32, u32)> = Vec::new();
        let (mut i, mut ip) = (0u32, 0usize);
        loop {
            while open.last().is_some_and(|&(_, e, _)| i >= e) {
                let (o, _, oi) = open.pop().unwrap();
                let lo = appends.partition_point(|a| a.0 < oi);
                let hi = appends.partition_point(|a| a.0 <= oi);
                for &(_, leaf) in &appends[lo..hi] {
                    new.push(Node {
                        parent: o,
                        size: 1,
                        ..leaf
                    });
                }
                new[o as usize].size = new.len() as u32 - o;
            }
            if i as usize == old.len() {
                break;
            }
            let top = open.last().map_or(NONE, |&(o, _, _)| o);
            while ip < inserts.len() && inserts[ip].0 == i {
                let (_, s, l) = inserts[ip];
                let base = new.len() as u32;
                for n in &insert_buf[s as usize..(s + l) as usize] {
                    new.push(Node {
                        parent: if n.parent == NONE {
                            top
                        } else {
                            n.parent - s + base
                        },
                        ..*n
                    });
                }
                ip += 1;
            }
            let n = old[i as usize];
            if n.dead {
                i += n.size;
                continue;
            }
            remap[i as usize] = new.len() as u32;
            open.push((new.len() as u32, i + n.size, i));
            new.push(Node { parent: top, ..n });
            i += 1;
        }
        self.spare = old;
        self.nodes = new;
        for edge in &mut self.edges {
            if let Some(&new_from) = remap.get(edge.from.node as usize)
                && new_from != NONE
            {
                edge.from.node = new_from;
            }
            if let Some(&new_to) = remap.get(edge.to.node as usize)
                && new_to != NONE
            {
                edge.to.node = new_to;
            }
        }
        remap
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
