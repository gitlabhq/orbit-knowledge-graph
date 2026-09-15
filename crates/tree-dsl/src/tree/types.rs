pub const NONE: u32 = u32::MAX;

#[repr(u16)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub enum EdgeKind {
    Calls = 1,
    Defines = 2,
    Imports = 3,
    Extends = 4,
}

impl EdgeKind {
    pub fn name(self) -> &'static str {
        match self {
            Self::Calls => "Calls",
            Self::Defines => "Defines",
            Self::Imports => "Imports",
            Self::Extends => "Extends",
        }
    }
}

impl std::fmt::Display for EdgeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[derive(Clone, Copy, Debug, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct Node {
    pub kind: u16,
    pub field: u16,
    pub parent: u32,
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
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
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

#[derive(Clone, Copy, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
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

#[derive(Clone, Default, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct Tree {
    pub nodes: Vec<Node>,
    pub label: String,
    kinds: Vec<(u16, Vec<u32>)>,
}

impl Tree {
    pub(crate) fn new(nodes: Vec<Node>, label: String) -> Self {
        let mut tree = Self::from_nodes(nodes);
        tree.label = label;
        tree
    }

    pub(crate) fn from_nodes(nodes: Vec<Node>) -> Self {
        let mut kinds = Vec::<(u16, Vec<u32>)>::new();
        for (i, node) in nodes.iter().enumerate() {
            match kinds.iter_mut().find(|(kind, _)| *kind == node.kind) {
                Some((_, ids)) => ids.push(i as u32),
                None => kinds.push((node.kind, vec![i as u32])),
            }
        }
        kinds.sort_unstable_by_key(|(kind, _)| *kind);
        Self {
            nodes,
            kinds,
            label: String::new(),
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
    pub fn hop(&self, i: u32) -> u32 {
        i + self.nodes[i as usize].size
    }

    pub fn children(&self, i: u32) -> impl Iterator<Item = u32> + '_ {
        let (end, mut c) = (self.hop(i), i + 1);
        std::iter::from_fn(move || {
            (c < end).then(|| {
                let r = c;
                c = self.hop(c);
                r
            })
        })
    }

    pub fn descendants(&self, i: u32) -> impl Iterator<Item = u32> + '_ {
        let end = self.hop(i);
        i + 1..end
    }

    pub fn nodes_of_kind(&self, kind: u16) -> &[u32] {
        self.kinds
            .binary_search_by_key(&kind, |(kind, _)| *kind)
            .map_or(&[], |i| self.kinds[i].1.as_slice())
    }

    /// Remap all sym IDs using the given table. Used after merging per-thread interners.
    pub fn remap_syms(&mut self, remap: &[u32]) {
        for n in &mut self.nodes {
            if n.sym != 0 && (n.sym as usize) < remap.len() {
                n.sym = remap[n.sym as usize];
            }
        }
    }
}
