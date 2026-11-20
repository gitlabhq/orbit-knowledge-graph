/// An edge kind: its name's id in `Lang::edges`, declared in pipeline.yaml.
#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct EdgeKind(pub u16);

#[repr(u8)]
#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub enum CallResolution {
    #[default]
    Unknown,
    Callable,
    /// On a TypeFlow edge: the producing call's callee root has a local
    /// value, so wave 2 does not type the call by its name.
    NonCallable,
    Reference,
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct Edge {
    pub from_tree: u32,
    pub from_node: u32,
    pub to_tree: u32,
    pub to_node: u32,
    pub kind: EdgeKind,
    pub site: Option<u32>,
    pub call_resolution: CallResolution,
}

impl Edge {
    /// The edge with both ends in tree `fi`.
    pub fn in_file(self, fi: u32) -> Self {
        Self {
            from_tree: fi,
            to_tree: fi,
            ..self
        }
    }

    pub fn new(from_tree: u32, from_node: u32, to_tree: u32, to_node: u32, kind: EdgeKind) -> Self {
        Self {
            from_tree,
            from_node,
            to_tree,
            to_node,
            kind,
            site: None,
            call_resolution: CallResolution::Unknown,
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
    pub fn from_loc(&self) -> crate::engine::Loc {
        crate::engine::Loc {
            fi: self.from_tree,
            node: self.from_node,
        }
    }
    pub fn to_loc(&self) -> crate::engine::Loc {
        crate::engine::Loc {
            fi: self.to_tree,
            node: self.to_node,
        }
    }
    pub fn local(from: u32, to: u32, kind: EdgeKind) -> Self {
        Self::new(0, from, 0, to, kind)
    }
}
