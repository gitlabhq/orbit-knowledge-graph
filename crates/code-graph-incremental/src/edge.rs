#[repr(u16)]
#[derive(
    Clone,
    Copy,
    Debug,
    PartialEq,
    Eq,
    Hash,
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

#[repr(u8)]
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub enum CallResolution {
    #[default]
    Unknown,
    Callable,
    NonCallable,
    Reference,
}

#[derive(Clone, Copy, Debug, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
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
    pub fn local(from: u32, to: u32, kind: EdgeKind) -> Self {
        Self::new(0, from, 0, to, kind)
    }
}
