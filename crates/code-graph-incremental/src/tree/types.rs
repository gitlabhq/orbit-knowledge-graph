use indextree::{Arena, NodeId};
use rustc_hash::FxHashMap;
use smallvec::SmallVec;

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

#[derive(Clone, Copy, Debug, Default)]
#[allow(dead_code)]
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

/// One file's syntax tree; `source` is dropped once rewriting ends.
#[derive(Clone)]
#[allow(dead_code)]
pub struct Tree {
    pub(crate) arena: Arena<Node>,
    pub(crate) root: NodeId,
    pub label: String,
    pub tags: FxHashMap<u32, SmallVec<[Tag; 2]>>,
    pub source: std::sync::Arc<str>,
}
