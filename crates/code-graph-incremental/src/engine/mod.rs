mod builder;
mod compile;
mod expr;
mod graph;
mod host;
pub mod lang;
mod machine;
pub mod pattern;
pub mod profile;
mod program;
mod run;
mod walk;

use smallvec::SmallVec;

use crate::tree::{Compact, Mutable, Node, Storage};
pub use builder::{Engine, EngineBuilder, Mounted, PipelineSource};
pub use graph::Graph;
pub use host::{
    Arg, Backend, Call, Calls, Cx, Derive, Entry, FnId, Function, Hooks, KeyPart, Kind, Links,
    Module, Outputs, Registration, Registry, register, register_backend,
};
pub use program::{Def, Part, Phase, Pipeline, RowShape, resolution};
pub use run::{Indexed, Run, WHOLE, Write};
use run::{Key, MAX_FIXPOINT_ROUNDS, Memo, SLOTS};
pub use walk::{run_family, run_tree, run_trees, scope_budget};

pub type Tree<S = Compact> = crate::tree::Tree<S>;
pub type Cursor<'a, S = Compact> = crate::tree::Cursor<'a, S>;

/// A tree storage the engine runs over. Each module function is compiled
/// once per storage, so a call dispatches statically.
pub trait Store: Storage<Node = Node> + Sized + 'static {
    fn call<M: Module>(f: &Function<M>) -> Call<M, Self>;
    fn hooks(r: &Registration) -> &Hooks<Self>;
    fn derive(d: &Derive) -> fn(Cursor<'_, Self>) -> u32;
    /// The tree as one templates can build in, if this storage allows it.
    fn writable(tree: &mut Tree<Self>) -> Option<&mut Tree<Mutable>>;
}

impl Store for Compact {
    fn call<M: Module>(f: &Function<M>) -> Call<M, Self> {
        f.call.compact
    }
    fn hooks(r: &Registration) -> &Hooks<Self> {
        &r.compact
    }
    fn derive(d: &Derive) -> fn(Cursor<'_, Self>) -> u32 {
        d.compact
    }
    fn writable(_: &mut Tree<Self>) -> Option<&mut Tree<Mutable>> {
        None
    }
}

impl Store for Mutable {
    fn call<M: Module>(f: &Function<M>) -> Call<M, Self> {
        f.call.mutable
    }
    fn hooks(r: &Registration) -> &Hooks<Self> {
        &r.mutable
    }
    fn derive(d: &Derive) -> fn(Cursor<'_, Self>) -> u32 {
        d.mutable
    }
    fn writable(tree: &mut Tree<Self>) -> Option<&mut Tree<Mutable>> {
        Some(tree)
    }
}

#[derive(
    Clone,
    Copy,
    Debug,
    Default,
    PartialEq,
    Eq,
    Hash,
    PartialOrd,
    Ord,
    rkyv::Archive,
    rkyv::Serialize,
    rkyv::Deserialize,
)]
pub struct Loc {
    pub fi: u32,
    pub node: u32,
}

impl Loc {
    pub fn new(fi: usize, node: u32) -> Self {
        Self {
            fi: u32::try_from(fi).expect("file index exceeds u32"),
            node,
        }
    }
}

impl<S: crate::tree::Storage> From<crate::tree::Cursor<'_, S>> for Loc {
    fn from(c: crate::tree::Cursor<'_, S>) -> Self {
        Self {
            fi: c.fi(),
            node: c.index(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Val {
    Node(Loc),
    Type(Loc),
    Sym(u32),
    Kind(u16),
    Bool(bool),
    Opaque,
}

pub type Vals = SmallVec<[Val; 2]>;

impl Val {
    pub fn loc(self) -> Option<Loc> {
        match self {
            Val::Node(loc) | Val::Type(loc) => Some(loc),
            _ => None,
        }
    }
}

pub fn truthy(vals: &[Val]) -> bool {
    vals.iter().any(|v| *v != Val::Bool(false))
}

pub fn of(locs: impl IntoIterator<Item = impl Into<Loc>>) -> Vals {
    locs.into_iter().map(|l| Val::Node(l.into())).collect()
}

pub fn yes(ok: bool) -> Vals {
    SmallVec::from_elem(Val::Bool(ok), 1)
}

pub fn one(val: Val) -> Vals {
    SmallVec::from_elem(val, 1)
}
