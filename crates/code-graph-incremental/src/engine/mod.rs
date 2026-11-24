mod builder;
mod compile;
mod expr;
mod graph;
mod host;
pub mod lang;
pub mod pattern;
pub mod profile;
mod program;
mod run;
mod vm;
mod walk;

use smallvec::SmallVec;

use crate::tree::{Compact, Mutable, Node, Storage};
pub use builder::{Engine, EngineBuilder, Mounted, PipelineSource};
pub use graph::Graph;
pub use host::{
    Arg, Backend, Call, Calls, Cx, Derive, Entry, FnId, Function, Hooks, KeyPart, Kind, Links,
    Module, Outputs, Registration, Registry, SETS, register, register_backend,
};
pub use program::{Def, Part, Phase, Pipeline, RowShape, TreeSets};
pub use run::{Indexed, Run, WHOLE, Write};
use run::{Input, Key, MAX_FIXPOINT_ROUNDS, Memo, SLOTS};
pub use walk::{Around, Ran, run_shared, run_tree, scope_budget};

pub type Tree<S = Compact> = crate::tree::Tree<S>;
pub type Cursor<'a, S = Compact> = crate::tree::Cursor<'a, S>;
pub type Trees<'a, S = Compact> = crate::tree::Trees<'a, S>;

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

/// Distinct values in first-seen order; past a few it keeps a hash set, so
/// building a large set stays linear.
#[derive(Default)]
pub struct Distinct {
    vals: Vals,
    seen: Option<rustc_hash::FxHashSet<Val>>,
}

impl Distinct {
    /// Starts from `vals`, which must hold no repeats.
    pub fn of(vals: Vals) -> Self {
        let mut d = Self { vals, seen: None };
        d.index();
        d
    }

    pub fn push(&mut self, v: Val) {
        let new = match &mut self.seen {
            Some(seen) => seen.insert(v),
            None => !self.vals.contains(&v),
        };
        if new {
            self.vals.push(v);
            self.index();
        }
    }

    pub fn contains(&self, v: &Val) -> bool {
        match &self.seen {
            Some(seen) => seen.contains(v),
            None => self.vals.contains(v),
        }
    }

    pub fn into_vals(self) -> Vals {
        self.vals
    }

    fn index(&mut self) {
        if self.seen.is_none() && self.vals.len() > 16 {
            self.seen = Some(self.vals.iter().copied().collect());
        }
    }
}

impl Extend<Val> for Distinct {
    fn extend<I: IntoIterator<Item = Val>>(&mut self, vals: I) {
        vals.into_iter().for_each(|v| self.push(v));
    }
}

impl FromIterator<Val> for Distinct {
    fn from_iter<I: IntoIterator<Item = Val>>(vals: I) -> Self {
        let mut d = Self::default();
        d.extend(vals);
        d
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
