mod ops;
mod program;

use std::hash::Hash;
use std::str::FromStr;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use crate::constants::{RED_ZONE, STACK_SEGMENT};
pub use crate::dsl::rules::*;
pub use ops::{Chain, Members, Types, ancestors, chain, members};
pub use program::Program;

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

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub enum Val {
    #[default]
    None,
    Yes,
    Lit(u32, u16),
    Node(Loc),
    Nodes(SmallVec<[Loc; 2]>),
    Flow(SmallVec<[crate::ssa::Value; 2]>),
}

impl Val {
    pub fn is_none(&self) -> bool {
        matches!(self, Val::None)
    }
    pub fn node(&self) -> Option<Loc> {
        self.nodes().first().copied()
    }
    pub fn nodes(&self) -> &[Loc] {
        match self {
            Val::Node(loc) => std::slice::from_ref(loc),
            Val::Nodes(locs) => locs,
            _ => &[],
        }
    }
    pub fn sym(&self) -> u32 {
        match self {
            Val::Lit(sym, _) => *sym,
            _ => 0,
        }
    }
    pub fn kind(&self) -> u16 {
        match self {
            Val::Lit(_, kind) => *kind,
            _ => 0,
        }
    }
    pub fn of(locs: impl IntoIterator<Item = impl Into<Loc>>) -> Val {
        let locs: SmallVec<[Loc; 2]> = locs.into_iter().map(Into::into).collect();
        match locs.len() {
            0 => Val::None,
            1 => Val::Node(locs[0]),
            _ => Val::Nodes(locs),
        }
    }
    pub fn when(yes: bool) -> Val {
        if yes { Val::Yes } else { Val::None }
    }
}

pub trait Func: Copy + Eq + Hash + FromStr + strum::EnumProperty + Send + Sync + 'static {}
impl<T: Copy + Eq + Hash + FromStr + strum::EnumProperty + Send + Sync + 'static> Func for T {}

pub type Key<F> = (F, SmallVec<[Val; 3]>);

pub struct Guard<F> {
    busy: FxHashSet<Key<F>>,
    pub cut: bool,
}

impl<F> Default for Guard<F> {
    fn default() -> Self {
        Self {
            busy: FxHashSet::default(),
            cut: false,
        }
    }
}

pub type MemoMap<F> = FxHashMap<Key<F>, Val>;

pub trait Host {
    type F: Func;
    fn call(&mut self, f: Self::F, args: &[Val]) -> Val;
    fn memo_get(&mut self, key: &Key<Self::F>) -> Option<Val>;
    fn memo_put(&mut self, key: Key<Self::F>, val: &Val);
    fn guard(&mut self) -> &mut Guard<Self::F>;
    fn defs(&mut self) -> &mut Vec<u32>;
    fn step(&mut self) -> bool {
        true
    }
}

/// Memoized calls share one cycle guard: a call already in progress yields
/// nothing and marks the result in progress as cut, which is never stored.
pub fn call<H: Host>(h: &mut H, f: H::F, memo: bool, args: &[Val]) -> Val {
    if !memo {
        return h.call(f, args);
    }
    let key = (f, SmallVec::from(args));
    if let Some(found) = h.memo_get(&key) {
        return found;
    }
    if !h.guard().busy.insert(key.clone()) {
        h.guard().cut = true;
        return Val::None;
    }
    let outer = std::mem::take(&mut h.guard().cut);
    let found = stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || h.call(f, args));
    let guard = h.guard();
    guard.busy.remove(&key);
    let cut = guard.cut;
    guard.cut |= outer;
    if !cut {
        h.memo_put(key, &found);
    }
    found
}
