mod expr;
pub(crate) mod matching;
mod program;

use std::sync::LazyLock;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use crate::constants::{RED_ZONE, STACK_SEGMENT};
use crate::env::Env;
use crate::modules;
use crate::sentinel::{Killed, Sentinel};
use crate::tags::ReservedTags;
use crate::tree::{Compact, Edge};
pub use program::{Phase, Pipeline};

pub type Tree = crate::tree::Tree<Compact>;
pub type Cursor<'a> = crate::tree::Cursor<'a, Compact>;

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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Pure,
    Flow,
    Effect,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Arg {
    One,
    Set,
}

pub type Call = for<'c, 't> fn(&'c mut Cx<'t>, &[Vals]) -> Vals;

pub struct Function {
    pub name: &'static str,
    pub sig: &'static [Arg],
    pub kind: Kind,
    pub call: Call,
}

pub struct Module {
    pub name: &'static str,
    pub functions: &'static [Function],
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct FnId(u16);

static TABLE: LazyLock<Vec<(&'static str, &'static Function)>> = LazyLock::new(|| {
    modules::ALL
        .iter()
        .flat_map(|m| m.functions.iter().map(move |f| (m.name, f)))
        .collect()
});

pub fn lookup(module: &str, name: &str) -> Option<FnId> {
    let at = TABLE
        .iter()
        .position(|(m, f)| *m == module && f.name == name)?;
    Some(FnId(at as u16))
}

pub fn function(id: FnId) -> &'static Function {
    TABLE[id.0 as usize].1
}

pub fn module_of(id: FnId) -> &'static str {
    TABLE[id.0 as usize].0
}

#[macro_export]
macro_rules! fn_id {
    ($module:literal, $name:literal) => {{
        static ID: std::sync::LazyLock<$crate::engine::FnId> = std::sync::LazyLock::new(|| {
            $crate::engine::lookup($module, $name).expect(concat!($module, ".", $name))
        });
        *ID
    }};
}

type Key = (FnId, SmallVec<[Vals; 2]>);

pub struct Cx<'t> {
    pub trees: &'t [Tree],
    pub fi: u32,
    pub env: &'t Env,
    pub tags: ReservedTags,
    pub run: &'t Sentinel,
    pub file: Sentinel,
    pub killed: Option<Killed>,
    pub max_edges: usize,
    pub edges: Vec<Edge>,
    pub defs: Vec<u32>,
    pub ssa: modules::ssa::State,
    pure: FxHashMap<Key, Vals>,
    flow: FxHashMap<Key, Vals>,
    flow_at: modules::ssa::Epoch,
    busy: FxHashSet<Key>,
    cut: bool,
}

impl<'t> Cx<'t> {
    pub fn new(
        trees: &'t [Tree],
        fi: u32,
        env: &'t Env,
        run: &'t Sentinel,
        file: Sentinel,
    ) -> Self {
        Self {
            trees,
            fi,
            env,
            tags: ReservedTags::new(&env.lang),
            run,
            file,
            killed: None,
            max_edges: env.limits.max_file_edges,
            edges: Vec::new(),
            defs: Vec::new(),
            ssa: modules::ssa::State::default(),
            pure: FxHashMap::default(),
            flow: FxHashMap::default(),
            flow_at: Default::default(),
            busy: FxHashSet::default(),
            cut: false,
        }
    }

    pub fn tree(&self) -> &'t Tree {
        &self.trees[self.fi as usize]
    }

    pub fn at(&self, node: u32) -> Cursor<'t> {
        Cursor::new(self.trees, self.fi, node)
    }

    pub fn jump(&self, loc: Loc) -> Cursor<'t> {
        Cursor::new(self.trees, loc.fi, loc.node)
    }

    pub fn check(&self) -> Result<(), Killed> {
        self.run.check().and_then(|()| self.file.check())?;
        match self.max_edges {
            0 => Ok(()),
            cap if self.edges.len() <= cap => Ok(()),
            cap => Err(Killed::limit(
                "edges",
                &self.tree().label,
                self.edges.len(),
                cap,
            )),
        }
    }

    pub fn step(&mut self) -> bool {
        if self.killed.is_none()
            && let Err(killed) = self.check()
        {
            self.killed = Some(killed);
        }
        self.killed.is_none()
    }

    /// Cached calls share one cycle guard: a call already in progress yields
    /// nothing and marks the result in progress as cut. A pure result under a
    /// cut is never stored; a flow result lives one epoch, so it is stored to
    /// keep self-referential chains (`n = n.next()`) linear.
    pub fn call(&mut self, id: FnId, args: &[Vals]) -> Vals {
        let f = function(id);
        if f.kind == Kind::Effect {
            return (f.call)(self, args);
        }
        let key: Key = (id, SmallVec::from(args));
        if f.kind == Kind::Flow {
            let epoch = self.ssa.epoch(self.defs.len());
            if epoch != self.flow_at {
                self.flow.clear();
                self.flow_at = epoch;
            }
        }
        let memo = match f.kind {
            Kind::Pure => &self.pure,
            _ => &self.flow,
        };
        if let Some(found) = memo.get(&key) {
            return found.clone();
        }
        if !self.busy.insert(key.clone()) {
            self.cut = true;
            return Vals::new();
        }
        let outer = std::mem::take(&mut self.cut);
        let found = stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || (f.call)(self, args));
        self.busy.remove(&key);
        let cut = self.cut;
        self.cut |= outer;
        match f.kind {
            Kind::Pure if !cut => {
                self.pure.insert(key, found.clone());
            }
            Kind::Flow if self.flow_at == self.ssa.epoch(self.defs.len()) => {
                self.flow.insert(key, found.clone());
            }
            _ => {}
        }
        found
    }

    pub fn call1(&mut self, id: FnId, arg: Val) -> Vals {
        self.call(id, &[SmallVec::from_elem(arg, 1)])
    }
}
