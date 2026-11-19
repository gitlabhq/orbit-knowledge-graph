mod expr;
pub(crate) mod matching;
mod program;

use std::any::Any;
use std::marker::PhantomData;
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
    /// Passed but left out of the memo key: every value with the same keyed
    /// arguments must give the same result.
    Unkeyed,
}

pub trait Module: Sized + 'static {
    const NAME: &'static str;
    type State: Default + 'static;
    fn functions() -> &'static [Function<Self>];
    fn start<'t>(_cx: &mut Cx<'_, 't, Self>, _root: Cursor<'t>) {}
    fn finish(_cx: &mut Cx<'_, '_, Self>) {}
}

pub type Call<M> = for<'r, 't> fn(&mut Cx<'r, 't, M>, &[Vals]) -> Vals;

pub struct Function<M: Module> {
    pub name: &'static str,
    pub sig: &'static [Arg],
    pub kind: Kind,
    pub call: Call<M>,
}

pub struct Registration {
    name: &'static str,
    state: fn() -> Box<dyn Any>,
    describe: fn(u8) -> Vec<Entry>,
    invoke: fn(&mut Run, u8, u16, &[Vals]) -> Vals,
    start: fn(&mut Run, u8, u32),
    finish: fn(&mut Run, u8),
}

pub const fn register<M: Module>() -> Registration {
    Registration {
        name: M::NAME,
        state: new_state::<M>,
        describe: describe::<M>,
        invoke: invoke::<M>,
        start: start::<M>,
        finish: finish::<M>,
    }
}

fn new_state<M: Module>() -> Box<dyn Any> {
    Box::new(M::State::default())
}

fn describe<M: Module>(slot: u8) -> Vec<Entry> {
    M::functions()
        .iter()
        .enumerate()
        .map(|(index, f)| Entry {
            slot,
            index: index as u16,
            name: f.name,
            sig: f.sig,
            kind: f.kind,
        })
        .collect()
}

fn invoke<M: Module>(run: &mut Run, slot: u8, index: u16, args: &[Vals]) -> Vals {
    (M::functions()[usize::from(index)].call)(&mut Cx::new(run, slot), args)
}

fn start<M: Module>(run: &mut Run, slot: u8, root: u32) {
    let mut cx = Cx::new(run, slot);
    let root = cx.at(root);
    M::start(&mut cx, root);
}

fn finish<M: Module>(run: &mut Run, slot: u8) {
    M::finish(&mut Cx::new(run, slot));
}

pub struct Entry {
    slot: u8,
    index: u16,
    pub name: &'static str,
    pub sig: &'static [Arg],
    pub kind: Kind,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct FnId(u16);

static TABLE: LazyLock<Vec<Entry>> = LazyLock::new(|| {
    assert!(modules::ALL.len() < WALK);
    modules::ALL
        .iter()
        .enumerate()
        .flat_map(|(slot, m)| (m.describe)(slot as u8))
        .collect()
});

pub fn lookup(module: &str, name: &str) -> Option<FnId> {
    let at = TABLE
        .iter()
        .position(|e| modules::ALL[usize::from(e.slot)].name == module && e.name == name)?;
    Some(FnId(at as u16))
}

pub fn function(id: FnId) -> &'static Entry {
    &TABLE[usize::from(id.0)]
}

pub fn module_of(id: FnId) -> &'static str {
    modules::ALL[usize::from(function(id).slot)].name
}

pub fn slot_of(module: &str) -> Option<u8> {
    let at = modules::ALL.iter().position(|m| m.name == module)?;
    Some(at as u8)
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

const WALK: usize = 7;

type Key = (FnId, SmallVec<[Vals; 2]>);

enum Memo {
    Busy,
    Done { vals: Vals, reads: u8, stamp: u64 },
}

/// One phase run over one tree: navigation, the budget, the edges written,
/// one state slot per module, and the memo.
pub struct Run<'t> {
    trees: &'t [Tree],
    fi: u32,
    env: &'t Env,
    tags: ReservedTags,
    budget: &'t Sentinel,
    file: Sentinel,
    killed: Option<Killed>,
    max_edges: usize,
    edges: Vec<Edge>,
    defs: Vec<u32>,
    states: Box<[Box<dyn Any>]>,
    epochs: [u64; WALK + 1],
    reads: u8,
    memo: FxHashMap<Key, Memo>,
    cut: bool,
}

impl<'t> Run<'t> {
    fn new(trees: &'t [Tree], fi: u32, env: &'t Env, budget: &'t Sentinel, file: Sentinel) -> Self {
        Self {
            trees,
            fi,
            env,
            tags: ReservedTags::new(&env.lang),
            budget,
            file,
            killed: None,
            max_edges: env.limits.max_file_edges,
            edges: Vec::new(),
            defs: Vec::new(),
            states: modules::ALL.iter().map(|m| (m.state)()).collect(),
            epochs: [0; WALK + 1],
            reads: 0,
            memo: FxHashMap::default(),
            cut: false,
        }
    }

    fn check(&self) -> Result<(), Killed> {
        self.budget.check().and_then(|()| self.file.check())?;
        match self.max_edges {
            0 => Ok(()),
            cap if self.edges.len() <= cap => Ok(()),
            cap => Err(Killed::limit(
                "edges",
                &self.trees[self.fi as usize].label,
                self.edges.len(),
                cap,
            )),
        }
    }

    fn step(&mut self) -> bool {
        if self.killed.is_none()
            && let Err(killed) = self.check()
        {
            self.killed = Some(killed);
        }
        self.killed.is_none()
    }

    fn enter_def(&mut self, node: u32) {
        self.defs.push(node);
        self.epochs[WALK] += 1;
    }

    fn leave_def(&mut self) {
        self.defs.pop();
        self.epochs[WALK] += 1;
    }

    fn stamp(&self, reads: u8) -> u64 {
        (0..=WALK)
            .filter(|&slot| reads & (1 << slot) != 0)
            .map(|slot| self.epochs[slot])
            .sum()
    }

    /// Cached calls share one cycle guard: a call already in progress yields
    /// nothing and marks the result in progress as cut. A result stays valid
    /// until a state it read is written. A cut result that read no state is
    /// never stored; one that read state lives until that state changes, which
    /// keeps self-referential chains (`n = n.next()`) linear.
    fn call(&mut self, id: FnId, args: &[Vals]) -> Vals {
        let f = function(id);
        let invoke = modules::ALL[usize::from(f.slot)].invoke;
        if f.kind == Kind::Effect {
            return invoke(self, f.slot, f.index, args);
        }
        let keyed = f.sig.iter().zip(args).filter(|(a, _)| **a != Arg::Unkeyed);
        let key: Key = (id, keyed.map(|(_, v)| v.clone()).collect());
        match self.memo.get(&key) {
            Some(Memo::Busy) => {
                self.cut = true;
                return Vals::new();
            }
            Some(Memo::Done { vals, reads, stamp }) if self.stamp(*reads) == *stamp => {
                self.reads |= reads;
                return vals.clone();
            }
            _ => {}
        }
        self.memo.insert(key.clone(), Memo::Busy);
        let epochs = self.epochs;
        let outer = (
            std::mem::take(&mut self.cut),
            std::mem::take(&mut self.reads),
        );
        let found = stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || {
            invoke(self, f.slot, f.index, args)
        });
        let (cut, reads) = (self.cut, self.reads);
        self.cut |= outer.0;
        self.reads |= outer.1;
        if (cut && reads == 0) || epochs != self.epochs {
            self.memo.remove(&key);
        } else {
            let stamp = self.stamp(reads);
            let vals = found.clone();
            self.memo.insert(key, Memo::Done { vals, reads, stamp });
        }
        found
    }

    fn finish(mut self) -> Vec<Edge> {
        let mut seen = FxHashSet::default();
        self.edges
            .retain(|e| seen.insert((e.from(), e.to(), e.kind, e.site, e.call_resolution)));
        self.edges
    }
}

/// Runs a tree phase over one file: each module of the phase starts, the walk
/// runs, then each module finishes in reverse order.
pub fn run_tree(
    phase: &Phase,
    tree: &Tree,
    env: &Env,
    budget: &Sentinel,
    file: Sentinel,
) -> Result<Vec<Edge>, Killed> {
    let mut run = Run::new(std::slice::from_ref(tree), 0, env, budget, file);
    let root = tree.root();
    for &slot in &phase.modules {
        (modules::ALL[usize::from(slot)].start)(&mut run, slot, root.index());
    }
    phase.flow(&mut run, root);
    for &slot in phase.modules.iter().rev() {
        if let Some(killed) = run.killed.take() {
            return Err(killed);
        }
        (modules::ALL[usize::from(slot)].finish)(&mut run, slot);
    }
    match run.killed.take() {
        Some(killed) => Err(killed),
        None => Ok(run.finish()),
    }
}

/// The only surface a module sees: navigation, its own state, calls through
/// the engine, edges, and the budget.
pub struct Cx<'r, 't, M: Module> {
    run: &'r mut Run<'t>,
    slot: u8,
    module: PhantomData<fn() -> M>,
}

impl<'r, 't, M: Module> Cx<'r, 't, M> {
    fn new(run: &'r mut Run<'t>, slot: u8) -> Self {
        Self {
            run,
            slot,
            module: PhantomData,
        }
    }

    pub fn fi(&self) -> u32 {
        self.run.fi
    }

    pub fn tree(&self) -> &'t Tree {
        &self.run.trees[self.run.fi as usize]
    }

    pub fn at(&self, node: u32) -> Cursor<'t> {
        Cursor::new(self.run.trees, self.run.fi, node)
    }

    pub fn jump(&self, loc: Loc) -> Cursor<'t> {
        Cursor::new(self.run.trees, loc.fi, loc.node)
    }

    pub fn loc(&self, node: u32) -> Loc {
        Loc {
            fi: self.run.fi,
            node,
        }
    }

    pub fn env(&self) -> &'t Env {
        self.run.env
    }

    pub fn tags(&self) -> ReservedTags {
        self.run.tags
    }

    pub fn state(&mut self) -> &mut M::State {
        self.run.reads |= 1 << self.slot;
        self.slot_state()
    }

    pub fn write(&mut self) -> &mut M::State {
        self.run.reads |= 1 << self.slot;
        self.run.epochs[usize::from(self.slot)] += 1;
        self.slot_state()
    }

    fn slot_state(&mut self) -> &mut M::State {
        self.run.states[usize::from(self.slot)]
            .downcast_mut()
            .expect("each slot holds its module's state")
    }

    pub fn call(&mut self, id: FnId, args: &[Vals]) -> Vals {
        self.run.call(id, args)
    }

    pub fn call1(&mut self, id: FnId, arg: Val) -> Vals {
        self.run.call(id, &[one(arg)])
    }

    pub fn enclosing(&mut self) -> u32 {
        self.enclosing_def().unwrap_or(0)
    }

    pub fn enclosing_def(&mut self) -> Option<u32> {
        self.run.reads |= 1 << WALK;
        self.run.defs.last().copied()
    }

    pub fn emit(&mut self, edge: Edge) {
        self.run.edges.push(edge);
    }

    pub fn emitted(&self) -> usize {
        self.run.edges.len()
    }

    pub fn edges_since(&mut self, from: usize) -> &mut [Edge] {
        &mut self.run.edges[from..]
    }

    pub fn check(&self) -> Result<(), Killed> {
        self.run.check()
    }

    pub fn kill(&mut self, killed: Killed) {
        self.run.killed.get_or_insert(killed);
    }
}
