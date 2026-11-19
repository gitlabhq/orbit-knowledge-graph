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
pub use program::{Phase, Pipeline, resolution};

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
}

/// One part of a cache key: an argument, or a value derived from a node
/// argument. Calls with equal keys must have equal results.
#[derive(Clone, Copy)]
pub enum KeyPart {
    Arg(u8),
    Of(u8, fn(Cursor) -> u32),
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
    /// The cache key; every argument when empty.
    pub key: &'static [KeyPart],
    /// The functions this one calls, as `(module, function)`.
    pub uses: &'static [(&'static str, &'static str)],
    /// Required, not implemented: each phase binds a provider to it.
    pub required: bool,
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
            key: f.key,
            uses: f.uses,
            required: f.required,
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
    key: &'static [KeyPart],
    uses: &'static [(&'static str, &'static str)],
    pub required: bool,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct FnId(u16);

/// What a phase resolves once at load: the function each call runs, with
/// abstract functions replaced by their providers, and the states each
/// function can read through its calls. Any state read makes it Flow.
pub struct Links {
    to: Vec<FnId>,
    deps: Vec<u8>,
}

impl Links {
    pub fn load(binds: &[(FnId, FnId)]) -> Result<Self, crate::error::LoadError> {
        let ids = || (0..TABLE.len() as u16).map(FnId);
        let to: Vec<FnId> = ids()
            .map(|id| {
                binds
                    .iter()
                    .find_map(|&(of, to)| (of == id).then_some(to))
                    .unwrap_or(id)
            })
            .collect();
        let mut uses = Vec::with_capacity(TABLE.len());
        for id in ids() {
            let f = function(id);
            let mut called = Vec::new();
            for &(module, name) in f.uses {
                if module != modules::ALL[usize::from(f.slot)].name {
                    return Err(crate::error::LoadError(format!(
                        "{}.{} uses {module}.{name}: a module calls only its own functions",
                        modules::ALL[usize::from(f.slot)].name,
                        f.name
                    )));
                }
                let used = lookup(module, name).ok_or_else(|| {
                    crate::error::LoadError(format!("{} uses unknown {module}.{name}", f.name))
                })?;
                called.push(to[usize::from(used.0)]);
            }
            uses.push(called);
        }
        let own = |id: FnId| match function(id) {
            f if f.kind == Kind::Flow && !f.required => (1 << f.slot) | (1 << WALK),
            _ => 0,
        };
        let mut deps: Vec<u8> = ids().map(own).collect();
        loop {
            let mut changed = false;
            for id in ids() {
                let i = usize::from(id.0);
                let through = to[i];
                let reached = uses[usize::from(through.0)]
                    .iter()
                    .fold(deps[usize::from(through.0)], |acc, u| {
                        acc | deps[usize::from(u.0)]
                    });
                if reached != deps[i] {
                    deps[i] = reached;
                    changed = true;
                }
            }
            if !changed {
                return Ok(Self { to, deps });
            }
        }
    }

    pub fn kind(&self, id: FnId) -> Kind {
        let f = function(self.to[usize::from(id.0)]);
        match (f.kind, self.deps[usize::from(id.0)]) {
            (Kind::Pure, deps) if deps != 0 => Kind::Flow,
            (kind, _) => kind,
        }
    }
}

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

type Key = (FnId, SmallVec<[Val; 4]>);

enum Memo {
    Busy,
    Done { vals: Vals, deps: u8, stamp: u64 },
}

/// Results by call. An entry that read state stays until a sweep finds that
/// state written; sweeps run when the table doubles, so stale entries cost
/// amortized constant time.
#[derive(Default)]
struct Table {
    entries: FxHashMap<Key, Memo>,
    swept: usize,
}

/// One phase run over one tree: navigation, the budget, the edges written,
/// one state slot per module, and the memo.
pub struct Run<'t> {
    trees: &'t [Tree],
    fi: u32,
    env: &'t Env,
    tags: ReservedTags,
    links: &'t Links,
    budget: &'t Sentinel,
    file: Sentinel,
    killed: Option<Killed>,
    max_edges: usize,
    lang: usize,
    edges: Vec<Edge>,
    defs: Vec<u32>,
    states: Box<[Box<dyn Any>]>,
    epochs: [u64; WALK + 1],
    reading: u8,
    memo: Table,
    cut: bool,
}

impl<'t> Run<'t> {
    fn new(
        trees: &'t [Tree],
        fi: u32,
        env: &'t Env,
        links: &'t Links,
        budget: &'t Sentinel,
        file: Sentinel,
    ) -> Self {
        Self {
            trees,
            fi,
            env,
            links,
            tags: ReservedTags::new(&env.lang),
            budget,
            file,
            killed: None,
            max_edges: env.limits.max_file_edges,
            lang: env.rules_index(&trees[fi as usize].label),
            edges: Vec::new(),
            defs: Vec::new(),
            states: modules::ALL.iter().map(|m| (m.state)()).collect(),
            epochs: [0; WALK + 1],
            reading: u8::MAX,
            memo: Table::default(),
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

    fn stamp(&self, deps: u8) -> u64 {
        stamp(&self.epochs, deps)
    }

    fn key(&self, id: FnId, f: &Entry, args: &[Vals]) -> Key {
        let mut key = SmallVec::new();
        let arg = |key: &mut SmallVec<[Val; 4]>, at: usize| {
            if f.sig[at] == Arg::Set {
                key.push(Val::Sym(args[at].len() as u32));
            }
            key.extend_from_slice(&args[at]);
        };
        if f.key.is_empty() {
            (0..args.len()).for_each(|at| arg(&mut key, at));
        }
        for part in f.key {
            match *part {
                KeyPart::Arg(at) => arg(&mut key, usize::from(at)),
                KeyPart::Of(at, derive) => {
                    let loc = args[usize::from(at)].first().and_then(|v| v.loc());
                    let derived = loc.map(|l| derive(Cursor::new(self.trees, l.fi, l.node)));
                    key.push(Val::Sym(derived.unwrap_or(u32::MAX)));
                }
            }
        }
        (id, key)
    }

    fn sweep(&mut self) {
        let table = &mut self.memo;
        if table.entries.len() < 1024 || table.entries.len() < 2 * table.swept {
            return;
        }
        let epochs = &self.epochs;
        table.entries.retain(|_, memo| match memo {
            Memo::Busy => true,
            Memo::Done {
                deps, stamp: at, ..
            } => stamp(epochs, *deps) == *at,
        });
        table.swept = table.entries.len();
    }

    /// Cached calls share one cycle guard: a call already in progress yields
    /// nothing and marks the result in progress as cut. A result stays valid
    /// until a state its function can read is written. A cut Pure result is
    /// never stored; a cut Flow result lives until its states change, which
    /// keeps self-referential chains (`n = n.next()`) linear.
    fn call(&mut self, id: FnId, args: &[Vals]) -> Vals {
        let links = self.links;
        let (id, deps) = (links.to[usize::from(id.0)], links.deps[usize::from(id.0)]);
        let f = function(id);
        let invoke = modules::ALL[usize::from(f.slot)].invoke;
        let reading = std::mem::replace(&mut self.reading, deps);
        if f.kind == Kind::Effect {
            self.reading = u8::MAX;
            let found = invoke(self, f.slot, f.index, args);
            self.reading = reading;
            return found;
        }
        let key = self.key(id, f, args);
        match self.memo.entries.get(&key) {
            Some(Memo::Busy) => {
                self.cut = true;
                self.reading = reading;
                return Vals::new();
            }
            Some(Memo::Done { vals, deps, stamp }) if self.stamp(*deps) == *stamp => {
                self.reading = reading;
                return vals.clone();
            }
            _ => {}
        }
        self.sweep();
        self.memo.entries.insert(key.clone(), Memo::Busy);
        let epochs = self.epochs;
        let outer = std::mem::take(&mut self.cut);
        let found = stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || {
            invoke(self, f.slot, f.index, args)
        });
        let cut = self.cut;
        self.cut |= outer;
        self.reading = reading;
        if (cut && deps == 0) || epochs != self.epochs {
            self.memo.entries.remove(&key);
        } else {
            let stamp = self.stamp(deps);
            let vals = found.clone();
            self.memo
                .entries
                .insert(key, Memo::Done { vals, deps, stamp });
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

pub fn required(module: &str) -> impl Iterator<Item = FnId> + '_ {
    (0..TABLE.len() as u16)
        .map(FnId)
        .filter(move |&id| function(id).required && module_of(id) == module)
}

impl Links {
    pub(crate) fn to(&self, id: FnId) -> FnId {
        self.to[usize::from(id.0)]
    }
}

fn stamp(epochs: &[u64; WALK + 1], mut deps: u8) -> u64 {
    let mut sum = 0;
    while deps != 0 {
        sum += epochs[deps.trailing_zeros() as usize];
        deps &= deps - 1;
    }
    sum
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
    let mut run = Run::new(
        std::slice::from_ref(tree),
        0,
        env,
        &phase.links,
        budget,
        file,
    );
    let root = tree.root();
    for &slot in &phase.modules {
        (modules::ALL[usize::from(slot)].start)(&mut run, slot, root.index());
    }
    let late = phase.flow(&mut run, root);
    for &slot in phase.modules.iter().rev() {
        if let Some(killed) = run.killed.take() {
            return Err(killed);
        }
        (modules::ALL[usize::from(slot)].finish)(&mut run, slot);
    }
    phase.revisit(&mut run, root, late);
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
        debug_assert!(
            self.run.reading & (1 << self.slot) != 0,
            "{} state read by a function whose uses do not reach it",
            M::NAME
        );
        self.slot_state()
    }

    pub fn write(&mut self) -> &mut M::State {
        self.run.epochs[usize::from(self.slot)] += 1;
        self.slot_state()
    }

    fn slot_state(&mut self) -> &mut M::State {
        self.run.states[usize::from(self.slot)]
            .downcast_mut()
            .expect("each slot holds its module's state")
    }

    pub fn call(&mut self, id: FnId, args: &[Vals]) -> Vals {
        assert_eq!(
            function(id).slot,
            self.slot,
            "{} called {}.{}: a module calls only its own functions",
            M::NAME,
            module_of(id),
            function(id).name
        );
        self.run.call(id, args)
    }

    pub fn call1(&mut self, id: FnId, arg: Val) -> Vals {
        self.call(id, &[one(arg)])
    }

    pub fn enclosing(&mut self) -> u32 {
        self.enclosing_def().unwrap_or(0)
    }

    pub fn enclosing_def(&mut self) -> Option<u32> {
        debug_assert!(
            self.run.reading & (1 << WALK) != 0,
            "walk read outside Flow"
        );
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
