mod expr;
pub mod lang;
pub mod pattern;
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
use crate::tree::{Compact, Edge, EdgeKind, Mutable, Node, Storage};
pub use program::{Def, Part, Phase, Pipeline, RowShape, Walk, resolution};

pub type Tree<S = Compact> = crate::tree::Tree<S>;
pub type Cursor<'a, S = Compact> = crate::tree::Cursor<'a, S>;

/// A tree storage the engine runs over. Each module function is compiled
/// once per storage, so a call dispatches statically.
pub trait Store: Storage<Node = Node> + Sized + 'static {
    fn call<M: Module>(f: &Function<M>) -> Call<M, Self>;
    fn hooks(r: &Registration) -> &Hooks<Self>;
    fn derive(d: &Derive) -> fn(Cursor<'_, Self>) -> u32;
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
    Of(u8, Derive),
}

#[derive(Clone, Copy)]
pub struct Derive {
    pub compact: fn(Cursor<'_, Compact>) -> u32,
    pub mutable: fn(Cursor<'_, Mutable>) -> u32,
}

pub trait Module: Sized + 'static {
    const NAME: &'static str;
    type State: Default + 'static;
    fn functions() -> &'static [Function<Self>];
    fn start<'t, S: Store>(_cx: &mut Cx<'_, 't, Self, S>, _root: Cursor<'t, S>) {}
    fn finish<S: Store>(_cx: &mut Cx<'_, '_, Self, S>) {}
    fn indexes() -> &'static [IndexDecl] {
        &[]
    }
}

/// An index a module declares over a family's trees and edges. The engine
/// builds it the first time a file of a family run asks for it, and every
/// file of that run shares it.
pub struct IndexDecl {
    pub name: &'static str,
    pub build: fn(&crate::intern::Lang, &[Tree<Compact>], &[Edge]) -> Box<dyn Any + Send + Sync>,
}

/// The entries a family phase's `index:` rules add, by index name and key.
#[derive(Default)]
pub struct Indexed(FxHashMap<(u32, u32), Vec<Loc>>);

impl Indexed {
    fn build(
        phase: &Phase,
        trees: &[Tree<Compact>],
        files: usize,
        env: &Env,
        budget: &Sentinel,
    ) -> Self {
        use rayon::prelude::*;
        if !phase.has_indexers() {
            return Self::default();
        }
        let entries: Vec<Vec<(u32, u32, Loc)>> = (0..files)
            .into_par_iter()
            .map(|fi| {
                let mut run = Run::new(trees, fi as u32, env, phase, budget, budget);
                phase.index_entries(&mut run, Cursor::new(trees, fi as u32, 0))
            })
            .collect();
        let mut map: FxHashMap<(u32, u32), Vec<Loc>> = FxHashMap::default();
        for (name, key, at) in entries.into_iter().flatten() {
            map.entry((name, key)).or_default().push(at);
        }
        Self(map)
    }
}

/// The declared indexes of one family run, each built at most once.
pub struct Indexes<'t> {
    lang: &'t crate::intern::Lang,
    trees: &'t [Tree<Compact>],
    edges: &'t [Edge],
    built: Box<[std::sync::OnceLock<Box<dyn Any + Send + Sync>>]>,
}

static INDEXES: LazyLock<Vec<(u8, &'static IndexDecl)>> = LazyLock::new(|| {
    modules::ALL
        .iter()
        .enumerate()
        .flat_map(|(slot, m)| (m.indexes)().iter().map(move |d| (slot as u8, d)))
        .collect()
});

impl<'t> Indexes<'t> {
    pub fn new(
        lang: &'t crate::intern::Lang,
        trees: &'t [Tree<Compact>],
        edges: &'t [Edge],
    ) -> Self {
        Self {
            lang,
            trees,
            edges,
            built: (0..INDEXES.len())
                .map(|_| std::sync::OnceLock::new())
                .collect(),
        }
    }

    fn get(&self, slot: u8, name: &str) -> Option<&(dyn Any + Send + Sync)> {
        let at = INDEXES
            .iter()
            .position(|(s, d)| *s == slot && d.name == name)?;
        let built =
            self.built[at].get_or_init(|| (INDEXES[at].1.build)(self.lang, self.trees, self.edges));
        Some(built.as_ref())
    }
}

pub type Call<M, S> = for<'r, 't> fn(&mut Cx<'r, 't, M, S>, &[Vals]) -> Vals;

pub struct Calls<M: Module> {
    pub compact: Call<M, Compact>,
    pub mutable: Call<M, Mutable>,
}

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
    pub call: Calls<M>,
}

pub struct Hooks<S: Store> {
    invoke: fn(&mut Run<S>, u8, u16, &[Vals]) -> Vals,
    start: fn(&mut Run<S>, u8, u32),
    finish: fn(&mut Run<S>, u8),
}

pub struct Registration {
    name: &'static str,
    state: fn() -> Box<dyn Any>,
    describe: fn(u8) -> Vec<Entry>,
    compact: Hooks<Compact>,
    mutable: Hooks<Mutable>,
    indexes: fn() -> &'static [IndexDecl],
}

pub const fn register<M: Module>() -> Registration {
    Registration {
        name: M::NAME,
        state: new_state::<M>,
        describe: describe::<M>,
        compact: Hooks {
            invoke: invoke::<M, Compact>,
            start: start::<M, Compact>,
            finish: finish::<M, Compact>,
        },
        mutable: Hooks {
            invoke: invoke::<M, Mutable>,
            start: start::<M, Mutable>,
            finish: finish::<M, Mutable>,
        },
        indexes: M::indexes,
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

fn invoke<M: Module, S: Store>(run: &mut Run<S>, slot: u8, index: u16, args: &[Vals]) -> Vals {
    (S::call(&M::functions()[usize::from(index)]))(&mut Cx::new(run, slot), args)
}

fn start<M: Module, S: Store>(run: &mut Run<S>, slot: u8, root: u32) {
    let mut cx = Cx::new(run, slot);
    let root = cx.at(root);
    M::start(&mut cx, root);
}

fn finish<M: Module, S: Store>(run: &mut Run<S>, slot: u8) {
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
pub struct Run<'t, S: Store = Compact> {
    trees: &'t [Tree<S>],
    fi: u32,
    pub(crate) lang_ref: &'t crate::intern::Lang,
    links: &'t Links,
    budget: &'t Sentinel,
    file: &'t Sentinel,
    killed: Option<Killed>,
    max_edges: usize,
    graph: Option<&'t EdgeIndex>,
    files: u32,
    project: Option<u32>,
    indexes: Option<&'t Indexes<'t>>,
    indexed: Option<&'t Indexed>,
    declared: &'t [u32],
    fns: &'t [program::Def],
    graph_fi: u32,
    lang: usize,
    data: &'t [Vals],
    edges: Vec<Edge>,
    rows: Vec<Row>,
    defs: Vec<u32>,
    states: Box<[Box<dyn Any>]>,
    epochs: [u64; WALK + 1],
    reading: u8,
    memo: Table,
    cut: bool,
}

impl<'t, S: Store> Run<'t, S> {
    fn new(
        trees: &'t [Tree<S>],
        fi: u32,
        env: &'t Env,
        phase: &'t Phase,
        budget: &'t Sentinel,
        file: &'t Sentinel,
    ) -> Self {
        let rules = env.rules_index(&trees[fi as usize].label);
        let mut run = Self::detached(trees, fi, &env.lang, &phase.links, budget, file);
        run.max_edges = env.limits.max_file_edges;
        run.lang = rules;
        run.data = phase.data.get(rules).map_or(&[], Vec::as_slice);
        run.declared = &phase.indexes;
        run.fns = &phase.defs;
        run
    }

    /// A run with no language rules, data or edge cap, for evaluating
    /// expressions outside a phase walk.
    pub fn detached(
        trees: &'t [Tree<S>],
        fi: u32,
        lang: &'t crate::intern::Lang,
        links: &'t Links,
        budget: &'t Sentinel,
        file: &'t Sentinel,
    ) -> Self {
        Self {
            trees,
            fi,
            lang_ref: lang,
            links,
            budget,
            file,
            killed: None,
            max_edges: 0,
            graph: None,
            files: trees.len() as u32,
            project: None,
            indexes: None,
            indexed: None,
            declared: &[],
            fns: &[],
            graph_fi: 0,
            lang: 0,
            data: &[],
            edges: Vec::new(),
            rows: Vec::new(),
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

    /// A node's text as a symbol: its own symbol, or its source text.
    /// A value's text: a symbol as is, or a node's own text.
    pub(crate) fn text_of(&self, val: Val) -> Option<u32> {
        match val {
            Val::Sym(sym) => Some(sym),
            Val::Node(loc) | Val::Type(loc) => Some(self.sym_at(loc)).filter(|&s| s != 0),
            _ => None,
        }
    }

    pub(crate) fn sym_at(&self, loc: Loc) -> u32 {
        self.trees[loc.fi as usize].sym_at(loc.node, self.lang_ref)
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
                    let derived =
                        loc.map(|l| S::derive(&derive)(Cursor::new(self.trees, l.fi, l.node)));
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
        let invoke = S::hooks(&modules::ALL[usize::from(f.slot)]).invoke;
        let reading = std::mem::replace(&mut self.reading, deps);
        if f.kind == Kind::Effect {
            self.reading = u8::MAX;
            let found = invoke(self, f.slot, f.index, args);
            self.reading = reading;
            return found;
        }
        let key = self.key(id, f, args);
        let found = self.memoized(key, deps, |run| invoke(run, f.slot, f.index, args));
        self.reading = reading;
        found
    }

    /// A YAML function's result, cached like a module function's.
    pub(crate) fn call_def(&mut self, def: u16, args: &[Vals]) -> Vals {
        let defs = self.fns;
        let deps = defs[usize::from(def)].deps;
        let key = defs[usize::from(def)].key(self, args);
        let reading = std::mem::replace(&mut self.reading, deps);
        let found = self.memoized((FnId(u16::MAX - def), key), deps, |run| {
            defs[usize::from(def)].eval(run, args)
        });
        self.reading = reading;
        found
    }

    /// Cached calls share one cycle guard: a call already in progress yields
    /// nothing and marks the result in progress as cut. A cut Pure result is
    /// never stored; a cut Flow result lives until its states change.
    fn memoized(&mut self, key: Key, deps: u8, compute: impl FnOnce(&mut Self) -> Vals) -> Vals {
        match self.memo.entries.get(&key) {
            Some(Memo::Busy) => {
                self.cut = true;
                return Vals::new();
            }
            Some(Memo::Done { vals, deps, stamp }) if self.stamp(*deps) == *stamp => {
                return vals.clone();
            }
            _ => {}
        }
        self.sweep();
        self.memo.entries.insert(key.clone(), Memo::Busy);
        let epochs = self.epochs;
        let outer = std::mem::take(&mut self.cut);
        let found = stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || compute(self));
        let cut = self.cut;
        self.cut |= outer;
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
    pub(crate) fn deps_of(&self, id: FnId) -> u8 {
        self.deps[usize::from(id.0)]
    }

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
pub fn run_tree<S: Store>(
    phase: &Phase,
    tree: &Tree<S>,
    env: &Env,
    budget: &Sentinel,
) -> Result<Vec<Edge>, Killed> {
    let file = &tree_budget(phase, env, &tree.label);
    let mut run = Run::new(std::slice::from_ref(tree), 0, env, phase, budget, file);
    let root = tree.root();
    for &slot in &phase.modules {
        (S::hooks(&modules::ALL[usize::from(slot)]).start)(&mut run, slot, root.index());
    }
    let late = phase.flow(&mut run, root);
    for &slot in phase.modules.iter().rev() {
        if let Some(killed) = run.killed.take() {
            return Err(killed);
        }
        (S::hooks(&modules::ALL[usize::from(slot)]).finish)(&mut run, slot);
    }
    phase.revisit(&mut run, root, late);
    match run.killed.take() {
        Some(killed) => Err(killed),
        None => Ok(run.finish()),
    }
}

/// The phase's per-tree budget, on thread CPU time; unbounded when the phase
/// declares none.
fn tree_budget(phase: &Phase, env: &Env, label: &str) -> Sentinel {
    let ms = phase
        .budget
        .tree
        .as_deref()
        .and_then(|name| env.limits.ms(name))
        .unwrap_or(u64::MAX);
    Sentinel::new(phase.label, label, ms).on_thread_cpu()
}

/// A phase's per-scope budget, a child of `run`.
pub fn scope_budget(phase: &Phase, env: &Env, run: &Sentinel) -> Sentinel {
    let ms = phase
        .budget
        .scope
        .as_deref()
        .and_then(|name| env.limits.ms(name))
        .unwrap_or(u64::MAX);
    run.child(phase.label, "", ms)
}

/// Runs a family phase: the phase's walk on each file in parallel, over all
/// the family's trees and the graph so far. Each file has its own memo.
pub fn run_family(
    phase: &Phase,
    trees: &[Tree<Compact>],
    project: Option<u32>,
    edges: &[Edge],
    graph: &EdgeIndex,
    env: &Env,
    budget: &Sentinel,
) -> Result<Vec<Edge>, Killed> {
    use rayon::prelude::*;
    let scope = scope_budget(phase, env, budget);
    let indexes = Indexes::new(&env.lang, trees, edges);
    let files = project.unwrap_or(trees.len() as u32) as usize;
    let indexed = Indexed::build(phase, trees, files, env, &scope);
    let edges: Vec<Result<Vec<Edge>, Killed>> = (0..files)
        .into_par_iter()
        .map(|fi| {
            let file = tree_budget(phase, env, &trees[fi].label);
            let mut run = Run::new(trees, fi as u32, env, phase, &scope, &file);
            run.graph = Some(graph);
            run.graph_fi = fi as u32;
            run.indexes = Some(&indexes);
            run.indexed = Some(&indexed);
            run.files = files as u32;
            run.project = project;
            let root = Cursor::new(trees, fi as u32, 0);
            for &slot in &phase.modules {
                (Compact::hooks(&modules::ALL[usize::from(slot)]).start)(&mut run, slot, 0);
            }
            let late = phase.flow(&mut run, root);
            for &slot in phase.modules.iter().rev() {
                (Compact::hooks(&modules::ALL[usize::from(slot)]).finish)(&mut run, slot);
            }
            phase.revisit(&mut run, root, late);
            match run.killed.take() {
                Some(killed) => Err(killed),
                None => Ok(run.finish()),
            }
        })
        .collect();
    let mut out = Vec::new();
    for result in edges {
        out.extend(result?);
    }
    Ok(out)
}

/// A row a `row:` hook emitted at `at`: the values of its shape's columns,
/// then its extra id parts.
pub struct Row {
    pub shape: u16,
    pub at: Loc,
    pub values: Box<[Option<Val>]>,
}

/// Runs a graph phase: the phase's walk on every tree, the project tree
/// included and each root visited, in parallel with the graph so far.
pub fn run_graph(
    phase: &Phase,
    trees: &[Tree<Compact>],
    project: u32,
    edges: &[Edge],
    env: &Env,
    budget: &Sentinel,
) -> Result<Vec<Row>, Killed> {
    use rayon::prelude::*;
    let scope = scope_budget(phase, env, budget);
    let graph = EdgeIndex::new(edges);
    let indexes = Indexes::new(&env.lang, trees, edges);
    let rows: Vec<Result<Vec<Row>, Killed>> = (0..trees.len())
        .into_par_iter()
        .map(|fi| {
            let file = tree_budget(phase, env, &trees[fi].label);
            let mut run = Run::new(trees, fi as u32, env, phase, &scope, &file);
            run.graph = Some(&graph);
            run.graph_fi = fi as u32;
            run.indexes = Some(&indexes);
            run.files = project;
            run.project = Some(project);
            let root = Cursor::new(trees, fi as u32, 0);
            phase.flow_all(&mut run, root);
            match run.killed.take() {
                Some(killed) => Err(killed),
                None => Ok(run.rows),
            }
        })
        .collect();
    let mut out = Vec::new();
    for result in rows {
        out.extend(result?);
    }
    Ok(out)
}

/// Applies a rewrite phase to one tree in place.
pub fn rewrite_tree(
    phase: &Phase,
    tree: &mut Tree<Mutable>,
    env: &Env,
    budget: &Sentinel,
) -> Result<(), Killed> {
    let file = tree_budget(phase, env, &tree.label);
    phase.rewrite(tree, env, budget, &file)
}

/// Applies a rewrite phase to one tree with the rules at index `rules`.
pub fn rewrite_tree_with(
    phase: &Phase,
    tree: &mut Tree<Mutable>,
    env: &Env,
    rules: usize,
    budget: &Sentinel,
) -> Result<(), Killed> {
    let file = tree_budget(phase, env, &tree.label);
    phase.rewrite_with(tree, env, rules, budget, &file)
}

/// Sets the tags of a tag-only phase on one tree.
pub fn tag_tree<S: Store>(
    phase: &Phase,
    tree: &mut Tree<S>,
    fi: u32,
    env: &Env,
    graph: &EdgeIndex,
    budget: &Sentinel,
) -> Result<(), Killed> {
    let file = tree_budget(phase, env, &tree.label);
    phase.tag(tree, fi, env, graph, budget, &file)
}

/// Which nodes of the graph have an edge of each kind, by direction.
pub struct EdgeIndex {
    incoming: FxHashSet<EdgeKey>,
    outgoing: FxHashMap<EdgeKey, SmallVec<[Loc; 2]>>,
    unresolved: FxHashMap<EdgeKey, SmallVec<[Loc; 2]>>,
    sites: FxHashMap<EdgeKey, SmallVec<[(Loc, Loc); 1]>>,
}

/// An edge kind and the file and node at one end.
type EdgeKey = (EdgeKind, u32, u32);

impl EdgeIndex {
    pub fn new(edges: &[Edge]) -> Self {
        let mut outgoing: FxHashMap<_, SmallVec<[Loc; 2]>> = FxHashMap::default();
        let mut unresolved: FxHashMap<_, SmallVec<[Loc; 2]>> = FxHashMap::default();
        let mut sites: FxHashMap<_, SmallVec<[(Loc, Loc); 1]>> = FxHashMap::default();
        for e in edges {
            let (from, to) = (e.from_loc(), e.to_loc());
            let key = (e.kind, e.from_tree, e.from_node);
            outgoing.entry(key).or_default().push(to);
            if e.call_resolution == crate::tree::CallResolution::Unknown {
                unresolved.entry(key).or_default().push(to);
            }
            if let Some(site) = e.site {
                sites
                    .entry((e.kind, e.from_tree, site))
                    .or_default()
                    .push((from, to));
            }
        }
        Self {
            incoming: edges
                .iter()
                .map(|e| (e.kind, e.to_tree, e.to_node))
                .collect(),
            outgoing,
            unresolved,
            sites,
        }
    }
}

/// The only surface a module sees: navigation, its own state, calls through
/// the engine, edges, and the budget.
pub struct Cx<'r, 't, M: Module, S: Store = Compact> {
    run: &'r mut Run<'t, S>,
    slot: u8,
    module: PhantomData<fn() -> M>,
}

impl<'r, 't, M: Module, S: Store> Cx<'r, 't, M, S> {
    fn new(run: &'r mut Run<'t, S>, slot: u8) -> Self {
        Self {
            run,
            slot,
            module: PhantomData,
        }
    }

    pub fn fi(&self) -> u32 {
        self.run.fi
    }

    pub fn tree(&self) -> &'t Tree<S> {
        &self.run.trees[self.run.fi as usize]
    }

    pub fn at(&self, node: u32) -> Cursor<'t, S> {
        Cursor::new(self.run.trees, self.run.fi, node)
    }

    pub fn jump(&self, loc: Loc) -> Cursor<'t, S> {
        Cursor::new(self.run.trees, loc.fi, loc.node)
    }

    pub fn loc(&self, node: u32) -> Loc {
        Loc {
            fi: self.run.fi,
            node,
        }
    }

    pub fn lang(&self) -> &'t crate::intern::Lang {
        self.run.lang_ref
    }

    /// One of this module's declared indexes, in a family run.
    pub fn index<T: 'static>(&self, name: &str) -> Option<&'t T> {
        self.run.indexes?.get(self.slot, name)?.downcast_ref()
    }

    /// What a YAML `index:` rule filed under `key` in the index `name`.
    pub fn indexed(&self, name: &str, key: u32) -> &'t [Loc] {
        let Some(indexed) = self.run.indexed else {
            return &[];
        };
        let name = self.run.lang_ref.syms.intern(name);
        assert!(
            self.run.declared.contains(&name),
            "index {} is not declared by this phase",
            self.run.lang_ref.syms.resolve(name)
        );
        indexed.0.get(&(name, key)).map_or(&[], Vec::as_slice)
    }

    /// A value's text: a symbol as is, or a node's own text.
    pub fn text(&self, val: Val) -> Option<u32> {
        self.run.text_of(val)
    }

    /// Whether `node` of this tree has an edge of `kind` in the phase's graph.
    pub fn has_edge(&self, kind: EdgeKind, incoming: bool, node: u32) -> bool {
        let key = (kind, self.run.graph_fi, node);
        self.run.graph.is_some_and(|g| match incoming {
            true => g.incoming.contains(&key),
            false => g.outgoing.contains_key(&key),
        })
    }

    /// The targets of `from`'s edges of `kind` in the graph so far.
    pub fn targets(&self, kind: EdgeKind, from: Loc) -> &'t [Loc] {
        self.run
            .graph
            .and_then(|g| g.outgoing.get(&(kind, from.fi, from.node)))
            .map_or(&[], |t| t.as_slice())
    }

    /// The targets of the edges of `kind` from `from` whose call resolution is unknown.
    pub fn unresolved_targets(&self, kind: EdgeKind, from: Loc) -> &'t [Loc] {
        self.run
            .graph
            .and_then(|g| g.unresolved.get(&(kind, from.fi, from.node)))
            .map_or(&[], |t| t.as_slice())
    }

    /// The edges of `kind` whose site is `site` in file `fi`, as (from, to).
    pub fn at_site(&self, kind: EdgeKind, fi: u32, site: u32) -> &'t [(Loc, Loc)] {
        self.run
            .graph
            .and_then(|g| g.sites.get(&(kind, fi, site)))
            .map_or(&[], |t| t.as_slice())
    }

    /// How many files the run covers; file indexes are `0..files()`.
    pub fn files(&self) -> u32 {
        self.run.files
    }

    /// The project tree's root, in a family run.
    pub fn project(&self) -> Option<Cursor<'t, S>> {
        self.run
            .project
            .map(|fi| Cursor::new(self.run.trees, fi, 0))
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
