mod compile;
mod expr;
mod graph;
pub mod lang;
mod machine;
pub mod pattern;
pub mod profile;
mod program;
mod walk;

use std::any::Any;
use std::marker::PhantomData;
use std::sync::LazyLock;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use crate::env::Env;
use crate::modules;
use crate::sentinel::{Killed, Sentinel};
use crate::tree::{Compact, Edge, EdgeKind, Mutable, Node, Storage};
pub use graph::Graph;
pub use program::{Def, Part, Phase, Pipeline, RowShape, resolution};
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

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Kind {
    Pure,
    /// Pure and cheap: called each time, never cached.
    Inline,
    Flow,
    Effect,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Arg {
    One,
    Set,
    /// An edge kind's name, a number once the rules load.
    Edge,
    /// `'pattern' [by key]`: a declared index, its id once the rules load.
    Pattern,
    /// A row shape's name, a number once the rules load; the call's named
    /// arguments then give the shape's values, in its order.
    Shape,
    /// Any further sets, last in a signature.
    Rest,
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
    /// The node kinds the module recognizes, by role; pipeline.yaml binds each
    /// role to a kind of the vocabulary.
    const KINDS: &'static [&'static str] = &[];
    /// The tags the module reads, by role, bound the same way.
    const TAGS: &'static [&'static str] = &[];
}

/// The anchor under which a piece files every match of a tree.
pub const WHOLE: u32 = u32::MAX;

/// What a run needs to build index pieces on demand, and the run's
/// family-wide directory of each index, merged from the files' pieces.
pub struct Indexed<'t, S: Store> {
    phase: &'t Phase,
    trees: &'t [Tree<S>],
    files: usize,
    env: &'t Env,
    budget: &'t Sentinel,
    family: Box<[std::sync::OnceLock<FxHashMap<u32, Vec<Loc>>>]>,
}

impl<'t, S: Store> Indexed<'t, S> {
    fn new(
        phase: &'t Phase,
        trees: &'t [Tree<S>],
        files: usize,
        env: &'t Env,
        budget: &'t Sentinel,
    ) -> Self {
        Self {
            phase,
            trees,
            files,
            env,
            budget,
            family: (0..phase.index_count)
                .map(|_| std::sync::OnceLock::new())
                .collect(),
        }
    }

    fn piece(&self, fi: u32, id: u32) -> &'t crate::tree::Piece {
        let (trees, env, phase, budget) = (self.trees, self.env, self.phase, self.budget);
        trees[fi as usize]
            .indexes
            .piece(phase.index_count, id as usize, || {
                let rules = env.rules_index(&trees[fi as usize].label);
                let mut run = Run::new(trees, fi, rules, env, phase, budget, budget);
                walk::piece(phase, &mut run, Cursor::new(trees, fi, 0), id)
            })
    }

    fn family(&self, id: u32) -> &FxHashMap<u32, Vec<Loc>> {
        self.family[id as usize].get_or_init(|| {
            let mut map: FxHashMap<u32, Vec<Loc>> = FxHashMap::default();
            for fi in 0..self.files as u32 {
                for (&(anchor, key), nodes) in self.piece(fi, id) {
                    if anchor == WHOLE {
                        map.entry(key)
                            .or_default()
                            .extend(nodes.iter().map(|&node| Loc { fi, node }));
                    }
                }
            }
            map
        })
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
    pub(crate) kinds: &'static [&'static str],
    pub(crate) tags: &'static [&'static str],
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
        kinds: M::KINDS,
        tags: M::TAGS,
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

impl Entry {
    /// The kind of argument `at`; past the signature, its last.
    pub fn arg(&self, at: usize) -> Arg {
        self.sig[at.min(self.sig.len().saturating_sub(1))]
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct FnId(u16);

/// What a phase resolves once at load: the function each call runs, with
/// abstract functions replaced by their providers, and the states each
/// function can read through its calls. Any state read makes it Flow.
/// How a phase wires required functions: to another module's function, or
/// to a YAML definition.
pub struct Links {
    to: Vec<FnId>,
    defs: Vec<Option<u16>>,
    deps: Vec<u8>,
}

impl Links {
    pub fn load(
        binds: &[(FnId, FnId)],
        defs: &[(FnId, u16)],
    ) -> Result<Self, crate::error::LoadError> {
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
            f if f.kind == Kind::Flow && !f.required => 1 << f.slot,
            _ => 0,
        };
        let mut deps: Vec<u8> = ids()
            .map(|id| match defs.iter().any(|&(of, _)| of == id) {
                true => u8::MAX,
                false => own(id),
            })
            .collect();
        let defs = ids()
            .map(|id| defs.iter().find_map(|&(of, d)| (of == id).then_some(d)))
            .collect();
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
                return Ok(Self { to, defs, deps });
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
    assert!(modules::ALL.len() <= SLOTS);
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

/// Module state slots; a dependency set is a `u8` mask over them.
const SLOTS: usize = 8;

type Key = (FnId, SmallVec<[Val; 4]>);

/// A bound on reruns of one cycle; sets only grow, so a cycle over finite
/// trees converges well before it.
const MAX_FIXPOINT_ROUNDS: u32 = 64;

enum Memo {
    Busy(Vals),
    /// A result; `within` holds, for a result that read unfinished calls, the
    /// fixpoint round it belongs to and those calls.
    Done {
        vals: Vals,
        deps: u8,
        stamp: u64,
        within: Option<(u64, Vec<Key>)>,
    },
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
    graph: Option<&'t Graph>,
    files: u32,
    project: Option<u32>,
    indexed: Option<&'t Indexed<'t, S>>,
    fns: &'t [program::Def],
    overrides: Option<&'t std::collections::HashMap<u16, u16>>,
    enclosing_kinds: &'t [u16],
    graph_fi: u32,
    lang: usize,
    data: &'t [Vals],
    edges: Vec<Edge>,
    rows: Vec<Row>,
    /// The tree writes of the node being fired, applied once it is done.
    writes: Vec<Write>,
    states: Box<[Box<dyn Any>]>,
    epochs: [u64; SLOTS],
    reading: u8,
    memo: Table,
    reads: FxHashSet<Key>,
    /// Bumped each time a cycle head reruns, retiring results of the last round.
    round: u64,
    profile: Option<Box<profile::Profile>>,
    roles: &'t [program::Roles],
    phase_modules: &'t [u8],
    /// The evaluator's stacks, reused by each evaluation of this run.
    machine: Option<machine::Machine<'t, S>>,
    /// Each node's nearest ancestor that is a scope or the root, filled on use.
    scopes: FxHashMap<Loc, Option<Loc>>,
}

impl<'t, S: Store> Run<'t, S> {
    fn new(
        trees: &'t [Tree<S>],
        fi: u32,
        rules: usize,
        env: &'t Env,
        phase: &'t Phase,
        budget: &'t Sentinel,
        file: &'t Sentinel,
    ) -> Self {
        let mut run = Self::detached(trees, fi, &env.lang, &phase.links, budget, file);
        run.max_edges = env.limits.max_file_edges;
        run.lang = rules;
        run.data = phase.data.get(rules).map_or(&[], Vec::as_slice);
        run.fns = &phase.defs;
        run.overrides = phase.overrides.get(rules);
        run.enclosing_kinds = &phase.enclosing;
        run.roles = &phase.roles;
        run.phase_modules = &phase.modules;
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
            indexed: None,
            fns: &[],
            overrides: None,
            enclosing_kinds: &[],
            graph_fi: 0,
            lang: 0,
            data: &[],
            edges: Vec::new(),
            rows: Vec::new(),
            writes: Vec::new(),
            states: modules::ALL.iter().map(|m| (m.state)()).collect(),
            epochs: [0; SLOTS],
            reading: u8::MAX,
            memo: Table::default(),
            reads: FxHashSet::default(),
            round: 0,
            profile: profile::ON.then(Box::default),
            roles: &[],
            phase_modules: &[],
            machine: None,
            scopes: FxHashMap::default(),
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

    fn stamp(&self, deps: u8) -> u64 {
        stamp(&self.epochs, deps)
    }

    fn key(&self, id: FnId, f: &Entry, args: &[Vals]) -> Key {
        let mut key = SmallVec::new();
        let arg = |key: &mut SmallVec<[Val; 4]>, at: usize| {
            if matches!(f.arg(at), Arg::Set | Arg::Rest) {
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
            Memo::Busy(_) => true,
            Memo::Done {
                deps,
                stamp: at,
                within,
                ..
            } => within.is_none() && stamp(epochs, *deps) == *at,
        });
        table.swept = table.entries.len();
    }

    /// A module function's result, cached; it stays valid until a state the
    /// function can read is written.
    fn call(&mut self, id: FnId, args: &[Vals]) -> Vals {
        machine::call(self, id, args)
    }

    /// Starts the phase's modules on the run's tree.
    fn start_modules(&mut self) {
        for &slot in self.phase_modules {
            (S::hooks(&modules::ALL[usize::from(slot)]).start)(self, slot, 0);
        }
    }

    /// Finishes the phase's modules; the edges, without repeats, and rows
    /// the run wrote, unless it was stopped.
    fn end(mut self) -> Result<(Vec<Edge>, Vec<Row>), Killed> {
        if let Some(killed) = self.killed.take() {
            return Err(killed);
        }
        for &slot in self.phase_modules.iter().rev() {
            (S::hooks(&modules::ALL[usize::from(slot)]).finish)(&mut self, slot);
        }
        if let Some(killed) = self.killed.take() {
            return Err(killed);
        }
        let mut seen = FxHashSet::default();
        self.edges
            .retain(|e| seen.insert((e.from(), e.to(), e.kind, e.site, e.call_resolution)));
        Ok((
            std::mem::take(&mut self.edges),
            std::mem::take(&mut self.rows),
        ))
    }

    pub(crate) fn profiling(&self) -> bool {
        self.profile.is_some()
    }

    pub(crate) fn profile_rule(
        &mut self,
        id: usize,
        src: &str,
        fired: bool,
        matching: std::time::Duration,
        total: std::time::Duration,
    ) {
        if let Some(p) = &mut self.profile {
            p.rule(id, src, fired, matching, total);
        }
    }

    fn scope_above(&mut self, at: Loc) -> Option<Loc> {
        let kinds = self.enclosing_kinds;
        let mut path = Vec::new();
        let mut c = Cursor::new(self.trees, at.fi, at.node);
        let found = loop {
            if let Some(&known) = self.scopes.get(&Loc::from(c)) {
                break known;
            }
            path.push(Loc::from(c));
            match c.parent() {
                None => break None,
                Some(p) if kinds.contains(&p.kind()) || p.parent().is_none() => {
                    break Some(Loc::from(p));
                }
                Some(p) => c = p,
            }
        };
        for node in path {
            self.scopes.insert(node, found);
        }
        found
    }

    fn fn_name(&self, id: FnId) -> String {
        match TABLE.get(usize::from(id.0)) {
            Some(f) => format!("{}.{}", modules::ALL[usize::from(f.slot)].name, f.name),
            None => self.fns.get(usize::from(u16::MAX - id.0)).map_or_else(
                || format!("def #{}", u16::MAX - id.0),
                |d| d.name.to_string(),
            ),
        }
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

fn stamp(epochs: &[u64; SLOTS], mut deps: u8) -> u64 {
    let mut sum = 0;
    while deps != 0 {
        sum += epochs[deps.trailing_zeros() as usize];
        deps &= deps - 1;
    }
    sum
}

/// Runs a tree phase over one file: each module of the phase starts, the walk
/// runs, then each module finishes in reverse order.
/// A write to the tree a run reads, applied after the node that made it:
/// a tag on `at`, or on the first node of kind `on` under it; or a template
/// built at `at` from the first symbol of each of its sets.
pub enum Write {
    Tag {
        at: u32,
        key: u32,
        on: u16,
        val: u32,
    },
    Build {
        template: u16,
        at: u32,
        syms: Vec<Option<u32>>,
    },
}

/// A row a `row:` hook emitted at `at`: the values of its shape's columns,
/// then its extra id parts.
pub struct Row {
    pub shape: u16,
    pub at: Loc,
    pub values: Box<[Option<Val>]>,
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

    /// The kind ids bound to the module's `KINDS` roles, in their order.
    pub fn kinds(&self) -> &'t [u16] {
        self.run
            .roles
            .get(usize::from(self.slot))
            .map_or(&[], |r| &r.kinds)
    }

    /// The tag ids bound to the module's `TAGS` roles, in their order.
    pub fn tags(&self) -> &'t [u32] {
        self.run
            .roles
            .get(usize::from(self.slot))
            .map_or(&[], |r| &r.tags)
    }

    /// A value's text: a symbol as is, or a node's own text.
    pub fn text(&self, val: Val) -> Option<u32> {
        self.run.text_of(val)
    }

    /// Whether `node` of this tree has an edge of `kind` in the phase's graph.
    pub fn has_edge(&self, kind: EdgeKind, incoming: bool, node: u32) -> bool {
        let at = Loc {
            fi: self.run.graph_fi,
            node,
        };
        self.run.graph.is_some_and(|g| g.has(kind, incoming, at))
    }

    /// The targets of `from`'s edges of `kind` in the graph so far.
    pub fn targets(&self, kind: EdgeKind, from: Loc) -> &'t [Loc] {
        self.run.graph.map_or(&[], |g| g.targets(kind, from))
    }

    /// The targets of the edges of `kind` from `from` whose call resolution is unknown.
    pub fn unresolved_targets(&self, kind: EdgeKind, from: Loc) -> &'t [Loc] {
        self.run.graph.map_or(&[], |g| g.unresolved(kind, from))
    }

    /// The edges of `kind` made at `site`, as (from, to).
    pub fn at_site(&self, kind: EdgeKind, site: Loc) -> &'t [(Loc, Loc)] {
        self.run.graph.map_or(&[], |g| g.at_site(kind, site))
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

    pub fn emit(&mut self, edge: Edge) {
        self.run.edges.push(edge);
    }

    pub fn row(&mut self, row: Row) {
        self.run.rows.push(row);
    }

    /// Writes the run's tree once the node being fired is done.
    pub fn write_tree(&mut self, write: Write) {
        self.run.writes.push(write);
    }

    /// The innermost scope above `at`, or none at the root.
    pub fn scope_above(&mut self, at: Loc) -> Option<Loc> {
        self.run.scope_above(at)
    }

    /// One tree's piece of a declared rule index, in a run that has them.
    pub fn piece(&self, fi: u32, id: u32) -> Option<&'t crate::tree::Piece> {
        Some(self.run.indexed?.piece(fi, id))
    }

    /// A declared rule index's whole-tree matches across the family, by key.
    pub fn family(&self, id: u32) -> Option<&'t FxHashMap<u32, Vec<Loc>>> {
        Some(self.run.indexed?.family(id))
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

impl<S: Store> Drop for Run<'_, S> {
    fn drop(&mut self) {
        let Some(mut p) = self.profile.take() else {
            return;
        };
        for ((id, _), memo) in &self.memo.entries {
            let s = p.stats.entry(*id).or_default();
            s.entries += 1;
            if let Memo::Done { vals, .. } = memo {
                s.values += vals.len() as u64;
            }
        }
        p.merge(|id| self.fn_name(id));
    }
}
