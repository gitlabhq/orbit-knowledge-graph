//! The module host API: modules, their functions, the registry that links them, and the context a function runs in.

use std::any::Any;
use std::marker::PhantomData;

use rustc_hash::FxHashMap;

use super::*;
use crate::sentinel::Killed;
use crate::tree::{Compact, Edge, EdgeKind, Mutable};

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
    pub(super) invoke: fn(&mut Run<S>, u8, u16, &[Vals]) -> Vals,
    pub(super) start: fn(&mut Run<S>, u8, u32),
    pub(super) finish: fn(&mut Run<S>, u8),
}

/// A module that keeps what rules write to it until the engine collects it:
/// its output from each run, merged across a phase's runs.
pub trait Backend: Module {
    type Output: Default + Send + 'static;
    fn take(state: &mut Self::State) -> Self::Output;
    fn merge(into: &mut Self::Output, from: Self::Output);
}

pub(super) type Output = Box<dyn Any + Send>;

/// How the engine collects a backend's output, untyped.
#[derive(Clone, Copy)]
pub(super) struct Collect {
    pub(super) take: fn(&mut dyn Any) -> Output,
    pub(super) merge: fn(&mut Output, Output),
}

/// What a phase's runs left in each mounted backend.
#[derive(Default)]
pub struct Outputs(pub(super) Vec<(&'static str, Collect, Output)>);

impl Outputs {
    pub(super) fn merge(&mut self, from: Outputs) {
        for (name, collect, output) in from.0 {
            match self.0.iter_mut().find(|(n, _, _)| *n == name) {
                Some((_, collect, into)) => (collect.merge)(into, output),
                None => self.0.push((name, collect, output)),
            }
        }
    }

    /// Backend `B`'s merged output; empty if the phase wrote none.
    pub fn take<B: Backend>(&mut self) -> B::Output {
        match self.0.iter().position(|(n, _, _)| *n == B::NAME) {
            Some(at) => *self
                .0
                .swap_remove(at)
                .2
                .downcast::<B::Output>()
                .expect("a backend's output has its type"),
            None => B::Output::default(),
        }
    }
}

pub(super) fn take_output<B: Backend>(state: &mut dyn Any) -> Output {
    let state = state
        .downcast_mut::<B::State>()
        .expect("each slot holds its module's state");
    Box::new(B::take(state))
}

pub(super) fn merge_output<B: Backend>(into: &mut Output, from: Output) {
    let into = into
        .downcast_mut::<B::Output>()
        .expect("a backend's output has its type");
    let from = *from
        .downcast::<B::Output>()
        .expect("a backend's output has its type");
    B::merge(into, from);
}

pub struct Registration {
    pub(super) collect: Option<Collect>,
    pub(super) name: &'static str,
    pub(super) state: fn() -> Box<dyn Any>,
    pub(super) describe: fn(u8) -> Vec<Entry>,
    pub(super) compact: Hooks<Compact>,
    pub(super) mutable: Hooks<Mutable>,
    pub(crate) kinds: &'static [&'static str],
    pub(crate) tags: &'static [&'static str],
}

/// Mounts backend `B`: a module whose output the engine collects.
pub const fn register_backend<B: Backend>() -> Registration {
    Registration {
        collect: Some(Collect {
            take: take_output::<B>,
            merge: merge_output::<B>,
        }),
        ..register::<B>()
    }
}

pub const fn register<M: Module>() -> Registration {
    Registration {
        collect: None,
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

pub(super) fn new_state<M: Module>() -> Box<dyn Any> {
    Box::new(M::State::default())
}

pub(super) fn describe<M: Module>(slot: u8) -> Vec<Entry> {
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

pub(super) fn invoke<M: Module, S: Store>(
    run: &mut Run<S>,
    slot: u8,
    index: u16,
    args: &[Vals],
) -> Vals {
    (S::call(&M::functions()[usize::from(index)]))(&mut Cx::new(run, slot), args)
}

pub(super) fn start<M: Module, S: Store>(run: &mut Run<S>, slot: u8, root: u32) {
    let mut cx = Cx::new(run, slot);
    let root = cx.at(root);
    M::start(&mut cx, root);
}

pub(super) fn finish<M: Module, S: Store>(run: &mut Run<S>, slot: u8) {
    M::finish(&mut Cx::new(run, slot));
}

pub struct Entry {
    pub(super) slot: u8,
    pub(super) index: u16,
    pub name: &'static str,
    pub sig: &'static [Arg],
    pub kind: Kind,
    pub(super) key: &'static [KeyPart],
    pub(super) uses: &'static [(&'static str, &'static str)],
    pub required: bool,
}

impl Entry {
    /// The kind of argument `at`; past the signature, its last.
    pub fn arg(&self, at: usize) -> Arg {
        self.sig[at.min(self.sig.len().saturating_sub(1))]
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct FnId(pub(super) u16);

/// What a phase resolves once at load: the function each call runs, with
/// abstract functions replaced by their providers, and the states each
/// function can read through its calls. Any state read makes it Flow.
/// How a phase wires required functions: to another module's function, or
/// to a YAML definition.
pub struct Links {
    pub(super) to: Vec<FnId>,
    pub(super) defs: Vec<Option<u16>>,
    pub(super) deps: Vec<u64>,
}

impl Links {
    pub fn load(
        registry: &Registry,
        binds: &[(FnId, FnId)],
        defs: &[(FnId, u16)],
    ) -> Result<Self, crate::error::LoadError> {
        let ids = || (0..registry.len() as u16).map(FnId);
        let to: Vec<FnId> = ids()
            .map(|id| {
                binds
                    .iter()
                    .find_map(|&(of, to)| (of == id).then_some(to))
                    .unwrap_or(id)
            })
            .collect();
        let mut uses = Vec::with_capacity(registry.len());
        for id in ids() {
            let f = registry.function(id);
            let mut called = Vec::new();
            for &(module, name) in f.uses {
                if module != registry.module_of(id) {
                    return Err(crate::error::LoadError(format!(
                        "{}.{} uses {module}.{name}: a module calls only its own functions",
                        registry.module_of(id),
                        f.name
                    )));
                }
                let used = registry.lookup(module, name).ok_or_else(|| {
                    crate::error::LoadError(format!("{} uses unknown {module}.{name}", f.name))
                })?;
                called.push(to[usize::from(used.0)]);
            }
            uses.push(called);
        }
        let own = |id: FnId| match registry.function(id) {
            f if f.kind == Kind::Flow && !f.required => 1u64 << f.slot,
            _ => 0,
        };
        let every = 1u64
            .checked_shl(registry.modules.len() as u32)
            .map_or(u64::MAX, |bit| bit - 1);
        let mut deps: Vec<u64> = ids()
            .map(|id| match defs.iter().any(|&(of, _)| of == id) {
                true => every,
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

    pub fn kind(&self, registry: &Registry, id: FnId) -> Kind {
        let f = registry.function(self.to[usize::from(id.0)]);
        match (f.kind, self.deps[usize::from(id.0)]) {
            (Kind::Pure, deps) if deps != 0 => Kind::Flow,
            (kind, _) => kind,
        }
    }
}

/// A mounted module list and its functions, numbered in module order.
pub struct Registry {
    pub(super) modules: &'static [Registration],
    pub(super) table: Vec<Entry>,
    /// Each module's first function id.
    pub(super) starts: Vec<u16>,
}

impl Registry {
    pub fn new(modules: &'static [Registration]) -> Self {
        assert!(modules.len() <= SLOTS);
        let mut table = Vec::new();
        let mut starts = Vec::with_capacity(modules.len());
        for (slot, m) in modules.iter().enumerate() {
            starts.push(table.len() as u16);
            table.extend((m.describe)(slot as u8));
        }
        Self {
            modules,
            table,
            starts,
        }
    }

    pub fn lookup(&self, module: &str, name: &str) -> Option<FnId> {
        let at = self
            .table
            .iter()
            .position(|e| self.modules[usize::from(e.slot)].name == module && e.name == name)?;
        Some(FnId(at as u16))
    }

    pub fn function(&self, id: FnId) -> &Entry {
        &self.table[usize::from(id.0)]
    }

    pub fn module_of(&self, id: FnId) -> &'static str {
        self.modules[usize::from(self.function(id).slot)].name
    }

    pub fn slot_of(&self, module: &str) -> Option<u8> {
        let at = self.modules.iter().position(|m| m.name == module)?;
        Some(at as u8)
    }

    pub fn module(&self, slot: u8) -> &'static Registration {
        &self.modules[usize::from(slot)]
    }

    /// How many functions the mounted modules declare.
    pub(crate) fn len(&self) -> usize {
        self.table.len()
    }

    pub fn required<'a>(&'a self, module: &'a str) -> impl Iterator<Item = FnId> + 'a {
        (0..self.len() as u16)
            .map(FnId)
            .filter(move |&id| self.function(id).required && self.module_of(id) == module)
    }
}

impl Links {
    pub(crate) fn deps_of(&self, id: FnId) -> u64 {
        self.deps[usize::from(id.0)]
    }

    pub(crate) fn to(&self, id: FnId) -> FnId {
        self.to[usize::from(id.0)]
    }

    /// The definition a required function is bound to, if it is.
    pub(crate) fn def_of(&self, id: FnId) -> Option<u16> {
        self.defs[usize::from(id.0)]
    }
}

/// the engine, edges, and the budget.
pub struct Cx<'r, 't, M: Module, S: Store = Compact> {
    pub(super) run: &'r mut Run<'t, S>,
    pub(super) slot: u8,
    pub(super) module: PhantomData<fn() -> M>,
}

impl<'r, 't, M: Module, S: Store> Cx<'r, 't, M, S> {
    pub(super) fn new(run: &'r mut Run<'t, S>, slot: u8) -> Self {
        Self {
            run,
            slot,
            module: PhantomData,
        }
    }

    pub fn fi(&self) -> u32 {
        self.run.input.fi
    }

    pub fn tree(&self) -> &'t Tree<S> {
        &self.run.input.trees[self.run.input.fi as usize]
    }

    pub fn at(&self, node: u32) -> Cursor<'t, S> {
        Cursor::new(self.run.input.trees, self.run.input.fi, node)
    }

    pub fn jump(&self, loc: Loc) -> Cursor<'t, S> {
        Cursor::new(self.run.input.trees, loc.fi, loc.node)
    }

    pub fn loc(&self, node: u32) -> Loc {
        Loc {
            fi: self.run.input.fi,
            node,
        }
    }

    pub fn lang(&self) -> &'t crate::intern::Lang {
        self.run.lang()
    }

    /// The kind ids bound to the module's `KINDS` roles, in their order.
    pub fn kinds(&self) -> &'t [u16] {
        self.run
            .phase
            .roles
            .get(usize::from(self.slot))
            .map_or(&[], |r| &r.kinds)
    }

    /// The tag ids bound to the module's `TAGS` roles, in their order.
    pub fn tags(&self) -> &'t [u32] {
        self.run
            .phase
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
            fi: self.run.input.graph_fi,
            node,
        };
        self.run
            .graph_reads
            .borrow_mut()
            .push((kind, at.fi, at.node));
        self.run
            .input
            .graph
            .is_some_and(|g| g.has(kind, incoming, at))
    }

    /// The targets of `from`'s edges of `kind` in the graph so far.
    pub fn targets(&self, kind: EdgeKind, from: Loc) -> &'t [Loc] {
        self.run
            .graph_reads
            .borrow_mut()
            .push((kind, from.fi, from.node));
        self.run.input.graph.map_or(&[], |g| g.targets(kind, from))
    }

    /// The targets of `from`'s edges of `kind` that carry no label.
    pub fn unlabelled_targets(&self, kind: EdgeKind, from: Loc) -> &'t [Loc] {
        self.run
            .graph_reads
            .borrow_mut()
            .push((kind, from.fi, from.node));
        self.run
            .input
            .graph
            .map_or(&[], |g| g.unlabelled(kind, from))
    }

    /// The edges of `kind` made at `site`, as (from, to).
    pub fn at_site(&self, kind: EdgeKind, site: Loc) -> &'t [(Loc, Loc)] {
        self.run
            .graph_reads
            .borrow_mut()
            .push((kind, site.fi, site.node));
        self.run.input.graph.map_or(&[], |g| g.at_site(kind, site))
    }

    /// How many files the run covers; file indexes are `0..files()`.
    pub fn files(&self) -> u32 {
        self.run.input.files
    }

    /// The project tree's root, in a family run.
    pub fn project(&self) -> Option<Cursor<'t, S>> {
        self.run
            .input
            .project
            .map(|fi| Cursor::new(self.run.input.trees, fi, 0))
    }

    pub fn state(&mut self) -> &mut M::State {
        debug_assert!(
            self.run.reading & (1u64 << self.slot) != 0,
            "{} state read by a function whose uses do not reach it",
            M::NAME
        );
        self.slot_state()
    }

    pub fn write(&mut self) -> &mut M::State {
        self.run.epochs[usize::from(self.slot)] += 1;
        self.run.written += 1;
        self.slot_state()
    }

    pub(super) fn slot_state(&mut self) -> &mut M::State {
        self.run.states[usize::from(self.slot)]
            .downcast_mut()
            .expect("each slot holds its module's state")
    }

    pub fn call(&mut self, id: FnId, args: &[Vals]) -> Vals {
        let registry = self.run.registry();
        assert_eq!(
            registry.function(id).slot,
            self.slot,
            "{} called {}.{}: a module calls only its own functions",
            M::NAME,
            registry.module_of(id),
            registry.function(id).name
        );
        self.run.call(id, args)
    }

    /// Calls the module's own function `name`.
    pub fn call_own(&mut self, name: &str, args: &[Vals]) -> Vals {
        let index = M::functions()
            .iter()
            .position(|f| f.name == name)
            .unwrap_or_else(|| panic!("{} declares no function {name}", M::NAME));
        let first = self.run.registry().starts[usize::from(self.slot)];
        self.call(FnId(first + index as u16), args)
    }

    pub fn call1(&mut self, id: FnId, arg: Val) -> Vals {
        self.call(id, &[one(arg)])
    }

    pub fn emit(&mut self, edge: Edge) {
        self.run.edges.push(edge);
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
        Some(self.run.input.indexed?.piece(fi, id))
    }

    /// A declared rule index's whole-tree matches across the family, by key.
    pub fn family(&self, id: u32) -> Option<&'t FxHashMap<u32, Vec<Loc>>> {
        Some(self.run.input.indexed?.family(id))
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
