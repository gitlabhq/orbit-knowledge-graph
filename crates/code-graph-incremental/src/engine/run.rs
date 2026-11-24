//! One phase run over one tree: navigation, the budget, edges written, module state, and the memo.

use std::any::Any;

use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use super::*;
use crate::sentinel::{Killed, Sentinel};
use crate::tree::{Compact, Edge};

/// The anchor under which a piece files every match of a tree.
pub const WHOLE: u32 = u32::MAX;

/// What a run needs to build index pieces on demand, and the run's
/// family-wide directory of each index, merged from the files' pieces.
pub struct Indexed<'t, S: Store> {
    pub(super) phase: &'t Phase,
    pub(super) trees: Trees<'t, S>,
    pub(super) engine: &'t Engine,
    pub(super) budget: &'t Sentinel,
    pub(super) family: Box<[std::sync::OnceLock<FxHashMap<u32, Vec<Loc>>>]>,
}

impl<'t, S: Store> Indexed<'t, S> {
    pub(super) fn new(
        phase: &'t Phase,
        trees: Trees<'t, S>,
        engine: &'t Engine,
        budget: &'t Sentinel,
    ) -> Self {
        Self {
            phase,
            trees,
            engine,
            budget,
            family: (0..phase.index_count * engine.lang.tree_sets().len())
                .map(|_| std::sync::OnceLock::new())
                .collect(),
        }
    }

    pub(super) fn piece(&self, fi: u32, id: u32) -> &'t crate::tree::Piece {
        let (trees, engine, phase, budget) = (self.trees, self.engine, self.phase, self.budget);
        trees
            .get(fi)
            .indexes
            .piece(phase.index_count, id as usize, || {
                let started = profile::start();
                let input = Input {
                    trees,
                    fi,
                    rules: engine.rules_of(trees, fi),
                    graph: None,
                    indexed: None,
                    budget,
                    file: budget,
                };
                let mut run = Run::new(input, phase, engine);
                let piece = walk::piece(phase, &mut run, Cursor::new(trees, fi, 0), id);
                profile::piece(phase.index_label(id), started, &piece);
                piece
            })
    }

    pub(super) fn family(&self, set: usize, id: u32) -> &FxHashMap<u32, Vec<Loc>> {
        self.family[set * self.phase.index_count + id as usize].get_or_init(|| {
            let started = profile::start();
            let mut map: FxHashMap<u32, Vec<Loc>> = FxHashMap::default();
            for fi in self.trees.range(set) {
                for (&(anchor, key), nodes) in self.piece(fi, id) {
                    if anchor == WHOLE {
                        map.entry(key)
                            .or_default()
                            .extend(nodes.iter().map(|&node| Loc { fi, node }));
                    }
                }
            }
            profile::family(self.phase.index_label(id), started, map.len());
            map
        })
    }
}

/// Module state slots; a dependency set is a `u64` mask over them.
pub(super) const SLOTS: usize = 64;

pub(super) type Key = (FnId, SmallVec<[Val; 4]>);

/// A bound on reruns of one cycle; sets only grow, so a cycle over finite
/// trees converges well before it.
pub(super) const MAX_FIXPOINT_ROUNDS: u32 = 64;

pub(super) enum Memo {
    Busy(Vals),
    /// A result; `within` holds, for a result that read unfinished calls, the
    /// fixpoint round it belongs to and those calls.
    Done {
        vals: Vals,
        deps: u64,
        stamp: u64,
        within: Option<(u64, Vec<Key>)>,
    },
}

/// Results by call. An entry that read state stays until a sweep finds that
/// state written; sweeps run when the table doubles, so stale entries cost
/// amortized constant time.
#[derive(Default)]
pub(super) struct Table {
    pub(super) entries: FxHashMap<Key, Memo>,
    pub(super) swept: usize,
}

/// What a run reads besides its phase and engine.
pub(super) struct Input<'t, S: Store> {
    pub(super) trees: Trees<'t, S>,
    /// The tree the run walks.
    pub(super) fi: u32,
    /// The rule set the run's tree runs.
    pub(super) rules: usize,
    pub(super) graph: Option<&'t Graph>,
    pub(super) indexed: Option<&'t Indexed<'t, S>>,
    pub(super) budget: &'t Sentinel,
    pub(super) file: &'t Sentinel,
}

/// One phase run over one tree: navigation, the budget, the edges written,
/// one state slot per module, and the memo.
pub struct Run<'t, S: Store = Compact> {
    pub(super) input: Input<'t, S>,
    pub(super) phase: &'t Phase,
    pub(super) engine: &'t Engine,
    pub(super) killed: Option<Killed>,
    /// The graph ends the run looked edges up by.
    pub(super) graph_reads: std::cell::RefCell<Vec<super::graph::End>>,
    pub(super) edges: Vec<Edge>,
    /// The tree writes of the node being fired, applied once it is done.
    pub(super) writes: Vec<Write>,
    pub(super) states: Box<[Box<dyn Any>]>,
    pub(super) epochs: Box<[u64]>,
    pub(super) written: u64,
    pub(super) reading: u64,
    /// The state slots the innermost unfinished call has read; its result's dependencies.
    pub(super) used: u64,
    pub(super) memo: Table,
    /// The unfinished calls that results of the current cycle read.
    pub(super) cycle_reads: FxHashSet<Key>,
    pub(super) reinvoking: FxHashSet<Key>,
    /// Bumped each time a cycle head reruns, retiring results of the last round.
    pub(super) round: u64,
    pub(super) profile: Option<Box<profile::Profile>>,
    /// The evaluator's stacks, reused by each evaluation of this run.
    pub(super) vm: Option<vm::Vm<'t, S>>,
    /// Each node's nearest ancestor that is a scope or the root, filled on use.
    pub(super) scopes: FxHashMap<Loc, Option<Loc>>,
}

impl<'t, S: Store> Run<'t, S> {
    pub(super) fn new(input: Input<'t, S>, phase: &'t Phase, engine: &'t Engine) -> Self {
        let registry = &engine.pipeline.registry;
        Self {
            input,
            phase,
            engine,
            killed: None,
            graph_reads: Default::default(),
            edges: Vec::new(),
            writes: Vec::new(),
            states: registry.modules.iter().map(|m| (m.state)()).collect(),
            epochs: vec![0; registry.modules.len()].into(),
            written: 0,
            reading: u64::MAX,
            used: 0,
            memo: Table::default(),
            cycle_reads: FxHashSet::default(),
            reinvoking: FxHashSet::default(),
            round: 0,
            profile: profile::ON.then(Box::default),
            vm: None,
            scopes: FxHashMap::default(),
        }
    }

    pub(super) fn registry(&self) -> &'t Registry {
        &self.engine.pipeline.registry
    }

    pub(super) fn lang(&self) -> &'t crate::intern::Lang {
        &self.engine.lang
    }

    pub(super) fn check(&self) -> Result<(), Killed> {
        self.input
            .budget
            .check()
            .and_then(|()| self.input.file.check())?;
        match self.engine.limits.max_file_edges {
            0 => Ok(()),
            cap if self.edges.len() <= cap => Ok(()),
            cap => Err(Killed::limit(
                "edges",
                &self.input.trees.get(self.input.fi).label,
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
        self.input.trees.get(loc.fi).sym_at(loc.node, self.lang())
    }

    pub(super) fn step(&mut self) -> bool {
        if self.killed.is_none()
            && let Err(killed) = self.check()
        {
            self.killed = Some(killed);
        }
        self.killed.is_none()
    }

    pub(super) fn stamp(&self, deps: u64) -> u64 {
        stamp(&self.epochs, deps)
    }

    pub(super) fn key(&self, id: FnId, f: &Entry, args: &[Vals]) -> Key {
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
                    let derived = loc
                        .map(|l| S::derive(&derive)(Cursor::new(self.input.trees, l.fi, l.node)));
                    key.push(Val::Sym(derived.unwrap_or(u32::MAX)));
                }
            }
        }
        (id, key)
    }

    pub(super) fn sweep(&mut self) {
        let table = &mut self.memo;
        if table.entries.len() < 1024 || table.entries.len() < 2 * table.swept {
            return;
        }
        let (epochs, profile) = (&self.epochs, &mut self.profile);
        table.entries.retain(|key, memo| {
            let keep = match memo {
                Memo::Busy(_) => true,
                Memo::Done {
                    deps,
                    stamp: at,
                    within,
                    ..
                } => within.is_none() && stamp(epochs, *deps) == *at,
            };
            if let (false, Some(p)) = (keep, profile.as_mut()) {
                p.swept(key.0);
            }
            keep
        });
        table.swept = table.entries.len();
    }

    /// A module function's result, cached; it stays valid until a state the
    /// function can read is written.
    pub(super) fn call(&mut self, id: FnId, args: &[Vals]) -> Vals {
        vm::call(self, id, args)
    }

    /// Starts the phase's modules on the run's tree.
    pub(super) fn start_modules(&mut self) {
        let (phase, registry) = (self.phase, self.registry());
        for &slot in &phase.modules {
            (S::hooks(registry.module(slot)).start)(self, slot, 0);
        }
    }

    /// Finishes the phase's modules; the edges the run wrote, without
    /// repeats, and its backends' outputs, unless it was stopped.
    pub(super) fn end(&mut self) -> Result<(Vec<Edge>, Outputs), Killed> {
        if let Some(killed) = self.killed.take() {
            return Err(killed);
        }
        let (phase, registry) = (self.phase, self.registry());
        for &slot in phase.modules.iter().rev() {
            (S::hooks(registry.module(slot)).finish)(self, slot);
        }
        if let Some(killed) = self.killed.take() {
            return Err(killed);
        }
        let mut seen = FxHashSet::default();
        self.edges.retain(|e| seen.insert(*e));
        let mut outputs = Outputs::default();
        for &slot in &phase.modules {
            let module = registry.module(slot);
            if let Some(collect) = module.collect {
                let output = (collect.take)(self.states[usize::from(slot)].as_mut());
                outputs.0.push((module.name, collect, output));
            }
        }
        Ok((std::mem::take(&mut self.edges), outputs))
    }

    pub(crate) fn profiling(&self) -> bool {
        self.profile.is_some()
    }

    pub(crate) fn profile_rule(
        &mut self,
        id: usize,
        src: &str,
        fired: bool,
        own_ns: u64,
        total: std::time::Duration,
    ) {
        if let Some(p) = &mut self.profile {
            p.rule(id, src, fired, own_ns, total);
        }
    }

    pub(super) fn scope_above(&mut self, at: Loc) -> Option<Loc> {
        let kinds = &self.phase.enclosing;
        let mut path = Vec::new();
        let mut c = Cursor::new(self.input.trees, at.fi, at.node);
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

    pub(super) fn fn_name(&self, id: FnId) -> String {
        let registry = self.registry();
        match registry.table.get(usize::from(id.0)) {
            Some(f) => format!("{}.{}", registry.module(f.slot).name, f.name),
            None => self
                .phase
                .defs
                .get(usize::from(u16::MAX - id.0))
                .map_or_else(
                    || format!("def #{}", u16::MAX - id.0),
                    |d| d.name.to_string(),
                ),
        }
    }
}

pub(super) fn stamp(epochs: &[u64], mut deps: u64) -> u64 {
    let mut sum = 0;
    while deps != 0 {
        sum += epochs[deps.trailing_zeros() as usize];
        deps &= deps - 1;
    }
    sum
}

/// A write to the tree a run reads, applied after the node that made it:
/// a tag on `at`, or on the first node of kind `on` under it; or a template
/// built at `at` from the first symbol of each of its sets.
pub enum Write {
    /// Parses node `at`'s text with the frontend `frontend` names and
    /// grafts the tree under it.
    Parse { at: u32, frontend: u32 },
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

impl<S: Store> Drop for Run<'_, S> {
    fn drop(&mut self) {
        if let Some(p) = self.profile.take() {
            p.merge(self);
        }
    }
}
