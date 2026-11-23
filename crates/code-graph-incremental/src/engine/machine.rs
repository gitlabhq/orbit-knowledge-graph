//! Evaluates expressions and definition calls on explicit stacks: tasks still
//! to do, the values they produce, and the slot vectors they read. A
//! definition call is a frame here, not a Rust call, so recursion in the rules
//! never grows the Rust stack; the fixpoint of a cycle is a frame that runs its
//! body again.

use rustc_hash::FxHashSet;
use smallvec::SmallVec;

use super::expr::{Expr, Op, apply};
use std::time::{Duration, Instant};

use super::program::{Case, Cases, DefImpl, Scratch};
use super::{
    Cursor, FnId, Key, Loc, MAX_FIXPOINT_ROUNDS, Memo, Run, Store, Val, Vals, WALK, truthy,
};

type Args = SmallVec<[Vals; 3]>;

/// A definition call whose body is running: what the fixpoint and the cache
/// need once its value arrives.
struct Pending {
    key: Key,
    def: u16,
    args: Args,
    deps: u8,
    epochs: [u64; WALK + 1],
    outer: FxHashSet<Key>,
    reading: u8,
    rounds: u32,
    started: Option<std::time::Instant>,
}

/// Where a set of cases stands: the candidates for its argument, the next to
/// try, whether a guard or a value is being evaluated, the groups that have
/// fired and the union of their values.
struct Trying<'e, S: Store> {
    list: &'e [u16],
    cases: &'e Cases<Case>,
    at: usize,
    args: Args,
    w: Scratch<S>,
    env: usize,
    step: Step,
    fired: SmallVec<[u16; 4]>,
    out: Vals,
    started: Option<Instant>,
    matched: Option<Duration>,
}

#[derive(Clone, Copy)]
enum Step {
    Next,
    Guard,
    Value,
}

/// One step to take; small, so the task stack moves little. Work that keeps
/// state across steps waits on the frame stack and resumes from there.
enum Task<'e> {
    Eval(&'e Expr, usize),
    Union(usize),
    Op(Op, usize),
    Call(FnId, usize),
    Or(&'e Expr, usize),
    LoopStart {
        every: bool,
        slot: usize,
        body: &'e Expr,
        env: usize,
    },
    MapStart(u16, usize),
    PopEnv,
    /// Resumes the frame on top of the frame stack.
    Resume,
}

/// Work in progress across steps, in the order its `Resume` tasks were queued.
enum Frame<'e, S: Store> {
    Loop {
        every: bool,
        slot: usize,
        body: &'e Expr,
        env: usize,
        list: Vals,
        at: usize,
        saved: Vals,
        acc: Option<Vals>,
    },
    Map {
        def: u16,
        args: Args,
        heads: Vals,
        at: usize,
        out: Vals,
    },
    Finish(Pending),
    Cases(Trying<'e, S>),
}

/// The stacks of one evaluation; a run keeps one set and reuses it, and a
/// call back into the rules from module code takes a fresh set.
pub(super) struct Machine<'e, S: Store> {
    tasks: Vec<Task<'e>>,
    frames: Vec<Frame<'e, S>>,
    values: Vec<Vals>,
    envs: Vec<Vec<Vals>>,
    /// Slot vectors and case scratch spaces kept for reuse.
    spare: Vec<Vec<Vals>>,
    scratch: Vec<Scratch<S>>,
}

impl<S: Store> Default for Machine<'_, S> {
    fn default() -> Self {
        Self {
            tasks: Vec::new(),
            frames: Vec::new(),
            values: Vec::new(),
            envs: Vec::new(),
            spare: Vec::new(),
            scratch: Vec::new(),
        }
    }
}

/// The value of `expr` with `caps` as its slots.
pub(super) fn eval<'t, S: Store>(run: &mut Run<'t, S>, expr: &'t Expr, caps: &[Vals]) -> Vals {
    let mut m = run.machine.take().unwrap_or_default();
    let env = m.env(caps.iter().cloned());
    m.tasks.push(Task::Eval(expr, env));
    let found = m.run(run);
    m.release_envs();
    run.machine = Some(m);
    found
}

/// The value of a call to definition `def`, cached and run to its fixpoint.
pub(super) fn call_def<'t, S: Store>(run: &mut Run<'t, S>, def: u16, args: &[Vals]) -> Vals {
    let fns = run.fns;
    let mut m = run.machine.take().unwrap_or_default();
    let found = match m.start(run, fns, def, args.iter().cloned().collect()) {
        Some(vals) => vals,
        None => m.run(run),
    };
    m.release_envs();
    run.machine = Some(m);
    found
}

/// Fires `cases` at `node`: each group's first case that holds.
pub(super) fn fire<'t, S: Store>(
    run: &mut Run<'t, S>,
    cases: &'t Cases<Case>,
    slots: usize,
    node: Loc,
) {
    let mut m = run.machine.take().unwrap_or_default();
    m.try_cases(
        run,
        cases,
        slots,
        SmallVec::from_elem(SmallVec::from_elem(Val::Node(node), 1), 1),
    );
    m.run(run);
    m.release_envs();
    run.machine = Some(m);
}

impl<'e, S: Store> Machine<'e, S> {
    fn run<'t>(&mut self, run: &mut Run<'t, S>) -> Vals
    where
        't: 'e,
    {
        let fns: &'e [super::program::Def] = run.fns;
        while let Some(task) = self.tasks.pop() {
            self.step(run, fns, task);
        }
        self.values.pop().unwrap_or_default()
    }

    fn step<'t>(&mut self, run: &mut Run<'t, S>, fns: &'e [super::program::Def], task: Task<'e>) {
        match task {
            Task::Eval(expr, env) => self.eval(run, fns, expr, env),
            Task::Union(n) => {
                let at = self.values.len() - n;
                let mut out = Vals::new();
                for part in self.values.drain(at..) {
                    out.extend(part);
                }
                self.values.push(out);
            }
            Task::Op(op, n) => {
                let args = self.take(n);
                let found = run.operate(op, &args);
                self.values.push(found);
            }
            Task::Call(id, n) => {
                let args = self.take(n);
                let found = apply(run, id, &args);
                self.values.push(found);
            }
            Task::Or(b, env) => {
                if self.values.last().is_some_and(|v| v.is_empty()) {
                    self.values.pop();
                    self.tasks.push(Task::Eval(b, env));
                }
            }
            Task::LoopStart {
                every,
                slot,
                body,
                env,
            } => {
                let list = self.values.pop().unwrap_or_default();
                let saved = std::mem::take(&mut self.envs[env][slot]);
                self.frame(Frame::Loop {
                    every,
                    slot,
                    body,
                    env,
                    list,
                    at: 0,
                    saved,
                    acc: None,
                });
            }
            Task::Resume => self.resume(run, fns),
            Task::MapStart(def, n) => {
                let mut args = self.take(n);
                let heads = std::mem::take(&mut args[0]);
                self.frame(Frame::Map {
                    def,
                    args,
                    heads,
                    at: 0,
                    out: Vals::new(),
                });
            }
            Task::PopEnv => self.pop_env(),
        }
    }

    /// Continues the frame on top of the frame stack, in place while it has
    /// more to do.
    fn resume<'t>(&mut self, run: &mut Run<'t, S>, fns: &'e [super::program::Def]) {
        match self.frames.last() {
            Some(Frame::Loop { .. }) => self.resume_loop(),
            Some(Frame::Map { .. }) => self.resume_map(run, fns),
            Some(Frame::Finish(_)) => {
                if let Some(Frame::Finish(pending)) = self.frames.pop() {
                    self.finish(run, fns, pending);
                }
            }
            Some(Frame::Cases(_)) => {
                if let Some(Frame::Cases(trying)) = self.frames.pop() {
                    self.cases(run, trying);
                }
            }
            None => {}
        }
    }

    /// `first` takes the first body value that is not empty; `every` keeps
    /// the values every body yields.
    fn resume_loop(&mut self) {
        let Some(Frame::Loop {
            every,
            slot,
            body,
            env,
            list,
            at,
            saved,
            acc,
        }) = self.frames.last_mut()
        else {
            return;
        };
        let (every, slot, body, env) = (*every, *slot, *body, *env);
        if *at > 0 {
            let found = self.values.pop().unwrap_or_default();
            if !every && !found.is_empty() {
                let saved = std::mem::take(saved);
                self.frames.pop();
                self.envs[env][slot] = saved;
                self.values.push(found);
                return;
            }
            if every {
                *acc = Some(match acc.take() {
                    None => found,
                    Some(c) => c.into_iter().filter(|x| found.contains(x)).collect(),
                });
            }
        }
        match list.get(*at).copied() {
            Some(v) => {
                *at += 1;
                self.envs[env][slot] = SmallVec::from_elem(v, 1);
                self.tasks.push(Task::Resume);
                self.tasks.push(Task::Eval(body, env));
            }
            None => {
                let (saved, acc) = (std::mem::take(saved), acc.take());
                self.frames.pop();
                self.envs[env][slot] = saved;
                self.values.push(acc.unwrap_or_default());
            }
        }
    }

    /// Calls a definition on each head in turn and unions the values.
    fn resume_map<'t>(&mut self, run: &mut Run<'t, S>, fns: &'e [super::program::Def]) {
        let Some(Frame::Map {
            def,
            args,
            heads,
            at,
            out,
        }) = self.frames.last_mut()
        else {
            return;
        };
        if *at > 0 {
            for found in self.values.pop().unwrap_or_default() {
                if !out.contains(&found) {
                    out.push(found);
                }
            }
        }
        let Some(&head) = heads.get(*at) else {
            let out = std::mem::take(out);
            self.frames.pop();
            self.values.push(out);
            return;
        };
        *at += 1;
        args[0] = SmallVec::from_elem(head, 1);
        let (def, call) = (*def, args.clone());
        self.tasks.push(Task::Resume);
        if let Some(found) = self.start(run, fns, def, call) {
            self.values.push(found);
        }
    }

    /// A new environment holding `slots`, on a reused vector.
    fn env(&mut self, slots: impl IntoIterator<Item = Vals>) -> usize {
        let mut env = self.spare.pop().unwrap_or_default();
        env.clear();
        env.extend(slots);
        self.envs.push(env);
        self.envs.len() - 1
    }

    fn pop_env(&mut self) {
        if let Some(env) = self.envs.pop() {
            self.spare.push(env);
        }
    }

    fn release_envs(&mut self) {
        while !self.envs.is_empty() {
            self.pop_env();
        }
    }

    fn frame(&mut self, frame: Frame<'e, S>) {
        self.frames.push(frame);
        self.tasks.push(Task::Resume);
    }

    fn take(&mut self, n: usize) -> Args {
        let at = self.values.len() - n;
        self.values.drain(at..).collect()
    }

    fn eval<'t>(
        &mut self,
        run: &mut Run<'t, S>,
        fns: &'e [super::program::Def],
        expr: &'e Expr,
        env: usize,
    ) {
        if let Some(found) = self.leaf(run, expr, env) {
            self.values.push(found);
            return;
        }
        let (then, parts) = match expr {
            Expr::Union(parts) => (Task::Union(parts.len()), parts),
            Expr::Op(op, parts) => (Task::Op(*op, parts.len()), parts),
            Expr::Call(id, parts) => (Task::Call(*id, parts.len()), parts),
            Expr::Def(def, parts) => (Task::MapStart(*def, parts.len()), parts),
            Expr::Or(a, b) => {
                self.tasks.push(Task::Or(b, env));
                self.tasks.push(Task::Eval(a, env));
                return;
            }
            Expr::First(list, slot, body) | Expr::Every(list, slot, body) => {
                self.tasks.push(Task::LoopStart {
                    every: matches!(expr, Expr::Every(..)),
                    slot: usize::from(*slot),
                    body,
                    env,
                });
                self.tasks.push(Task::Eval(list, env));
                return;
            }
            _ => return,
        };
        if parts.iter().all(is_leaf) {
            for part in parts.iter() {
                let found = self.leaf(run, part, env).unwrap_or_default();
                self.values.push(found);
            }
            self.step(run, fns, then);
            return;
        }
        self.tasks.push(then);
        for part in parts.iter().rev() {
            self.tasks.push(Task::Eval(part, env));
        }
    }

    /// The value of an expression that needs no further steps.
    fn leaf<'t>(&self, run: &Run<'t, S>, expr: &Expr, env: usize) -> Option<Vals> {
        Some(match expr {
            Expr::Var(slot) => self.envs[env][usize::from(*slot)].clone(),
            Expr::Lit(val) => SmallVec::from_elem(*val, 1),
            Expr::Empty => Vals::new(),
            Expr::Data(at) => run.data[usize::from(*at)].clone(),
            _ => return None,
        })
    }

    /// Starts a call: its cached value, or its body's tasks and a frame that
    /// takes the body's value.
    fn start<'t>(
        &mut self,
        run: &mut Run<'t, S>,
        fns: &'e [super::program::Def],
        def: u16,
        args: Args,
    ) -> Option<Vals> {
        let def = run
            .overrides
            .and_then(|o| o.get(&def).copied())
            .unwrap_or(def);
        let d = &fns[usize::from(def)];
        let key: Key = (FnId(u16::MAX - def), d.key(run, &args));
        match run.memo.entries.get(&key) {
            Some(Memo::Busy(partial)) => {
                let partial = partial.clone();
                if let Some(p) = &mut run.profile {
                    p.hit(key.0, true);
                }
                run.reads.insert(key);
                return Some(partial);
            }
            Some(Memo::Done {
                vals,
                deps,
                stamp,
                within,
            }) if run.stamp(*deps) == *stamp
                && within.as_ref().is_none_or(|(round, _)| *round == run.round) =>
            {
                let vals = vals.clone();
                if let Some(p) = &mut run.profile {
                    p.hit(key.0, false);
                }
                if let Some((_, read)) = within {
                    let read = read.clone();
                    run.reads.extend(read);
                }
                return Some(vals);
            }
            _ => {}
        }
        run.sweep();
        run.memo
            .entries
            .insert(key.clone(), Memo::Busy(Vals::new()));
        let pending = Pending {
            started: run.profile.as_mut().map(|p| p.enter(key.0)),
            epochs: run.epochs,
            outer: std::mem::take(&mut run.reads),
            reading: std::mem::replace(&mut run.reading, d.deps),
            deps: d.deps,
            key,
            def,
            args,
            rounds: 0,
        };
        self.body(run, fns, pending);
        None
    }

    /// Queues a definition's body, then the frame that takes its value.
    fn body<'t>(&mut self, run: &mut Run<'t, S>, fns: &'e [super::program::Def], pending: Pending) {
        let def = pending.def;
        let d = &fns[usize::from(def)];
        if let DefImpl::Expr(expr) = &d.body {
            let blank = d.slots.max(d.params).saturating_sub(pending.args.len());
            let env = self.env(
                pending
                    .args
                    .iter()
                    .cloned()
                    .chain(std::iter::repeat_n(Vals::new(), blank)),
            );
            self.frame(Frame::Finish(pending));
            self.tasks.push(Task::PopEnv);
            self.tasks.push(Task::Eval(expr, env));
            return;
        }
        let args = pending.args.clone();
        self.frame(Frame::Finish(pending));
        match &d.body {
            DefImpl::Expr(_) => {}
            DefImpl::Cases(_) if args[0].len() > 1 => {
                let mut rest = args;
                let heads = std::mem::take(&mut rest[0]);
                self.frame(Frame::Map {
                    def,
                    args: rest,
                    heads,
                    at: 0,
                    out: Vals::new(),
                });
            }
            DefImpl::Cases(cases) => self.try_cases(run, cases, d.slots, args),
        }
    }

    /// Queues trying `cases` on `args`, whose first set holds one value.
    fn try_cases<'t>(
        &mut self,
        run: &mut Run<'t, S>,
        cases: &'e Cases<Case>,
        slots: usize,
        args: Args,
    ) {
        let node = args[0].first().and_then(|v| v.loc());
        let kind = node.map(|h| Cursor::new(run.trees, h.fi, h.node).kind());
        let list = kind
            .and_then(|k| cases.by_kind.get(&k))
            .unwrap_or(&cases.any);
        let mut w = self.scratch.pop().unwrap_or_else(|| Scratch::new(0));
        w.reset(slots.max(args.len()));
        self.frame(Frame::Cases(Trying {
            list,
            cases,
            at: 0,
            w,
            args,
            env: 0,
            step: Step::Next,
            fired: SmallVec::new(),
            out: Vals::new(),
            started: None,
            matched: None,
        }));
    }

    /// Takes a body's value: runs the body again while a cycle through this
    /// call is still growing, then caches the value and hands it on.
    fn finish<'t>(&mut self, run: &mut Run<'t, S>, fns: &'e [super::program::Def], mut p: Pending) {
        let found = self.values.pop().unwrap_or_default();
        let heads = run.reads.remove(&p.key);
        if heads && run.reads.is_empty() && p.rounds < MAX_FIXPOINT_ROUNDS {
            if let Some(Memo::Busy(partial)) = run.memo.entries.get(&p.key) {
                let same =
                    found.len() == partial.len() && found.iter().all(|v| partial.contains(v));
                if !same {
                    run.memo.entries.insert(p.key.clone(), Memo::Busy(found));
                    run.round += 1;
                    p.rounds += 1;
                    self.body(run, fns, p);
                    return;
                }
            }
        }
        if let (Some(prof), Some(start)) = (&mut run.profile, p.started) {
            prof.leave(p.key.0, start, p.rounds);
        }
        let read: Vec<Key> = run.reads.iter().cloned().collect();
        run.reads.extend(p.outer);
        if p.epochs != run.epochs {
            run.memo.entries.remove(&p.key);
        } else {
            let stamp = run.stamp(p.deps);
            let within = (!read.is_empty()).then(|| (run.round, read));
            run.memo.entries.insert(
                p.key,
                Memo::Done {
                    vals: found.clone(),
                    deps: p.deps,
                    stamp,
                    within,
                },
            );
        }
        run.reading = p.reading;
        self.values.push(found);
    }

    /// Tries cases in order: in each group, the first whose pattern matches
    /// and whose guard holds gives its value.
    fn cases<'t>(&mut self, run: &mut Run<'t, S>, mut t: Trying<'e, S>) {
        match t.step {
            Step::Guard => {
                if truthy(&self.values.pop().unwrap_or_default()) {
                    t.matched = t.started.map(|s| s.elapsed());
                    self.value(t);
                    return;
                }
                self.pop_env();
                attempted(run, &t, false);
                t.at += 1;
            }
            Step::Value => {
                let found = self.values.pop().unwrap_or_default();
                self.pop_env();
                attempted(run, &t, true);
                match t.out.is_empty() {
                    true => t.out = found,
                    false => found.into_iter().for_each(|v| {
                        if !t.out.contains(&v) {
                            t.out.push(v);
                        }
                    }),
                }
                let at = t.list[t.at];
                t.fired.push(t.cases.group(at));
                t.at = match t.cases.stops[usize::from(at)] {
                    true => t.list.len(),
                    false => t.at + 1,
                };
            }
            Step::Next => {}
        }
        while t.fired.len() < t.cases.width
            && let Some(&at) = t.list.get(t.at)
        {
            let (pat, guard, _) = &t.cases.items[usize::from(at)];
            if t.fired.contains(&t.cases.group(at)) {
                t.at += 1;
                continue;
            }
            t.started = run.profiling().then(Instant::now);
            t.matched = None;
            let matched = match (pat, t.args[0].first().and_then(|v| v.loc())) {
                (Some(m), Some(h)) => t.w.matches(run, m, Cursor::new(run.trees, h.fi, h.node)),
                (Some(_), None) => false,
                (None, _) => {
                    t.w.vals[0] = t.args[0].clone();
                    true
                }
            };
            if !matched {
                attempted(run, &t, false);
                t.at += 1;
                continue;
            }
            if t.args.len() > 1 {
                t.w.vals[1..t.args.len()].clone_from_slice(&t.args[1..]);
            }
            t.env = self.env(t.w.vals.iter().cloned());
            match guard {
                Some(g) => {
                    t.step = Step::Guard;
                    let env = t.env;
                    self.frame(Frame::Cases(t));
                    self.tasks.push(Task::Eval(g, env));
                }
                None => {
                    t.matched = t.started.map(|s| s.elapsed());
                    self.value(t);
                }
            }
            return;
        }
        self.scratch.push(t.w);
        self.values.push(t.out);
    }

    fn value(&mut self, mut t: Trying<'e, S>) {
        let (_, _, value) = &t.cases.items[usize::from(t.list[t.at])];
        let env = t.env;
        t.step = Step::Value;
        self.frame(Frame::Cases(t));
        self.tasks.push(Task::Eval(value, env));
    }
}

/// Profiles one attempt of the case at `t.at`.
fn attempted<S: Store>(run: &mut Run<'_, S>, t: &Trying<'_, S>, fired: bool) {
    let (Some(started), Some(m)) = (t.started, &t.cases.items[usize::from(t.list[t.at])].0) else {
        return;
    };
    let total = started.elapsed();
    run.profile_rule(
        m as *const _ as usize,
        m.src(),
        fired,
        t.matched.unwrap_or(total),
        total,
    );
}

fn is_leaf(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Var(_) | Expr::Lit(_) | Expr::Empty | Expr::Data(_)
    )
}
