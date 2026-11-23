//! Evaluates expressions and calls on explicit stacks: tasks still to do,
//! the values they produce, and the slot vectors they read. A cached call,
//! to a definition or a module function, is a frame here: its value is
//! looked up or computed, and a cycle through it reruns its body until its
//! value stops growing.

use std::time::{Duration, Instant};

use rustc_hash::FxHashSet;
use smallvec::SmallVec;

use super::expr::Expr;
use super::program::{Cases, Def, DefImpl, Scratch};
use super::{
    Arg, Cursor, FnId, Key, Kind, Loc, MAX_FIXPOINT_ROUNDS, Memo, Run, Store, Val, Vals, function,
    truthy,
};
use crate::constants::{RED_ZONE, STACK_SEGMENT};

type Args = SmallVec<[Vals; 3]>;

/// What a call runs: a definition, or a module function by its id.
#[derive(Clone, Copy)]
enum Callee {
    Def(u16),
    Native(FnId),
}

/// A cached call whose body is running: what the fixpoint and the cache
/// need once its value arrives.
struct Pending {
    key: Key,
    callee: Callee,
    args: Args,
    deps: u8,
    epochs: [u64; super::SLOTS],
    outer: FxHashSet<Key>,
    reading: u8,
    rounds: u32,
    started: Option<Instant>,
}

/// Where a set of cases stands: the candidates for its argument, the next to
/// try, whether a guard or a value is being evaluated, the groups that have
/// fired and the union of their values.
struct Trying<'e, S: Store> {
    list: &'e [u16],
    cases: &'e Cases,
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
    Call(Callee, usize),
    Or(&'e Expr, usize),
    LoopStart {
        every: bool,
        slot: usize,
        body: &'e Expr,
        env: usize,
    },
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
    /// A call made once per value of argument `at`, its values unioned.
    Each {
        callee: Callee,
        args: Args,
        at: usize,
        list: Vals,
        next: usize,
        out: Vals,
        seen: Option<FxHashSet<Val>>,
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

/// Runs `start` on the run's machine, or on a fresh one when module code
/// calls back in while the run's is busy; the value it leaves.
fn on_machine<'t, S: Store>(
    run: &mut Run<'t, S>,
    start: impl FnOnce(&mut Machine<'t, S>, &mut Run<'t, S>),
) -> Vals {
    let mut m = run.machine.take().unwrap_or_default();
    start(&mut m, run);
    let found = m.run(run);
    m.release_envs();
    run.machine = Some(m);
    found
}

/// The value of `expr` with `caps` as its slots.
pub(super) fn eval<'t, S: Store>(run: &mut Run<'t, S>, expr: &'t Expr, caps: &[Vals]) -> Vals {
    on_machine(run, |m, _| {
        let env = m.env(caps.iter().cloned());
        m.tasks.push(Task::Eval(expr, env));
    })
}

/// The value of a call to module function `id`, as module code makes it.
pub(super) fn call<S: Store>(run: &mut Run<'_, S>, id: FnId, args: &[Vals]) -> Vals {
    on_machine(run, |m, run| {
        let fns = run.fns;
        m.call(run, fns, Callee::Native(id), args.iter().cloned().collect())
    })
}

/// Fires `cases` at `node`: each group's first case that holds.
pub(super) fn fire<'t, S: Store>(run: &mut Run<'t, S>, cases: &'t Cases, slots: usize, node: Loc) {
    on_machine(run, |m, run| {
        let args = SmallVec::from_elem(SmallVec::from_elem(Val::Node(node), 1), 1);
        m.try_cases(run, cases, slots, args);
    });
}

impl<'e, S: Store> Machine<'e, S> {
    fn run<'t>(&mut self, run: &mut Run<'t, S>) -> Vals
    where
        't: 'e,
    {
        let fns: &'e [Def] = run.fns;
        while let Some(task) = self.tasks.pop() {
            self.step(run, fns, task);
        }
        self.values.pop().unwrap_or_default()
    }

    fn step<'t>(&mut self, run: &mut Run<'t, S>, fns: &'e [Def], task: Task<'e>) {
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
            Task::Call(callee, n) => {
                let args = self.take(n);
                self.spread(run, fns, callee, args);
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
            Task::PopEnv => self.pop_env(),
        }
    }

    /// Continues the frame on top of the frame stack, in place while it has
    /// more to do.
    fn resume<'t>(&mut self, run: &mut Run<'t, S>, fns: &'e [Def]) {
        match self.frames.last() {
            Some(Frame::Loop { .. }) => self.resume_loop(),
            Some(Frame::Each { .. }) => self.resume_each(run, fns),
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

    /// Makes the next of a spread call's calls, after adding the last one's
    /// values; a large union keeps a set to stay linear.
    fn resume_each<'t>(&mut self, run: &mut Run<'t, S>, fns: &'e [Def]) {
        let Some(Frame::Each {
            callee,
            args,
            at,
            list,
            next,
            out,
            seen,
        }) = self.frames.last_mut()
        else {
            return;
        };
        if *next > 0 {
            for found in self.values.pop().unwrap_or_default() {
                let new = match seen {
                    Some(seen) => seen.insert(found),
                    None => !out.contains(&found),
                };
                if new {
                    out.push(found);
                    if seen.is_none() && out.len() > 16 {
                        *seen = Some(out.iter().copied().collect());
                    }
                }
            }
        }
        let Some(&val) = list.get(*next) else {
            let out = std::mem::take(out);
            self.frames.pop();
            self.values.push(out);
            return;
        };
        *next += 1;
        args[*at] = SmallVec::from_elem(val, 1);
        let (callee, call) = (*callee, args.clone());
        self.tasks.push(Task::Resume);
        self.spread(run, fns, callee, call);
    }

    /// Calls `callee`, once per value of the first argument it takes one
    /// value of but was given another number of.
    fn spread<'t>(&mut self, run: &mut Run<'t, S>, fns: &'e [Def], callee: Callee, args: Args) {
        let at = match callee {
            Callee::Def(_) => (args.first().is_some_and(|a| a.len() != 1)).then_some(0),
            Callee::Native(id) => function(id)
                .sig
                .iter()
                .zip(&args)
                .position(|(arg, vals)| *arg == Arg::One && vals.len() != 1),
        };
        let Some(at) = at else {
            return self.call(run, fns, callee, args);
        };
        let mut args = args;
        let list = std::mem::take(&mut args[at]);
        self.frame(Frame::Each {
            callee,
            args,
            at,
            list,
            next: 0,
            out: Vals::new(),
            seen: None,
        });
    }

    /// Queues a call, or pushes its value: a bound function's provider, an
    /// uncached function's value, or a cached call's.
    fn call<'t>(&mut self, run: &mut Run<'t, S>, fns: &'e [Def], callee: Callee, args: Args) {
        let (key, deps, callee) = match callee {
            Callee::Def(def) => {
                let def = run
                    .overrides
                    .and_then(|o| o.get(&def).copied())
                    .unwrap_or(def);
                let d = &fns[usize::from(def)];
                (
                    (FnId(u16::MAX - def), d.key(&args)),
                    d.deps,
                    Callee::Def(def),
                )
            }
            Callee::Native(id) => {
                let links = run.links;
                if let Some(def) = links.defs[usize::from(id.0)] {
                    return self.call(run, fns, Callee::Def(def), args);
                }
                let deps = links.deps[usize::from(id.0)];
                let id = links.to[usize::from(id.0)];
                let f = function(id);
                if matches!(f.kind, Kind::Effect | Kind::Inline) {
                    let reading = match f.kind {
                        Kind::Effect => std::mem::replace(&mut run.reading, u8::MAX),
                        _ => run.reading,
                    };
                    let found = invoke(run, id, &args);
                    run.reading = reading;
                    self.values.push(found);
                    return;
                }
                (run.key(id, f, &args), deps, Callee::Native(id))
            }
        };
        match run.memo.entries.get(&key) {
            Some(Memo::Busy(partial)) => {
                let partial = partial.clone();
                if let Some(p) = &mut run.profile {
                    p.hit(key.0, true);
                }
                run.reads.insert(key);
                self.values.push(partial);
                return;
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
                self.values.push(vals);
                return;
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
            reading: std::mem::replace(&mut run.reading, deps),
            deps,
            key,
            callee,
            args,
            rounds: 0,
        };
        self.body(run, fns, pending);
    }

    /// Queues a call's body, then the frame that takes its value; a module
    /// function's body runs at once.
    fn body<'t>(&mut self, run: &mut Run<'t, S>, fns: &'e [Def], pending: Pending) {
        let def = match pending.callee {
            Callee::Native(id) => {
                let args = pending.args.clone();
                self.frame(Frame::Finish(pending));
                let found = stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || invoke(run, id, &args));
                self.values.push(found);
                return;
            }
            Callee::Def(def) => def,
        };
        let d = &fns[usize::from(def)];
        match &d.body {
            DefImpl::Expr(expr) => {
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
            }
            DefImpl::Cases(cases) => {
                let args = pending.args.clone();
                self.frame(Frame::Finish(pending));
                self.try_cases(run, cases, d.slots, args);
            }
        }
    }

    /// Queues trying `cases` on `args`, whose first set holds one value.
    fn try_cases<'t>(&mut self, run: &mut Run<'t, S>, cases: &'e Cases, slots: usize, args: Args) {
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
    fn finish<'t>(&mut self, run: &mut Run<'t, S>, fns: &'e [Def], mut p: Pending) {
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

    fn eval<'t>(&mut self, run: &mut Run<'t, S>, fns: &'e [Def], expr: &'e Expr, env: usize) {
        if let Some(found) = self.leaf(run, expr, env) {
            self.values.push(found);
            return;
        }
        let (then, parts) = match expr {
            Expr::Union(parts) => (Task::Union(parts.len()), parts),
            Expr::Call(id, parts) => (Task::Call(Callee::Native(*id), parts.len()), parts),
            Expr::Def(def, parts) => (Task::Call(Callee::Def(*def), parts.len()), parts),
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
}

/// Runs module function `id` natively.
fn invoke<S: Store>(run: &mut Run<'_, S>, id: FnId, args: &[Vals]) -> Vals {
    let f = function(id);
    let hooks = S::hooks(&crate::modules::ALL[usize::from(f.slot)]);
    (hooks.invoke)(run, f.slot, f.index, args)
}

/// Profiles one attempt of the case at `t.at`.
fn attempted<S: Store>(run: &mut Run<'_, S>, t: &Trying<'_, S>, fired: bool) {
    let (Some(started), Some(m)) = (t.started, &t.cases.items[usize::from(t.list[t.at])].0) else {
        return;
    };
    let total = started.elapsed();
    run.profile_rule(
        m as *const _ as usize,
        &m.src,
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
