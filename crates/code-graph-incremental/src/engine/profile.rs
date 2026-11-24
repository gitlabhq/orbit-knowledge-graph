//! Counters per YAML definition, module function, rule, phase, declared
//! index, file and shared-phase round, collected when `CGI_PROFILE` is set
//! and merged across runs. With `CGI_PROFILE=json`, [`save`] also writes
//! them as JSON to `CGI_PROFILE_OUT` (default `cgi-profile.json`).

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::{LazyLock, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;
use serde_json::json;

use super::{FnId, Key, Memo, Run, Store, Val, Vals, WHOLE};
use crate::tree::{Edge, Piece, Storage, Tree};

pub static ON: LazyLock<bool> = LazyLock::new(|| std::env::var_os("CGI_PROFILE").is_some());

/// Rows per section of the text report.
const TOP: usize = 25;
/// Slowest files kept per phase in the JSON output.
const TOP_FILES: usize = 100;
/// Hottest keys kept per index in the JSON output.
const TOP_KEYS: usize = 50;
/// Stack depth past which deeper frames fold into the frame at this depth.
const MAX_DEPTH: usize = 48;

#[derive(Default, Clone, Copy, Serialize)]
pub struct Stat {
    pub calls: u64,
    pub hits: u64,
    pub cycles: u64,
    pub reruns: u64,
    pub total_ns: u64,
    pub self_ns: u64,
    /// Memo entries left at the end of runs, and their values.
    pub entries: u64,
    pub values: u64,
    /// Results computed and kept.
    pub stored: u64,
    /// Kept results a later read found stale.
    pub stale: u64,
    /// Kept results a sweep dropped.
    pub swept: u64,
    /// Results dropped because their call wrote the tree.
    pub discarded: u64,
    /// Keys kept that no read reused.
    pub never_hit: u64,
    /// Computations of a key already computed in the same run.
    pub recomputes: u64,
    /// Approximate bytes of the kept results: keys and values.
    pub bytes: u64,
}

impl Stat {
    fn add(&mut self, o: &Stat) {
        self.calls += o.calls;
        self.hits += o.hits;
        self.cycles += o.cycles;
        self.reruns += o.reruns;
        self.total_ns += o.total_ns;
        self.self_ns += o.self_ns;
        self.entries += o.entries;
        self.values += o.values;
        self.stored += o.stored;
        self.stale += o.stale;
        self.swept += o.swept;
        self.discarded += o.discarded;
        self.never_hit += o.never_hit;
        self.recomputes += o.recomputes;
        self.bytes += o.bytes;
    }

    fn waste(&self) -> u64 {
        self.stale + self.swept + self.discarded + self.never_hit + self.recomputes
    }
}

#[derive(Default, Clone, Copy, Serialize)]
struct IndexStat {
    pieces: u64,
    piece_ns: u64,
    piece_entries: u64,
    families: u64,
    family_ns: u64,
    family_keys: u64,
    lookups: u64,
    empty: u64,
}

/// One caller -> callee edge of the weighted call graph.
#[derive(Default, Clone, Copy, Serialize)]
struct EdgeStat {
    calls: u64,
    /// Calls the callee computed rather than answered from the memo.
    misses: u64,
    /// Time in the callee under this caller, outermost calls only.
    incl_ns: u64,
    self_ns: u64,
}

impl EdgeStat {
    fn add(&mut self, o: &EdgeStat) {
        self.calls += o.calls;
        self.misses += o.misses;
        self.incl_ns += o.incl_ns;
        self.self_ns += o.self_ns;
    }
}

#[derive(Default, Clone, Copy, Serialize)]
struct FileStat {
    ns: u64,
    nodes: u64,
    runs: u64,
}

#[derive(Default, Clone, Copy, Serialize)]
struct RoundStat {
    /// Shared-phase runs that reached this round.
    runs: u64,
    files: u64,
    added: u64,
    dropped: u64,
    ns: u64,
}

#[derive(Serialize)]
struct MemoTable {
    phase: String,
    file: String,
    entries: u64,
    bytes: u64,
}

#[derive(Default)]
struct Totals {
    fns: FxHashMap<String, Stat>,
    rules: FxHashMap<String, Stat>,
    phases: FxHashMap<String, Stat>,
    /// The weighted call graph by (caller, callee) name.
    edges: FxHashMap<(String, String), EdgeStat>,
    /// Self ns per stack, `phase;frame;frame`.
    folded: FxHashMap<String, u64>,
    indexes: FxHashMap<String, IndexStat>,
    /// Lookups and empty lookups per (index, key text).
    keys: FxHashMap<(String, String), (u64, u64)>,
    /// Per (phase, file).
    files: FxHashMap<(String, String), FileStat>,
    /// Per shared phase, by round.
    rounds: FxHashMap<String, Vec<RoundStat>>,
    /// The runs whose memo held the most bytes at their end.
    tables: Vec<MemoTable>,
}

static TOTAL: LazyLock<Mutex<Totals>> = LazyLock::new(Mutex::default);

fn total() -> MutexGuard<'static, Totals> {
    TOTAL.lock().unwrap_or_else(|e| e.into_inner())
}

/// A call-graph frame: a computed call, or one attempt of a rule or case.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Frame {
    Fn(FnId),
    Rule(usize),
}

const ROOT: u32 = u32::MAX;

/// An open frame: its stack-tree node, start, and the time of the frames
/// under it, all of them and the function frames only.
struct Open {
    frame: Frame,
    node: u32,
    start: Instant,
    inner: u64,
    inner_fns: u64,
}

/// One run's counters, keyed by the memo's function id.
#[derive(Default)]
pub struct Profile {
    pub stats: FxHashMap<FnId, Stat>,
    stack: Vec<Open>,
    active: FxHashMap<Frame, u32>,
    active_edges: FxHashMap<(Option<Frame>, Frame), u32>,
    edges: FxHashMap<(Option<Frame>, Frame), EdgeStat>,
    /// The stack tree: parent node, frame, self ns.
    nodes: Vec<(u32, Frame, u64)>,
    children: FxHashMap<(u32, Frame), u32>,
    rule_src: FxHashMap<usize, Box<str>>,
    rules: FxHashMap<usize, (Box<str>, Stat)>,
    /// Per key: times computed, times a kept result was reused, whether one was kept.
    keys: FxHashMap<Key, (u32, u32, bool)>,
    /// Per (index, key symbol): lookups and empty lookups.
    lookups: FxHashMap<(u32, u32), (u64, u64)>,
}

impl Profile {
    fn caller(&self) -> Option<Frame> {
        self.stack.last().map(|o| o.frame)
    }

    pub(super) fn hit(&mut self, key: &Key, busy: bool) {
        self.edges
            .entry((self.caller(), Frame::Fn(key.0)))
            .or_default()
            .calls += 1;
        let s = self.stats.entry(key.0).or_default();
        s.calls += 1;
        match busy {
            true => s.cycles += 1,
            false => {
                s.hits += 1;
                if let Some(k) = self.keys.get_mut(key) {
                    k.1 += 1;
                }
            }
        }
    }

    pub(super) fn stale(&mut self, id: FnId) {
        self.stats.entry(id).or_default().stale += 1;
    }

    pub(super) fn swept(&mut self, id: FnId) {
        self.stats.entry(id).or_default().swept += 1;
    }

    /// A computed result: kept as `memo`, or dropped when `None`.
    pub(super) fn done(&mut self, key: &Key, memo: Option<&Memo>) {
        let s = self.stats.entry(key.0).or_default();
        match memo {
            Some(memo) => {
                s.stored += 1;
                s.bytes += bytes(key, memo);
            }
            None => s.discarded += 1,
        }
        let k = self.keys.entry(key.clone()).or_default();
        k.2 |= memo.is_some();
        if k.0 > 0 {
            s.recomputes += 1;
        }
        k.0 += 1;
    }

    pub(super) fn lookup(&mut self, index: u32, key: u32, found: bool) {
        let l = self.lookups.entry((index, key)).or_default();
        l.0 += 1;
        l.1 += u64::from(!found);
    }

    fn open(&mut self, frame: Frame) -> Instant {
        let caller = self.caller();
        let edge = self.edges.entry((caller, frame)).or_default();
        edge.calls += 1;
        edge.misses += 1;
        *self.active_edges.entry((caller, frame)).or_default() += 1;
        *self.active.entry(frame).or_default() += 1;
        let parent = self.stack.last().map_or(ROOT, |o| o.node);
        let node = match self.stack.len() >= MAX_DEPTH {
            true => parent,
            false => *self.children.entry((parent, frame)).or_insert_with(|| {
                self.nodes.push((parent, frame, 0));
                self.nodes.len() as u32 - 1
            }),
        };
        let start = Instant::now();
        self.stack.push(Open {
            frame,
            node,
            start,
            inner: 0,
            inner_fns: 0,
        });
        start
    }

    /// Closes `frame` and any frame a stopped run left open above it; its
    /// total and self ns, and whether it was the outermost open call.
    fn close(&mut self, frame: Frame) -> Option<(u64, u64, u64, bool)> {
        let at = self.stack.iter().rposition(|o| o.frame == frame)?;
        while self.stack.len() > at + 1 {
            let open = self.stack.last().map(|o| o.frame)?;
            self.close(open);
        }
        let open = self.stack.pop()?;
        let total = open.start.elapsed().as_nanos() as u64;
        let own = total.saturating_sub(open.inner);
        let caller = self.caller();
        if let Some(parent) = self.stack.last_mut() {
            parent.inner += total;
            parent.inner_fns += match frame {
                Frame::Fn(_) => total,
                Frame::Rule(_) => open.inner_fns,
            };
        }
        let outermost = self.active.get_mut(&frame).is_some_and(|n| {
            *n -= 1;
            *n == 0
        });
        let edge_outermost = self
            .active_edges
            .get_mut(&(caller, frame))
            .is_some_and(|n| {
                *n -= 1;
                *n == 0
            });
        let edge = self.edges.entry((caller, frame)).or_default();
        edge.self_ns += own;
        if edge_outermost {
            edge.incl_ns += total;
        }
        if let Some(node) = self.nodes.get_mut(open.node as usize) {
            node.2 += own;
        }
        Some((total, total.saturating_sub(open.inner_fns), own, outermost))
    }

    pub fn enter(&mut self, id: FnId) -> Instant {
        self.open(Frame::Fn(id))
    }

    pub fn leave(&mut self, id: FnId, _start: Instant, reruns: u32) {
        let Some((total, own, _, outermost)) = self.close(Frame::Fn(id)) else {
            return;
        };
        let s = self.stats.entry(id).or_default();
        s.calls += 1;
        s.reruns += u64::from(reruns);
        if outermost {
            s.total_ns += total;
        }
        s.self_ns += own;
    }

    /// Opens one attempt of the rule or case `id`, written `src`.
    pub(super) fn enter_rule(&mut self, id: usize, src: &str) {
        self.rule_src.entry(id).or_insert_with(|| src.into());
        self.open(Frame::Rule(id));
    }

    /// Closes one attempt of a rule; its own ns, not counting any frame under it.
    pub(super) fn leave_rule(&mut self, id: usize) -> u64 {
        self.close(Frame::Rule(id)).map_or(0, |(_, _, own, _)| own)
    }

    /// One attempt of a rule: self time is its own frame's, total adds its calls and emits.
    pub fn rule(&mut self, id: usize, src: &str, fired: bool, own_ns: u64, total: Duration) {
        let (_, s) = self
            .rules
            .entry(id)
            .or_insert_with(|| (src.into(), Stat::default()));
        s.calls += 1;
        s.hits += u64::from(fired);
        s.self_ns += own_ns;
        s.total_ns += total.as_nanos() as u64;
    }

    /// Adds the run's counters and its final memo to the totals.
    pub(super) fn merge<S: Store>(mut self, run: &Run<'_, S>) {
        let mut held = 0;
        for (key, memo) in &run.memo.entries {
            let s = self.stats.entry(key.0).or_default();
            s.entries += 1;
            if let Memo::Done { vals, .. } = memo {
                s.values += vals.len() as u64;
            }
            held += bytes(key, memo);
        }
        for (key, &(_, reused, kept)) in &self.keys {
            if kept && reused == 0 {
                self.stats.entry(key.0).or_default().never_hit += 1;
            }
        }
        let rule_src = &self.rule_src;
        let name = |frame: Frame| match frame {
            Frame::Fn(id) => run.fn_name(id),
            Frame::Rule(id) => format!("rule {}", cut(rule_src.get(&id).map_or("", |s| s), 60)),
        };
        let root = format!("[{}]", run.phase.name);
        let mut paths: Vec<String> = Vec::with_capacity(self.nodes.len());
        let mut folded: Vec<(usize, u64)> = Vec::new();
        for (at, &(parent, frame, own)) in self.nodes.iter().enumerate() {
            let above = paths.get(parent as usize).unwrap_or(&root);
            paths.push(format!("{above};{}", name(frame).replace(';', ",")));
            if own > 0 {
                folded.push((at, own));
            }
        }
        let mut t = total();
        for (at, own) in folded {
            *t.folded.entry(std::mem::take(&mut paths[at])).or_default() += own;
        }
        for (id, stat) in self.stats.drain() {
            t.fns.entry(run.fn_name(id)).or_default().add(&stat);
        }
        for (_, (src, stat)) in self.rules.drain() {
            t.rules.entry(src.into()).or_default().add(&stat);
        }
        for ((from, to), edge) in self.edges.drain() {
            let from = from.map_or_else(|| root.clone(), name);
            t.edges.entry((from, name(to))).or_default().add(&edge);
        }
        for ((index, key), (lookups, empty)) in self.lookups.drain() {
            let label = run.phase.index_label(index).to_string();
            let ix = t.indexes.entry(label.clone()).or_default();
            ix.lookups += lookups;
            ix.empty += empty;
            let text = run.lang().syms.resolve(key).to_string();
            let k = t.keys.entry((label, text)).or_default();
            k.0 += lookups;
            k.1 += empty;
        }
        if held > 0 {
            t.tables.push(MemoTable {
                phase: run.phase.name.clone(),
                file: run.input.trees.get(run.input.fi).label.clone(),
                entries: run.memo.entries.len() as u64,
                bytes: held,
            });
            if t.tables.len() > 2 * TOP {
                t.tables.sort_by_key(|m| std::cmp::Reverse(m.bytes));
                t.tables.truncate(TOP);
            }
        }
    }
}

/// Approximate bytes of one memo entry: its slot, key and value storage.
fn bytes(key: &Key, memo: &Memo) -> u64 {
    let val = std::mem::size_of::<Val>();
    let spilled = |vals: &Vals| match vals.spilled() {
        true => vals.capacity() * val,
        false => 0,
    };
    let key_heap = |key: &Key| match key.1.spilled() {
        true => key.1.capacity() * val,
        false => 0,
    };
    let held = match memo {
        Memo::Busy(vals) => spilled(vals),
        Memo::Done { vals, within, .. } => {
            spilled(vals)
                + within.as_ref().map_or(0, |(_, read)| {
                    read.capacity() * std::mem::size_of::<Key>()
                        + read.iter().map(key_heap).sum::<usize>()
                })
        }
    };
    (std::mem::size_of::<(Key, Memo)>() + 1 + key_heap(key) + held) as u64
}

/// A start time when profiling.
pub fn start() -> Option<Instant> {
    ON.then(Instant::now)
}

fn since(started: Instant) -> u64 {
    started.elapsed().as_nanos() as u64
}

/// Wall time of one phase over one unit of work.
pub fn phase(name: &str, started: Option<Instant>) {
    let Some(started) = started else {
        return;
    };
    let ns = since(started);
    let mut t = total();
    let s = t.phases.entry(name.to_string()).or_default();
    s.calls += 1;
    s.total_ns += ns;
    s.self_ns += ns;
}

/// Wall time of one phase over one file, and the file's node count after it.
pub fn file<S: Storage>(phase: &str, tree: &Tree<S>, started: Option<Instant>) {
    let Some(started) = started else {
        return;
    };
    let ns = since(started);
    let nodes = u64::from(tree.len());
    let mut t = total();
    let f = t
        .files
        .entry((phase.to_string(), tree.label.clone()))
        .or_default();
    f.ns += ns;
    f.nodes = nodes;
    f.runs += 1;
}

/// One build of a tree's piece of index `label`.
pub(super) fn piece(label: &str, started: Option<Instant>, piece: &Piece) {
    let Some(started) = started else {
        return;
    };
    let ns = since(started);
    let entries = piece
        .iter()
        .filter(|((anchor, _), _)| *anchor == WHOLE)
        .map(|(_, nodes)| nodes.len() as u64)
        .sum::<u64>();
    let mut t = total();
    let ix = t.indexes.entry(label.to_string()).or_default();
    ix.pieces += 1;
    ix.piece_ns += ns;
    ix.piece_entries += entries;
}

/// One build of index `label`'s family directory from its trees' pieces.
pub(super) fn family(label: &str, started: Option<Instant>, keys: usize) {
    let Some(started) = started else {
        return;
    };
    let ns = since(started);
    let mut t = total();
    let ix = t.indexes.entry(label.to_string()).or_default();
    ix.families += 1;
    ix.family_ns += ns;
    ix.family_keys += keys as u64;
}

/// Round `at` of a shared phase: the files it walked and the edges it found,
/// against the last round's.
pub(super) fn round(
    phase: &str,
    at: usize,
    files: usize,
    now: &FxHashSet<Edge>,
    before: &FxHashSet<Edge>,
    started: Option<Instant>,
) {
    let Some(started) = started else {
        return;
    };
    let ns = since(started);
    let mut t = total();
    let rounds = t.rounds.entry(phase.to_string()).or_default();
    if rounds.len() <= at {
        rounds.resize(at + 1, RoundStat::default());
    }
    let r = &mut rounds[at];
    r.runs += 1;
    r.files += files as u64;
    r.added += now.difference(before).count() as u64;
    r.dropped += before.difference(now).count() as u64;
    r.ns += ns;
}

fn ms(ns: u64) -> f64 {
    ns as f64 / 1e6
}

fn pct(part: u64, of: u64) -> f64 {
    match of {
        0 => 0.0,
        n => 100.0 * part as f64 / n as f64,
    }
}

fn cut(s: &str, width: usize) -> String {
    let s = s.replace('\n', " ");
    match s.char_indices().nth(width - 1) {
        Some((at, _)) => format!("{}~", &s[..at]),
        None => s,
    }
}

fn sorted<K: Clone, V: Copy>(map: &FxHashMap<K, V>, by: impl Fn(&V) -> u64) -> Vec<(K, V)> {
    let mut rows: Vec<_> = map.iter().map(|(k, v)| (k.clone(), *v)).collect();
    rows.sort_by_key(|(_, v)| std::cmp::Reverse(by(v)));
    rows
}

fn header(out: &mut String, title: &str) {
    let _ = writeln!(out, "\n== {title} ==");
}

/// The counters so far as a text report: one section per kind, top rows only.
pub fn report() -> String {
    let t = total();
    let mut out = String::new();

    header(&mut out, "phases (ms summed over units)");
    let _ = writeln!(out, "{:<40} {:>12} {:>10}", "phase", "ms", "units");
    for (name, s) in sorted(&t.phases, |s| s.total_ns) {
        let _ = writeln!(out, "{:<40} {:>12.1} {:>10}", name, ms(s.total_ns), s.calls);
    }

    header(
        &mut out,
        &format!("definitions and functions (top {TOP} by self ms)"),
    );
    let _ = writeln!(
        out,
        "{:<40} {:>10} {:>10} {:>10} {:>6} {:>8} {:>8} {:>9}",
        "name", "self ms", "total ms", "calls", "hit %", "cycles", "reruns", "entries"
    );
    for (name, s) in sorted(&t.fns, |s| s.self_ns).into_iter().take(TOP) {
        let _ = writeln!(
            out,
            "{:<40} {:>10.1} {:>10.1} {:>10} {:>6.1} {:>8} {:>8} {:>9}",
            cut(&name, 40),
            ms(s.self_ns),
            ms(s.total_ns),
            s.calls,
            pct(s.hits, s.calls),
            s.cycles,
            s.reruns,
            s.entries
        );
    }

    header(&mut out, &format!("rules (top {TOP} by match ms)"));
    let _ = writeln!(
        out,
        "{:<60} {:>10} {:>10} {:>10} {:>9}",
        "rule", "match ms", "total ms", "tries", "fired"
    );
    for (src, s) in sorted(&t.rules, |s| s.self_ns).into_iter().take(TOP) {
        let _ = writeln!(
            out,
            "{:<60} {:>10.1} {:>10.1} {:>10} {:>9}",
            cut(&src, 60),
            ms(s.self_ns),
            ms(s.total_ns),
            s.calls,
            s.hits
        );
    }

    header(
        &mut out,
        &format!("memo (top {TOP} by waste = stale + swept + discarded + never-hit + recomputed)"),
    );
    let _ = writeln!(
        out,
        "{:<36} {:>9} {:>9} {:>8} {:>8} {:>8} {:>9} {:>9} {:>9} {:>7}",
        "name",
        "stored",
        "reused",
        "stale",
        "swept",
        "discard",
        "never-hit",
        "recompute",
        "KiB",
        "B/entry"
    );
    let memo_row = |out: &mut String, name: &str, s: &Stat| {
        let _ = writeln!(
            out,
            "{:<36} {:>9} {:>9} {:>8} {:>8} {:>8} {:>9} {:>9} {:>9.0} {:>7}",
            cut(name, 36),
            s.stored,
            s.hits,
            s.stale,
            s.swept,
            s.discarded,
            s.never_hit,
            s.recomputes,
            s.bytes as f64 / 1024.0,
            s.bytes.checked_div(s.stored).unwrap_or(0)
        );
    };
    for (name, s) in sorted(&t.fns, Stat::waste).into_iter().take(TOP) {
        memo_row(&mut out, &name, &s);
    }
    let mut all = Stat::default();
    t.fns.values().for_each(|s| all.add(s));
    memo_row(&mut out, "(all)", &all);
    let _ = writeln!(out, "\nlargest memos at the end of a run:");
    let mut tables: Vec<_> = t.tables.iter().collect();
    tables.sort_by_key(|m| std::cmp::Reverse(m.bytes));
    for m in tables.into_iter().take(10) {
        let _ = writeln!(
            out,
            "  {:>9.0} KiB {:>8} entries  {:<20} {}",
            m.bytes as f64 / 1024.0,
            m.entries,
            m.phase,
            m.file
        );
    }

    let edge_row = |out: &mut String, from: &str, to: &str, e: &EdgeStat| {
        let _ = writeln!(
            out,
            "{:<80} {:>10} {:>10} {:>10.1} {:>10.1}",
            cut(&format!("{from} -> {to}"), 80),
            e.calls,
            e.misses,
            ms(e.incl_ns),
            ms(e.self_ns)
        );
    };
    let edge_header = |out: &mut String| {
        let _ = writeln!(
            out,
            "{:<80} {:>10} {:>10} {:>10} {:>10}",
            "caller -> callee", "calls", "misses", "incl ms", "self ms"
        );
    };
    for (title, by) in [
        (
            "inclusive ms",
            (|e: &EdgeStat| e.incl_ns) as fn(&EdgeStat) -> u64,
        ),
        ("self ms", |e| e.self_ns),
        ("misses", |e| e.misses),
    ] {
        header(
            &mut out,
            &format!("call graph edges (top {TOP} by {title})"),
        );
        edge_header(&mut out);
        for ((from, to), e) in sorted(&t.edges, by).into_iter().take(TOP) {
            edge_row(&mut out, &from, &to, &e);
        }
    }
    header(
        &mut out,
        "callers of the 10 definitions with the most self ms (top 5 callers by self ms)",
    );
    for (callee, s) in sorted(&t.fns, |s| s.self_ns).into_iter().take(10) {
        let _ = writeln!(out, "{} ({:.1} self ms)", cut(&callee, 80), ms(s.self_ns));
        let mut callers: Vec<_> = t
            .edges
            .iter()
            .filter(|((_, to), _)| *to == callee)
            .collect();
        callers.sort_by_key(|(_, e)| std::cmp::Reverse(e.self_ns));
        for ((from, _), e) in callers.into_iter().take(5) {
            let _ = writeln!(
                out,
                "  {:>10.1} self ms {:>10.1} incl ms {:>10} calls {:>10} misses  {}",
                ms(e.self_ns),
                ms(e.incl_ns),
                e.calls,
                e.misses,
                cut(from, 70)
            );
        }
    }

    header(
        &mut out,
        "indexes (by build ms: pieces + family directories)",
    );
    let _ = writeln!(
        out,
        "{:<56} {:>7} {:>9} {:>9} {:>7} {:>9} {:>10} {:>10} {:>6}",
        "index", "pieces", "build ms", "entries", "family", "fam ms", "lookups", "empty", "empty%"
    );
    let indexes = sorted(&t.indexes, |i| i.piece_ns + i.family_ns);
    for (label, i) in &indexes {
        let _ = writeln!(
            out,
            "{:<56} {:>7} {:>9.1} {:>9} {:>7} {:>9.1} {:>10} {:>10} {:>6.1}",
            cut(label, 56),
            i.pieces,
            ms(i.piece_ns),
            i.piece_entries,
            i.families,
            ms(i.family_ns),
            i.lookups,
            i.empty,
            pct(i.empty, i.lookups)
        );
    }
    let mut keys = keys_by_index(&t);
    let _ = writeln!(
        out,
        "\nhottest keys (top 5 of the 10 most looked-up indexes):"
    );
    let mut busiest: Vec<_> = indexes.iter().filter(|(_, i)| i.lookups > 0).collect();
    busiest.sort_by_key(|(_, i)| std::cmp::Reverse(i.lookups));
    for (label, _) in busiest.into_iter().take(10) {
        let _ = writeln!(out, "  {}", cut(label, 100));
        for (key, lookups, empty) in keys
            .remove(label.as_str())
            .unwrap_or_default()
            .into_iter()
            .take(5)
        {
            let _ = writeln!(
                out,
                "    {lookups:>10} lookups {empty:>10} empty  {}",
                cut(key, 60)
            );
        }
    }

    header(&mut out, "slowest files per phase (top 10 by ms)");
    for (phase, files) in files_by_phase(&t) {
        let _ = writeln!(out, "{phase}:");
        for (file, f) in files.into_iter().take(10) {
            let _ = writeln!(
                out,
                "  {:>10.1} ms {:>8} nodes {:>4} runs  {}",
                ms(f.ns),
                f.nodes,
                f.runs,
                file
            );
        }
    }

    header(&mut out, "shared phase rounds");
    for (phase, rounds) in t.rounds.iter().collect::<BTreeMap<_, _>>() {
        let all: u64 = rounds.iter().map(|r| r.runs).sum();
        let runs = rounds.first().map_or(0, |r| r.runs);
        let _ = writeln!(out, "{phase}: {runs} runs, {all} rounds");
        let _ = writeln!(
            out,
            "  {:>5} {:>7} {:>10} {:>10} {:>10} {:>10}",
            "round", "runs", "files", "added", "dropped", "ms"
        );
        for (at, r) in rounds.iter().enumerate() {
            let _ = writeln!(
                out,
                "  {:>5} {:>7} {:>10} {:>10} {:>10} {:>10.1}",
                at,
                r.runs,
                r.files,
                r.added,
                r.dropped,
                ms(r.ns)
            );
        }
    }
    out
}

/// Per phase, its files slowest first.
fn files_by_phase(t: &Totals) -> BTreeMap<&str, Vec<(&str, FileStat)>> {
    let mut by: BTreeMap<&str, Vec<(&str, FileStat)>> = BTreeMap::new();
    for ((phase, file), f) in &t.files {
        by.entry(phase).or_default().push((file, *f));
    }
    for files in by.values_mut() {
        files.sort_by(|a, b| b.1.ns.cmp(&a.1.ns).then(a.0.cmp(b.0)));
    }
    by
}

/// Per index, its keys' lookups and empty lookups, most looked up first.
fn keys_by_index(t: &Totals) -> BTreeMap<&str, Vec<(&str, u64, u64)>> {
    let mut by: BTreeMap<&str, Vec<(&str, u64, u64)>> = BTreeMap::new();
    for ((label, key), &(lookups, empty)) in &t.keys {
        by.entry(label).or_default().push((key, lookups, empty));
    }
    for keys in by.values_mut() {
        keys.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(b.0)));
    }
    by
}

fn ordered<V>(m: &FxHashMap<String, V>) -> BTreeMap<&String, &V> {
    m.iter().collect()
}

/// Writes the counters as JSON when `CGI_PROFILE=json`; the path written.
pub fn save() -> std::io::Result<Option<std::path::PathBuf>> {
    if std::env::var_os("CGI_PROFILE").is_none_or(|v| v != "json") {
        return Ok(None);
    }
    let path: std::path::PathBuf = std::env::var_os("CGI_PROFILE_OUT")
        .unwrap_or_else(|| "cgi-profile.json".into())
        .into();
    let t = total();
    let edges: BTreeMap<_, _> = t
        .edges
        .iter()
        .map(|((from, to), e)| (format!("{from} -> {to}"), e))
        .collect();
    let keys: BTreeMap<_, Vec<_>> = keys_by_index(&t)
        .into_iter()
        .map(|(label, keys)| {
            let rows = keys
                .into_iter()
                .take(TOP_KEYS)
                .map(
                    |(key, lookups, empty)| json!({"key": key, "lookups": lookups, "empty": empty}),
                )
                .collect();
            (label, rows)
        })
        .collect();
    let files: BTreeMap<_, Vec<_>> = files_by_phase(&t)
        .into_iter()
        .map(|(phase, files)| {
            let rows = files
                .into_iter()
                .take(TOP_FILES)
                .map(
                    |(file, f)| json!({"file": file, "ns": f.ns, "nodes": f.nodes, "runs": f.runs}),
                )
                .collect();
            (phase, rows)
        })
        .collect();
    let mut tables: Vec<_> = t.tables.iter().collect();
    tables.sort_by_key(|m| std::cmp::Reverse(m.bytes));
    tables.truncate(TOP);
    let value = json!({
        "phases": ordered(&t.phases),
        "functions": ordered(&t.fns),
        "rules": ordered(&t.rules),
        "edges": edges,
        "indexes": ordered(&t.indexes),
        "index_keys": keys,
        "files": files,
        "rounds": ordered(&t.rounds),
        "memo_tables": tables,
    });
    std::fs::write(&path, serde_json::to_vec_pretty(&value)?)?;
    let mut stacks: Vec<_> = t.folded.iter().collect();
    stacks.sort();
    let mut folded = String::new();
    for (stack, ns) in stacks {
        let _ = writeln!(folded, "{stack} {ns}");
    }
    std::fs::write(path.with_extension("folded"), folded)?;
    Ok(Some(path))
}
