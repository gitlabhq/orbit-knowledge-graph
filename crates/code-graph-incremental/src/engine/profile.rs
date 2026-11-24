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
    /// Calls and misses per (caller, callee) pair of cached calls, by name.
    edges: FxHashMap<(String, String), (u64, u64)>,
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

/// One run's counters, keyed by the memo's function id.
#[derive(Default)]
pub struct Profile {
    pub stats: FxHashMap<FnId, Stat>,
    children: Vec<u64>,
    active: FxHashMap<FnId, u32>,
    stack: Vec<FnId>,
    edges: FxHashMap<(Option<FnId>, FnId), (u64, u64)>,
    rules: FxHashMap<usize, (Box<str>, Stat)>,
    /// Per key: times computed, times a kept result was reused.
    keys: FxHashMap<Key, (u32, u32)>,
    /// Per (index, key symbol): lookups and empty lookups.
    lookups: FxHashMap<(u32, u32), (u64, u64)>,
}

impl Profile {
    pub(super) fn hit(&mut self, key: &Key, busy: bool) {
        self.edges
            .entry((self.stack.last().copied(), key.0))
            .or_default()
            .0 += 1;
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

    pub fn enter(&mut self, id: FnId) -> Instant {
        let edge = self
            .edges
            .entry((self.stack.last().copied(), id))
            .or_default();
        edge.0 += 1;
        edge.1 += 1;
        self.stack.push(id);
        *self.active.entry(id).or_default() += 1;
        self.children.push(0);
        Instant::now()
    }

    pub fn leave(&mut self, id: FnId, start: Instant, reruns: u32) {
        let total = start.elapsed().as_nanos() as u64;
        self.stack.pop();
        let inner = self.children.pop().unwrap_or(0);
        if let Some(parent) = self.children.last_mut() {
            *parent += total;
        }
        let outermost = self.active.get_mut(&id).is_some_and(|n| {
            *n -= 1;
            *n == 0
        });
        let s = self.stats.entry(id).or_default();
        s.calls += 1;
        s.reruns += u64::from(reruns);
        if outermost {
            s.total_ns += total;
        }
        s.self_ns += total.saturating_sub(inner);
    }

    /// One attempt of a rule: self time is matching, total adds its emits.
    pub fn rule(&mut self, id: usize, src: &str, fired: bool, matching: Duration, total: Duration) {
        let (_, s) = self
            .rules
            .entry(id)
            .or_insert_with(|| (src.into(), Stat::default()));
        s.calls += 1;
        s.hits += u64::from(fired);
        s.self_ns += matching.as_nanos() as u64;
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
        for (key, &(_, reused)) in &self.keys {
            if reused == 0 {
                self.stats.entry(key.0).or_default().never_hit += 1;
            }
        }
        let name = |id: FnId| run.fn_name(id);
        let mut t = total();
        for (id, stat) in self.stats.drain() {
            t.fns.entry(name(id)).or_default().add(&stat);
        }
        for (_, (src, stat)) in self.rules.drain() {
            t.rules.entry(src.into()).or_default().add(&stat);
        }
        for ((from, to), (calls, misses)) in self.edges.drain() {
            let from = from.map_or_else(|| "(rule)".to_string(), name);
            let e = t.edges.entry((from, name(to))).or_default();
            e.0 += calls;
            e.1 += misses;
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

    header(
        &mut out,
        &format!("cached call edges (top {TOP} by misses)"),
    );
    let _ = writeln!(
        out,
        "{:<70} {:>10} {:>10}",
        "caller -> callee", "calls", "misses"
    );
    for ((from, to), (calls, misses)) in sorted(&t.edges, |e| e.1).into_iter().take(TOP) {
        let _ = writeln!(
            out,
            "{:<70} {calls:>10} {misses:>10}",
            cut(&format!("{from} -> {to}"), 70)
        );
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
        .map(|((from, to), (calls, misses))| {
            (
                format!("{from} -> {to}"),
                json!({"calls": calls, "misses": misses}),
            )
        })
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
    Ok(Some(path))
}
