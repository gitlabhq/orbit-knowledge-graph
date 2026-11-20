//! Counters per YAML definition, module function and phase, collected when
//! `CGI_PROFILE` is set and merged across files.

use std::fmt::Write;
use std::sync::{LazyLock, Mutex};
use std::time::Instant;

use rustc_hash::FxHashMap;

use super::FnId;

pub static ON: LazyLock<bool> = LazyLock::new(|| std::env::var_os("CGI_PROFILE").is_some());

static TOTAL: Mutex<Vec<(String, Stat)>> = Mutex::new(Vec::new());

#[derive(Default, Clone, Copy)]
pub struct Stat {
    pub calls: u64,
    pub hits: u64,
    pub cycles: u64,
    pub reruns: u64,
    pub total_ns: u64,
    pub self_ns: u64,
    pub entries: u64,
    pub values: u64,
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
    }
}

/// One run's counters, keyed by the memo's function id.
#[derive(Default)]
pub struct Profile {
    pub stats: FxHashMap<FnId, Stat>,
    children: Vec<u64>,
    active: FxHashMap<FnId, u32>,
}

impl Profile {
    pub fn hit(&mut self, id: FnId, busy: bool) {
        let s = self.stats.entry(id).or_default();
        s.calls += 1;
        match busy {
            true => s.cycles += 1,
            false => s.hits += 1,
        }
    }

    pub fn enter(&mut self, id: FnId) -> Instant {
        *self.active.entry(id).or_default() += 1;
        self.children.push(0);
        Instant::now()
    }

    pub fn leave(&mut self, id: FnId, start: Instant, reruns: u32) {
        let total = start.elapsed().as_nanos() as u64;
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

    pub fn merge(&mut self, name: impl Fn(FnId) -> String) {
        let mut total = TOTAL.lock().unwrap_or_else(|e| e.into_inner());
        for (id, stat) in self.stats.drain() {
            add(&mut total, name(id), &stat);
        }
    }
}

fn add(total: &mut Vec<(String, Stat)>, name: String, stat: &Stat) {
    match total.iter_mut().find(|(n, _)| *n == name) {
        Some((_, s)) => s.add(stat),
        None => total.push((name, *stat)),
    }
}

/// Wall time of one phase over one unit of work.
pub fn phase(name: &str, start: Instant) {
    let ns = start.elapsed().as_nanos() as u64;
    let stat = Stat {
        calls: 1,
        total_ns: ns,
        self_ns: ns,
        ..Stat::default()
    };
    add(
        &mut TOTAL.lock().unwrap_or_else(|e| e.into_inner()),
        format!("phase {name}"),
        &stat,
    );
}

/// The counters so far, slowest self time first.
pub fn report() -> String {
    let mut rows = TOTAL.lock().unwrap_or_else(|e| e.into_inner()).clone();
    rows.sort_by_key(|(_, s)| std::cmp::Reverse(s.self_ns));
    let mut out = format!(
        "{:<36} {:>10} {:>10} {:>10} {:>7} {:>7} {:>9} {:>9} {:>9}\n",
        "name", "self ms", "total ms", "calls", "hit %", "cycles", "reruns", "entries", "values"
    );
    for (name, s) in rows {
        let hit = match s.calls {
            0 => 0.0,
            n => 100.0 * s.hits as f64 / n as f64,
        };
        let _ = writeln!(
            out,
            "{:<36} {:>10.1} {:>10.1} {:>10} {:>7.1} {:>7} {:>9} {:>9} {:>9}",
            name,
            s.self_ns as f64 / 1e6,
            s.total_ns as f64 / 1e6,
            s.calls,
            hit,
            s.cycles,
            s.reruns,
            s.entries,
            s.values
        );
    }
    out
}
