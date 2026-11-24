//! Runs every incremental fixture suite in one process and compares the
//! failures with a saved list.
//!
//! `cargo run -q -p integration-tests-codegraph --profile gate --no-default-features --example gate -- [--save FILE] [--base FILE] [NAME...] 2>LOG`
//!
//! With `CGI_RULES_DIR` set to the crate dir of code-graph-incremental, rules
//! and fixtures are read from disk, so a YAML change needs no rebuild. A suite
//! still running after `CGI_SUITE_SECS` (10 by default) counts as a timeout.

use std::collections::{BTreeMap, BTreeSet};
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

enum Outcome {
    Pass,
    Fail,
    Timeout,
}

fn main() {
    let mut args = std::env::args().skip(1);
    let (mut save, mut base, mut names) = (None, None, Vec::new());
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--save" => save = args.next(),
            "--base" => base = args.next(),
            _ => names.push(arg),
        }
    }
    let limit = Duration::from_secs(
        std::env::var("CGI_SUITE_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10),
    );
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures_incremental");
    let mut suites = Vec::new();
    find(&root, &root, &mut suites);
    suites.sort();
    if !names.is_empty() {
        suites.retain(|(name, _)| names.iter().any(|n| n == name));
    }
    std::panic::set_hook(Box::new(|info| eprintln!("{info}")));
    let started = Instant::now();
    let workers = std::thread::available_parallelism().map_or(4, usize::from);
    let queue = Arc::new(Mutex::new(suites.clone()));
    let running: Arc<Mutex<BTreeMap<String, Instant>>> = Arc::default();
    let (done, results) = mpsc::channel::<(String, bool, f64)>();
    let worker = {
        let (queue, running, done) = (queue.clone(), running.clone(), done.clone());
        move || {
            let (queue, running, done) = (queue.clone(), running.clone(), done.clone());
            std::thread::spawn(move || work(&queue, &running, &done));
        }
    };
    for _ in 0..workers {
        worker();
    }
    drop(done);
    let mut outcomes: BTreeMap<String, (Outcome, f64)> = BTreeMap::new();
    while outcomes.len() < suites.len() {
        match results.recv_timeout(Duration::from_millis(100)) {
            Ok((name, ok, secs)) => {
                let outcome = if ok { Outcome::Pass } else { Outcome::Fail };
                outcomes.entry(name).or_insert((outcome, secs));
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => break,
            Err(mpsc::RecvTimeoutError::Timeout) => {}
        }
        let stuck: Vec<(String, f64)> = running
            .lock()
            .expect("running")
            .iter()
            .filter(|(name, at)| at.elapsed() > limit && !outcomes.contains_key(*name))
            .map(|(name, at)| (name.clone(), at.elapsed().as_secs_f64()))
            .collect();
        for (name, secs) in stuck {
            outcomes.insert(name, (Outcome::Timeout, secs));
            worker();
        }
    }
    let elapsed = started.elapsed();
    let pick = |want: fn(&Outcome) -> bool| -> BTreeSet<String> {
        outcomes
            .iter()
            .filter(|(_, (o, _))| want(o))
            .map(|(n, _)| n.clone())
            .collect()
    };
    let failed = pick(|o| !matches!(o, Outcome::Pass));
    let timeouts = pick(|o| matches!(o, Outcome::Timeout));
    println!(
        "{} suites: {} passing, {} failing ({} over {}s) in {:.1}s",
        suites.len(),
        suites.len() - failed.len(),
        failed.len(),
        timeouts.len(),
        limit.as_secs(),
        elapsed.as_secs_f64()
    );
    let mut slow: Vec<(&String, &(Outcome, f64))> =
        outcomes.iter().filter(|(_, (_, s))| *s >= 1.0).collect();
    slow.sort_by(|a, b| b.1.1.total_cmp(&a.1.1));
    for (name, (outcome, secs)) in slow.iter().take(25) {
        let mark = match outcome {
            Outcome::Pass => "pass",
            Outcome::Fail => "FAIL",
            Outcome::Timeout => "TIMEOUT",
        };
        println!("  {secs:8.2}s {mark:7} {name}");
    }
    if let Some(base) = base {
        let before: BTreeSet<String> = std::fs::read_to_string(&base)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .filter(|name| outcomes.contains_key(name))
            .collect();
        let new: Vec<&String> = failed.difference(&before).collect();
        let fixed: Vec<&String> = before.difference(&failed).collect();
        println!("newly failing ({}): {}", new.len(), join(&new));
        println!("newly passing ({}): {}", fixed.len(), join(&fixed));
    }
    if let Some(save) = save {
        let list: Vec<&str> = failed.iter().map(String::as_str).collect();
        std::fs::write(&save, list.join("\n") + "\n").expect("save failures");
    }
    std::process::exit(0);
}

type Queue = Arc<Mutex<Vec<(String, PathBuf)>>>;
type Running = Arc<Mutex<BTreeMap<String, Instant>>>;

/// Runs queued suites one at a time on a one-thread pool, so a suite never
/// runs another suite's work while it waits on its own.
fn work(queue: &Queue, running: &Running, done: &mpsc::Sender<(String, bool, f64)>) {
    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(1)
        .stack_size(code_graph_incremental::driver::WORKER_STACK_BYTES)
        .build()
        .expect("suite pool");
    loop {
        let Some((name, path)) = queue.lock().expect("queue").pop() else {
            return;
        };
        let yaml = std::fs::read_to_string(&path).expect("fixture");
        let at = Instant::now();
        running.lock().expect("running").insert(name.clone(), at);
        eprintln!("start {name}");
        let run = std::panic::catch_unwind(AssertUnwindSafe(|| {
            pool.install(|| integration_tests_codegraph::check_incremental_suite(&yaml))
        }));
        running.lock().expect("running").remove(&name);
        eprintln!("end {name}");
        let _ = done.send((name, run.is_ok(), at.elapsed().as_secs_f64()));
    }
}

fn join(names: &[&String]) -> String {
    names
        .iter()
        .map(|n| n.as_str())
        .collect::<Vec<_>>()
        .join(" ")
}

/// Every suite file under `dir`, named as the test harness names it.
fn find(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) {
    for entry in std::fs::read_dir(dir).expect("fixture dir").flatten() {
        let path = entry.path();
        if path.is_dir() {
            find(root, &path, out);
        } else if path.extension().is_some_and(|e| e == "yaml") {
            let rel = path
                .strip_prefix(root)
                .expect("under root")
                .to_string_lossy();
            let name = rel
                .replace(['/', '.', '-'], "_")
                .trim_end_matches("_yaml")
                .to_string();
            out.push((name, path));
        }
    }
}
