//! Runs every incremental fixture suite in one process and compares the
//! failures with a saved list.
//!
//! `cargo run -q -p integration-tests-codegraph --profile gate --example gate -- [--save FILE] [--base FILE] [NAME...] 2>LOG`
//!
//! With `CGI_RULES_DIR` set to the crate dir of code-graph-incremental, rules
//! and fixtures are read from disk, so a YAML change needs no rebuild.

use std::collections::BTreeSet;
use std::panic::AssertUnwindSafe;
use std::path::{Path, PathBuf};
use std::time::Instant;

use rayon::prelude::*;

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
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("fixtures_incremental");
    let mut suites = Vec::new();
    find(&root, &root, &mut suites);
    suites.sort();
    if !names.is_empty() {
        suites.retain(|(name, _)| names.iter().any(|n| n == name));
    }
    std::panic::set_hook(Box::new(|info| eprintln!("{info}")));
    let started = Instant::now();
    let pool = rayon::ThreadPoolBuilder::new()
        .stack_size(code_graph_incremental::driver::WORKER_STACK_BYTES)
        .build()
        .expect("worker pool");
    let runs: Vec<(String, bool, f64)> = pool.install(|| {
        suites
            .par_iter()
            .map(|(name, path)| {
                let yaml = std::fs::read_to_string(path).expect("fixture");
                let started = Instant::now();
                let run = std::panic::catch_unwind(AssertUnwindSafe(|| {
                    integration_tests_codegraph::check_incremental_suite(&yaml)
                }));
                (name.clone(), run.is_ok(), started.elapsed().as_secs_f64())
            })
            .collect()
    });
    let failed: BTreeSet<String> = runs
        .iter()
        .filter(|(_, ok, _)| !ok)
        .map(|(name, _, _)| name.clone())
        .collect();
    let elapsed = started.elapsed();
    println!(
        "{} suites: {} passing, {} failing in {:.1}s",
        suites.len(),
        suites.len() - failed.len(),
        failed.len(),
        elapsed.as_secs_f64()
    );
    let mut slow: Vec<&(String, bool, f64)> = runs.iter().filter(|(_, _, s)| *s >= 0.5).collect();
    slow.sort_by(|a, b| b.2.total_cmp(&a.2));
    for (name, ok, secs) in slow.iter().take(25) {
        println!("  {secs:8.2}s {} {name}", if *ok { "pass" } else { "FAIL" });
    }
    if let Some(base) = base {
        let before: BTreeSet<String> = std::fs::read_to_string(&base)
            .unwrap_or_default()
            .lines()
            .map(str::to_string)
            .collect();
        let new: Vec<_> = failed.difference(&before).collect();
        let fixed: Vec<_> = before.difference(&failed).collect();
        println!("newly failing ({}): {}", new.len(), join(&new));
        println!("newly passing ({}): {}", fixed.len(), join(&fixed));
    }
    if let Some(save) = save {
        let list: Vec<&str> = failed.iter().map(String::as_str).collect();
        std::fs::write(&save, list.join("\n") + "\n").expect("save failures");
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
