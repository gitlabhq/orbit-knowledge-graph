use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use code_graph_incremental::pipeline::{Canonicalize, Link, Parse, Rewrite, SourceFile};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, ItemPhase, Killed, Limits, Sentinel, templates};
mod common;

const MAIN: &str = "\
from utils import helper

class Greeter:
    def greet(self, name):
        return helper(name)
";

const UTILS: &str = "\
def helper(x):
    return x

class Store:
    pass
";

const BUDGET_MS: u64 = 20;
const STALL: Duration = Duration::from_millis(3 * BUDGET_MS);

fn sleeps_once() -> Sentinel {
    let slept = AtomicBool::new(false);
    Sentinel::new("run", "", u64::MAX).with_cancellation(Arc::new(move || {
        if !slept.swap(true, Ordering::Relaxed) {
            std::thread::sleep(STALL);
        }
        false
    }))
}

fn sleeps_always() -> Sentinel {
    Sentinel::new("run", "", u64::MAX).with_cancellation(Arc::new(|| {
        std::thread::sleep(Duration::from_millis(BUDGET_MS / 4));
        false
    }))
}

fn python(limits: Limits) -> Env {
    Env::with_limits(SupportLang::Python, limits).unwrap()
}

fn source() -> SourceFile {
    SourceFile::from(("main.py".to_string(), MAIN.to_string()))
}

#[test]
fn per_file_budgets_count_thread_cpu_time_not_time_asleep() {
    let env = python(Limits {
        file_parse_ms: BUDGET_MS,
        file_rewrite_ms: BUDGET_MS,
        file_link_ms: BUDGET_MS,
        ..Limits::UNLIMITED
    });
    let free = Sentinel::disabled();
    let parse = |run: &Sentinel| Parse.run(&env, run, source());
    let rewrite = |run: &Sentinel| Rewrite.run(&env, run, parse(&free).unwrap());
    let link = |run: &Sentinel| {
        let canonical = Canonicalize.run(&env, &free, rewrite(&free).unwrap());
        Link.run(&env, run, canonical.unwrap())
    };

    let killed: Vec<_> = [
        parse(&sleeps_once()).err(),
        rewrite(&sleeps_once()).err(),
        link(&sleeps_once()).err(),
    ]
    .into_iter()
    .flatten()
    .map(|killed| killed.label)
    .collect();

    assert_eq!(killed, Vec::<&str>::new());
}

#[test]
fn the_per_file_resolve_budget_counts_thread_cpu_time() {
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[("main.py", MAIN), ("utils.py", UTILS)]);
    let env = python(Limits {
        file_resolve_ms: BUDGET_MS,
        ..Limits::UNLIMITED
    });
    let mut context = Context::new(&env, common::walk(repo.path()));
    context.run = sleeps_always();

    let (context, _) = templates::index(context).unwrap().finish();

    assert_eq!(skipped(&context.report.skipped), Vec::<(&str, &str)>::new());
}

#[test]
fn a_parse_stops_before_the_thread_runs_out_of_stack() {
    let env = python(Limits::UNLIMITED);
    let source = "def f(x):\n    return x\n".repeat(2_000);

    let killed = std::thread::scope(|scope| {
        std::thread::Builder::new()
            .stack_size(128 * 1024)
            .spawn_scoped(scope, || {
                let file = SourceFile::from(("main.py".to_string(), source));
                Parse.run(&env, &Sentinel::disabled(), file).err()
            })
            .unwrap()
            .join()
            .unwrap()
    });

    assert_eq!(killed.map(|killed| killed.label), Some("tree-sitter"));
}

#[test]
fn js_ts_and_rust_files_answer_to_the_watchdog_not_the_phase_budgets() {
    let phases = Limits {
        file_parse_ms: 0,
        file_rewrite_ms: 0,
        file_link_ms: 0,
        ..Limits::UNLIMITED
    };
    let watchdog = Limits {
        file_resolve_ms: 0,
        ..Limits::UNLIMITED
    };
    let watched = [
        (
            SupportLang::JavaScript,
            "main.js",
            "export function f() { return 1; }\n",
        ),
        (
            SupportLang::TypeScript,
            "main.ts",
            "export function f() { return 1; }\n",
        ),
        (
            SupportLang::Tsx,
            "main.tsx",
            "export function f() { return 1; }\n",
        ),
        (SupportLang::Rust, "lib.rs", "pub fn f() -> u32 { 1 }\n"),
    ];

    for (lang, path, source) in watched {
        assert_eq!(
            index_one(lang, path, source, phases),
            Vec::<&str>::new(),
            "{path}"
        );
        assert_eq!(
            index_one(lang, path, source, watchdog),
            ["tree-sitter"],
            "{path}"
        );
    }
    let python = "def f():\n    return 1\n";
    assert_eq!(
        index_one(SupportLang::Python, "main.py", python, phases),
        ["tree-sitter"]
    );
}

fn index_one(lang: SupportLang, path: &str, source: &str, limits: Limits) -> Vec<&'static str> {
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[(path, source)]);
    let env = Env::with_limits(lang, limits).unwrap();
    let (context, _) = templates::index(Context::new(&env, common::walk(repo.path())))
        .unwrap()
        .finish();
    context.report.skipped.iter().map(|k| k.label).collect()
}

fn write_all(root: &Path, files: &[(&str, &str)]) {
    for (path, content) in files {
        std::fs::write(root.join(path), content).unwrap();
    }
}

fn skipped(killed: &[Killed]) -> Vec<(&'static str, &str)> {
    let mut found: Vec<_> = killed.iter().map(|k| (k.label, k.path.as_str())).collect();
    found.sort();
    found
}
