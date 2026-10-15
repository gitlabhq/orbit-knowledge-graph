use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use code_graph_incremental::canonical::Canonical as C;
use code_graph_incremental::pipeline::{Canonicalize, Link, Parse, Rewrite, SourceFile};
use code_graph_incremental::tree::EdgeKind;
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

    assert!(killed.is_empty(), "{killed:?}");
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

fn write_all(root: &Path, files: &[(&str, &str)]) {
    for (path, content) in files {
        std::fs::write(root.join(path), content).unwrap();
    }
}

fn skipped(killed: &[Killed]) -> Vec<(&str, &str)> {
    let mut found: Vec<_> = killed
        .iter()
        .map(|k| (k.label.as_ref(), k.path.as_str()))
        .collect();
    found.sort();
    found
}

#[test]
fn a_file_nested_past_the_depth_limit_keeps_what_lies_above_the_cut() {
    let repo = tempfile::tempdir().unwrap();
    let nested = format!(
        "function top() {{}}\nfunction nested(c) {{\ntop();\n{}{}}}\n",
        "if (c) {\n".repeat(200),
        "}\n".repeat(200)
    );
    write_all(repo.path(), &[("nested.js", &nested)]);
    let env = Env::with_limits(
        SupportLang::JavaScript,
        Limits {
            max_tree_depth: 64,
            ..Limits::UNLIMITED
        },
    )
    .unwrap();
    let graph = templates::index(Context::new(&env, common::walk(repo.path()))).unwrap();
    let report = &graph.context().report;
    assert!(report.skipped.is_empty());
    assert_eq!(skipped(&report.limited), [("depth", "nested.js")]);
    let state = &graph.value().state;
    let name = |tree: u32, node: u32| {
        let def = state.trees[tree as usize].cursor(node);
        env.lang
            .syms
            .resolve(def.child_sym(C::DefName).unwrap_or(0))
            .to_string()
    };
    let calls: Vec<_> = state
        .edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Calls)
        .map(|e| (name(e.from_tree, e.from_node), name(e.to_tree, e.to_node)))
        .collect();
    assert_eq!(calls, [("nested".to_string(), "top".to_string())]);
}
