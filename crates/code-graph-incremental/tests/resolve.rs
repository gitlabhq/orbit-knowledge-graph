use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use code_graph_incremental::canonical::Canonical as C;
use code_graph_incremental::pipeline::{FileStats, Resolved};
use code_graph_incremental::tree::{Cursor, EdgeKind};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, Killed, Limits, State, templates};
mod common;

const UTILS: &str = "\
def helper(x):
    return x

class Store:
    def get(self):
        pass
";

const MAIN: &str = "\
from utils import helper, Store
import missing

class Child(Store):
    pass

def run():
    helper(1)
    s = Store()
    s.get()
    missing.thing()
";

fn write_all(root: &Path, files: &[(&str, &str)]) {
    for (path, content) in files {
        std::fs::write(root.join(path), content).unwrap();
    }
}

fn resolve_repo(env: &Env, root: &Path) -> (Resolved, Vec<Killed>) {
    let (context, resolved) = templates::index(Context::new(env, common::walk(root)))
        .unwrap()
        .finish();
    (resolved, context.report.skipped)
}

fn resolved_paths(resolved: &Resolved) -> Vec<&str> {
    let mut paths: Vec<_> = resolved
        .resolved
        .iter()
        .map(|&fi| resolved.state.trees[fi as usize].label.as_str())
        .collect();
    paths.sort();
    paths
}

fn python_env(limits: Limits) -> Env {
    Env::with_limits(SupportLang::Python, limits).unwrap()
}

fn label(env: &Env, state: &State, fi: usize, node: u32) -> String {
    let cursor: Cursor<'_, code_graph_incremental::tree::Compact> = state.trees[fi].cursor(node);
    let sym = cursor
        .child_sym_of_kind(C::DefName as u16)
        .or(cursor.sym_opt())
        .unwrap_or(0);
    format!("{}:{}", state.trees[fi].label, env.lang.syms.resolve(sym))
}

fn cross_file(env: &Env, state: &State, kind: EdgeKind) -> Vec<(String, String)> {
    let mut found: Vec<_> = state
        .edges
        .iter()
        .filter(|e| e.kind == kind && e.from_fi() != e.to_fi())
        .map(|e| {
            (
                label(env, state, e.from_fi(), e.from_node),
                label(env, state, e.to_fi(), e.to_node),
            )
        })
        .collect();
    found.sort();
    found
}

fn pair(a: &str, b: &str) -> (String, String) {
    (a.into(), b.into())
}

#[test]
fn cross_file_timeout_keeps_resolved_edges_and_recovers_on_noop_reindex() {
    let repo = tempfile::tempdir().unwrap();
    std::fs::create_dir(repo.path().join("pkg")).unwrap();
    let main = format!("from pkg import mod\n{MAIN}    mod.f()\n");
    write_all(
        repo.path(),
        &[
            ("utils.py", UTILS),
            ("main.py", &main),
            ("pkg/mod.py", "def f():\n    pass\n"),
        ],
    );
    let kinds = [EdgeKind::Imports, EdgeKind::Calls, EdgeKind::Extends];
    let mut env = python_env(Limits::UNLIMITED);
    let (baseline, _) = resolve_repo(&env, repo.path());
    let expected = kinds.map(|kind| cross_file(&env, &baseline.state, kind));
    let empty_changes = || common::load(orbit_utils::vfs::sources::Memory(vec![]));
    let family_skip = |skipped: &[Killed]| {
        let skipped: Vec<_> = skipped
            .iter()
            .map(|k| (k.label.as_ref(), k.path.as_str(), k.cancelled))
            .collect();
        assert_eq!(skipped, [("resolve", "", false)]);
    };

    env.limits.cross_file_resolve_ms = 0;
    let (context, kept) =
        templates::reindex(Context::new(&env, empty_changes()), baseline.state, vec![])
            .unwrap()
            .finish();
    family_skip(&context.report.skipped);
    assert_eq!(
        kinds.map(|kind| cross_file(&env, &kept.state, kind)),
        expected
    );

    let (degraded, skipped) = resolve_repo(&env, repo.path());
    family_skip(&skipped);
    assert_eq!(degraded.state.trees.len(), 3);
    assert_eq!(
        kinds.map(|kind| cross_file(&env, &degraded.state, kind)),
        [
            vec![pair("main.py:", "pkg/mod.py:pkg/mod.py")],
            vec![],
            vec![]
        ]
    );

    env.limits.cross_file_resolve_ms = u64::MAX;
    let (context, recovered) =
        templates::reindex(Context::new(&env, empty_changes()), degraded.state, vec![])
            .unwrap()
            .finish();
    assert!(context.report.skipped.is_empty());
    assert_eq!(
        kinds.map(|kind| cross_file(&env, &recovered.state, kind)),
        expected
    );
}

#[test]
fn resolve_cancellation_and_run_deadline_remain_fatal() {
    use code_graph_incremental::pipeline::{
        Canonicalize, Each, Insert, ItemPhase, Link, Parse, Pipeline, Prepare, Resolve,
    };
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[("main.py", MAIN), ("utils.py", UTILS)]);
    let env = python_env(Limits::UNLIMITED);
    for cancelled in [false, true] {
        let (mut context, graph) = Pipeline::new(Context::new(&env, common::walk(repo.path())), ())
            .then(Prepare)
            .unwrap()
            .then(Each(
                Parse
                    .pipe(code_graph_incremental::pipeline::Rewrite)
                    .pipe(Canonicalize)
                    .pipe(Link),
            ))
            .unwrap()
            .then(Insert)
            .unwrap()
            .finish();
        context.run = code_graph_incremental::sentinel::Sentinel::new(
            "run",
            "",
            if cancelled { u64::MAX } else { 0 },
        )
        .with_cancellation(std::sync::Arc::new(move || cancelled));
        let error = Pipeline::new(context, graph)
            .then(Resolve)
            .err()
            .expect("run termination must fail");
        assert!(
            error
                .to_string()
                .contains(if cancelled { "cancelled" } else { "budget" })
        );
    }
}

#[test]
fn imports_and_calls_resolve_across_files() {
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[("utils.py", UTILS), ("main.py", MAIN)]);
    let env = python_env(Limits::UNLIMITED);

    let (resolved, skipped) = resolve_repo(&env, repo.path());

    assert!(skipped.is_empty());
    assert_eq!(
        cross_file(&env, &resolved.state, EdgeKind::Imports),
        [
            pair("main.py:Store", "utils.py:Store"),
            pair("main.py:helper", "utils.py:helper"),
        ]
    );
    assert_eq!(
        cross_file(&env, &resolved.state, EdgeKind::Calls),
        [
            pair("main.py:run", "utils.py:Store"),
            pair("main.py:run", "utils.py:get"),
            pair("main.py:run", "utils.py:helper"),
        ],
        "s = Store(); s.get() resolves through the imported type"
    );
    assert_eq!(
        cross_file(&env, &resolved.state, EdgeKind::Extends),
        [pair("main.py:Child", "utils.py:Store")]
    );
}

#[test]
fn an_import_with_no_file_stays_dangling() {
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[("utils.py", UTILS), ("main.py", MAIN)]);
    let env = python_env(Limits::UNLIMITED);

    let (resolved, _) = resolve_repo(&env, repo.path());

    let targets: Vec<_> = cross_file(&env, &resolved.state, EdgeKind::Imports)
        .into_iter()
        .map(|(_, to)| to)
        .collect();
    assert!(
        targets.iter().all(|t| t.starts_with("utils.py:")),
        "`import missing` points at nothing: {targets:?}"
    );
}

#[test]
fn changing_an_entrypoint_resolves_only_its_importers_without_relinking() {
    let repo = tempfile::tempdir().unwrap();
    std::fs::create_dir(repo.path().join("package")).unwrap();
    write_all(
        repo.path(),
        &[
            ("package/package.json", r#"{"main":"first.js"}"#),
            ("package/first.js", "export function run() {}"),
            ("package/second.js", "export function run() {}"),
            ("barrel.js", "export { run } from './package';"),
            (
                "main.js",
                "import { run } from './barrel'; function caller() { run(); }",
            ),
            ("unrelated.js", "function untouched() {}"),
        ],
    );
    let env = Env::with_limits(SupportLang::JavaScript, Limits::UNLIMITED).unwrap();
    let (resolved, _) = resolve_repo(&env, repo.path());
    let snapshot = tempfile::NamedTempFile::new().unwrap();
    resolved.state.save(&env, snapshot.path()).unwrap();
    let (env, state) = State::load(snapshot.path(), SupportLang::JavaScript).unwrap();
    let (context, resolved) = templates::reindex(
        Context::new(
            &env,
            common::load(orbit_utils::vfs::sources::Memory(vec![])),
        ),
        state,
        vec![],
    )
    .unwrap()
    .finish();
    assert!(context.report.files.is_empty());
    assert!(resolved.resolved.is_empty());
    assert_eq!(
        cross_file(&env, &resolved.state, EdgeKind::Calls),
        [pair("main.js:caller", "package/first.js:run")]
    );
    write_all(
        repo.path(),
        &[("package/package.json", r#"{"main":"second.js"}"#)],
    );
    let changes = common::load(orbit_utils::vfs::sources::Diff {
        root: repo.path(),
        paths: vec!["package/package.json".into()],
    });
    let (context, resolved) =
        templates::reindex(Context::new(&env, changes), resolved.state, vec![])
            .unwrap()
            .finish();
    assert!(context.report.skipped.is_empty());
    assert!(context.report.files.is_empty());
    assert_eq!(resolved_paths(&resolved), ["barrel.js", "main.js"]);
    assert_eq!(
        cross_file(&env, &resolved.state, EdgeKind::Calls),
        [pair("main.js:caller", "package/second.js:run")]
    );
}

#[test]
fn overlapping_importer_cycles_are_invalidated_once_after_multiple_edits() {
    let repo = tempfile::tempdir().unwrap();
    write_all(
        repo.path(),
        &[
            ("left.js", "export function left() {}"),
            ("right.js", "export function right() {}"),
            (
                "first.js",
                "export * from './left'; export * from './right'; export * from './second';",
            ),
            ("second.js", "export * from './first';"),
            (
                "main.js",
                "import { left, right } from './second'; function caller() { left(); right(); }",
            ),
            ("unrelated.js", "function untouched() {}"),
        ],
    );
    let env = Env::with_limits(SupportLang::JavaScript, Limits::UNLIMITED).unwrap();
    let (resolved, _) = resolve_repo(&env, repo.path());
    write_all(
        repo.path(),
        &[
            ("left.js", "export function left() { const value = 1; }"),
            ("right.js", "export function right() { const value = 2; }"),
        ],
    );
    let changes = common::load(orbit_utils::vfs::sources::Diff {
        root: repo.path(),
        paths: vec!["left.js".into(), "right.js".into()],
    });
    let (context, resolved) =
        templates::reindex(Context::new(&env, changes), resolved.state, vec![])
            .unwrap()
            .finish();
    let mut stats_paths: Vec<_> = context.report.files.keys().map(String::as_str).collect();
    stats_paths.sort();
    assert_eq!(stats_paths, ["left.js", "right.js"]);
    assert_eq!(
        resolved_paths(&resolved),
        ["first.js", "left.js", "main.js", "right.js", "second.js"]
    );
    assert_eq!(
        cross_file(&env, &resolved.state, EdgeKind::Calls),
        [
            pair("main.js:caller", "left.js:left"),
            pair("main.js:caller", "right.js:right"),
        ]
    );
}

/// The import pass is global; the per-file budget covers the second pass
/// (inheritance, decorators, destructuring).
#[test]
fn a_file_over_the_resolve_budget_keeps_imports_and_loses_inheritance() {
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[("utils.py", UTILS), ("main.py", MAIN)]);
    let env = python_env(Limits {
        file_resolve_ms: 0,
        ..Limits::UNLIMITED
    });

    let (resolved, skipped) = resolve_repo(&env, repo.path());

    let mut paths: Vec<_> = skipped
        .iter()
        .map(|k| (k.label.as_ref(), k.path.as_str()))
        .collect();
    paths.sort();
    assert_eq!(paths, [("resolve", "main.py"), ("resolve", "utils.py")]);
    assert_eq!(
        cross_file(&env, &resolved.state, EdgeKind::Imports),
        [
            pair("main.py:Store", "utils.py:Store"),
            pair("main.py:helper", "utils.py:helper"),
        ]
    );
    assert!(cross_file(&env, &resolved.state, EdgeKind::Extends).is_empty());
}

fn index_stats(limits: Limits) -> (HashMap<String, FileStats>, Vec<Killed>) {
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[("a.py", UTILS), ("b.py", MAIN)]);
    let env = python_env(limits);
    let (context, _) = templates::index(Context::new(&env, common::walk(repo.path())))
        .unwrap()
        .finish();
    (context.report.files, context.report.skipped)
}

fn sorted_paths(files: &HashMap<String, FileStats>) -> Vec<&str> {
    let mut paths: Vec<_> = files.keys().map(String::as_str).collect();
    paths.sort();
    paths
}

#[test]
fn file_stats_cover_parsed_files() {
    let (files, _) = index_stats(Limits::UNLIMITED);
    assert_eq!(sorted_paths(&files), ["a.py", "b.py"]);
    for stats in files.values() {
        assert!(stats.parse > Duration::ZERO);
        assert!(stats.resolve > Duration::ZERO);
    }
}

#[test]
fn file_stats_drop_files_killed_in_a_per_file_phase() {
    for limits in [
        Limits {
            file_parse_ms: 0,
            ..Limits::UNLIMITED
        },
        Limits {
            file_rewrite_ms: 0,
            ..Limits::UNLIMITED
        },
        Limits {
            file_link_ms: 0,
            ..Limits::UNLIMITED
        },
    ] {
        let (files, skipped) = index_stats(limits);
        assert!(!skipped.is_empty(), "{limits:?}");
        assert!(files.is_empty(), "{limits:?}");
    }
}

#[test]
fn file_stats_keep_files_killed_by_the_resolve_budget() {
    let (files, skipped) = index_stats(Limits {
        file_resolve_ms: 0,
        ..Limits::UNLIMITED
    });
    assert!(!skipped.is_empty());
    assert!(skipped.iter().all(|killed| killed.label == "resolve"));
    assert_eq!(sorted_paths(&files), ["a.py", "b.py"]);
}

#[test]
fn a_join_chain_resolves_in_work_linear_in_its_reaching_pairs() {
    fn resolve_ms(joins: usize) -> f64 {
        let mut source = String::from(
            "pub struct Node;\n\nimpl Node {\n    pub fn new() -> Node {\n        Node\n    }\n\n    pub fn next(&self) -> Node {\n        Node\n    }\n}\n\npub fn walk(c: bool) {\n    let mut n = Node::new();\n",
        );
        source += &"    if c {\n        n = n.next();\n    }\n".repeat(joins);
        source += "}\n";
        let repo = tempfile::tempdir().unwrap();
        std::fs::create_dir(repo.path().join("src")).unwrap();
        write_all(
            repo.path(),
            &[
                (
                    "Cargo.toml",
                    "[package]\nname = \"chain\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
                ),
                ("src/lib.rs", &source),
            ],
        );
        let env = Env::with_limits(SupportLang::Rust, Limits::UNLIMITED).unwrap();
        let (context, _) = templates::index(Context::new(&env, common::walk(repo.path())))
            .unwrap()
            .finish();
        context
            .report
            .phases
            .iter()
            .filter(|phase| phase.name == "resolve")
            .map(|phase| phase.elapsed.as_secs_f64() * 1000.0)
            .sum()
    }

    let pool = rayon::ThreadPoolBuilder::new()
        .stack_size(templates::WORKER_STACK_BYTES)
        .build()
        .unwrap();
    let best = |joins| (0..3).map(|_| resolve_ms(joins)).fold(f64::MAX, f64::min);
    let (small, large) = pool.install(|| (best(200), best(1600)));
    assert!(
        large < 128.0 * small.max(5.0),
        "resolving 8x the joins went from {small:.0} ms to {large:.0} ms"
    );
}
