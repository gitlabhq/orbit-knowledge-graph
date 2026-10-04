use std::path::Path;

use code_graph_incremental::canonical::Canonical as C;
use code_graph_incremental::pipeline::{
    Canonicalize, Each, Insert, Link, Parse, Prepare, Resolve, Resolved, Rewrite, Sources,
};
use code_graph_incremental::tree::{Cursor, EdgeKind};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, ItemPhase, Killed, Limits, Pipeline, State, inventory};

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
    let entries = inventory::walk(root).unwrap().into_inner();
    let sources = Sources {
        root: root.to_path_buf(),
        entries,
    };
    let (context, resolved) = Pipeline::new(Context::new(env), sources)
        .then(Prepare)
        .unwrap()
        .then(Each(Parse.pipe(Rewrite).pipe(Canonicalize).pipe(Link)))
        .unwrap()
        .then(Insert)
        .unwrap()
        .then(Resolve)
        .unwrap()
        .finish();
    (resolved, context.report.skipped)
}

fn python_env(limits: Limits) -> Env {
    Env::with_limits(SupportLang::Python, limits).unwrap()
}

fn label(env: &Env, state: &State, fi: usize, node: u32) -> String {
    let cursor: Cursor = state.trees[fi].cursor(node);
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

    let mut paths: Vec<_> = skipped.iter().map(|k| (k.label, k.path.as_str())).collect();
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
