use std::path::Path;

use code_graph_incremental::canonical::Canonical as C;
use code_graph_incremental::pipeline::{Changes, Resolved};
use code_graph_incremental::tree::{Cursor, EdgeKind};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, Killed, Limits, State, inventory, templates};

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
    let (context, resolved) = templates::index(Context::new(env), root, entries)
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
fn repeated_provider_imports_share_lookup_but_project_additions_still_resolve() {
    let repo = tempfile::tempdir().unwrap();
    let source = "import json\nimport sys\ndef run():\n    json.dumps({})\n    sys.getsizeof(1)\n";
    for index in 0..100 {
        std::fs::write(repo.path().join(format!("consumer_{index}.py")), source).unwrap();
    }
    let env = python_env(Limits::UNLIMITED);
    let entries = inventory::walk(repo.path()).unwrap().into_inner();
    let (context, resolved) = templates::index(Context::new(&env), repo.path(), entries)
        .unwrap()
        .finish();
    let stats = &context.report.import_lookups;
    assert_eq!(stats.sites, 200);
    assert_eq!(stats.unique_contexts, 2);
    assert_eq!(stats.provider_contexts, 2);
    assert_eq!(stats.module_searches, 1);
    assert_eq!(stats.module_searches_avoided, 99);
    assert!(cross_file(&env, &resolved.state, EdgeKind::Calls).is_empty());

    let snapshot = tempfile::NamedTempFile::new().unwrap();
    resolved.state.save(&env, snapshot.path()).unwrap();
    let (env, state) = State::load(snapshot.path(), SupportLang::Python).unwrap();
    write_all(repo.path(), &[("json.py", "def dumps(value): pass\n")]);
    let (_, resolved) = templates::reindex(
        Context::new(&env),
        state,
        repo.path(),
        Changes {
            changed: inventory::classify(repo.path(), ["json.py".into()]),
            removed: vec![],
        },
    )
    .unwrap()
    .finish();
    assert_eq!(
        cross_file(&env, &resolved.state, EdgeKind::Calls).len(),
        100
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
        Context::new(&env),
        state,
        repo.path(),
        Changes {
            changed: vec![],
            removed: vec![],
        },
    )
    .unwrap()
    .finish();
    assert!(context.report.files.is_empty());
    assert_eq!(
        cross_file(&env, &resolved.state, EdgeKind::Calls),
        [pair("main.js:caller", "package/first.js:run")]
    );
    write_all(
        repo.path(),
        &[("package/package.json", r#"{"main":"second.js"}"#)],
    );
    let changes = Changes {
        changed: inventory::classify(repo.path(), ["package/package.json".into()]),
        removed: vec![],
    };
    let (context, resolved) =
        templates::reindex(Context::new(&env), resolved.state, repo.path(), changes)
            .unwrap()
            .finish();
    assert!(context.report.skipped.is_empty());
    let mut resolved_files: Vec<_> = context
        .report
        .files
        .iter()
        .map(|file| {
            assert_eq!(file.phase, "resolve");
            file.path.as_str()
        })
        .collect();
    resolved_files.sort();
    assert_eq!(resolved_files, ["barrel.js", "main.js"]);
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
    let changes = Changes {
        changed: inventory::classify(repo.path(), ["left.js".into(), "right.js".into()]),
        removed: vec![],
    };
    let (context, resolved) =
        templates::reindex(Context::new(&env), resolved.state, repo.path(), changes)
            .unwrap()
            .finish();
    let mut resolved_files: Vec<_> = context
        .report
        .files
        .iter()
        .filter(|file| file.phase == "resolve")
        .map(|file| file.path.as_str())
        .collect();
    resolved_files.sort();
    assert_eq!(
        resolved_files,
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
