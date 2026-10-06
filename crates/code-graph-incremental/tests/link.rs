use std::path::Path;

use code_graph_incremental::canonical::Canonical as C;
use code_graph_incremental::pipeline::{
    Canonicalize, DirtyGraph, Each, Insert, Link, Parse, Prepare, Rewrite, Sources,
};
use code_graph_incremental::tree::{Cursor, EdgeKind};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, ItemPhase, Limits, Pipeline, State, inventory};

const MAIN: &str = "\
import os
from utils import helper

class Greeter:
    def greet(self, name):
        return helper(name)

def run():
    g = Greeter()
    g.greet(os.getcwd())
";

fn write_all(root: &Path, files: &[(&str, &[u8])]) {
    for (path, content) in files {
        std::fs::write(root.join(path), content).unwrap();
    }
}

fn link_repo(env: &Env, root: &Path) -> DirtyGraph {
    let entries = inventory::walk(root).unwrap().into_inner();
    let sources = Sources {
        root: root.to_path_buf(),
        entries,
    };
    Pipeline::new(Context::new(env), sources)
        .then(Prepare)
        .unwrap()
        .then(Each(Parse.pipe(Rewrite).pipe(Canonicalize).pipe(Link)))
        .unwrap()
        .then(Insert)
        .unwrap()
        .into_value()
}

fn python_env(limits: Limits) -> Env {
    Env::with_limits(SupportLang::Python, limits).unwrap()
}

fn label(env: &Env, node: Cursor) -> String {
    let sym = node
        .child_sym_of_kind(C::DefName as u16)
        .or(node.sym_opt())
        .unwrap_or(0);
    env.lang.syms.resolve(sym).to_string()
}

fn edges(env: &Env, state: &State, kind: EdgeKind) -> Vec<(String, String)> {
    let mut found: Vec<_> = state
        .edges
        .iter()
        .filter(|e| e.kind == kind)
        .map(|e| {
            let tree = &state.trees[e.from_fi()];
            (
                label(env, tree.cursor(e.from_node)),
                label(env, state.trees[e.to_fi()].cursor(e.to_node)),
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
fn linking_a_file_yields_its_local_edges() {
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[("main.py", MAIN.as_bytes())]);
    let env = python_env(Limits::UNLIMITED);

    let graph = link_repo(&env, repo.path());

    assert!(graph.state.edges.iter().all(|e| e.from_fi() == e.to_fi()));
    assert_eq!(
        edges(&env, &graph.state, EdgeKind::Defines),
        [pair("Greeter", "greet")]
    );
    assert_eq!(
        edges(&env, &graph.state, EdgeKind::Calls),
        [pair("run", "Greeter"), pair("run", "greet")],
        "g = Greeter(); g.greet() resolves through the local type"
    );
    assert_eq!(
        edges(&env, &graph.state, EdgeKind::Imports),
        [pair("greet", "helper"), pair("run", "os")],
        "each use of an imported name points at the import"
    );
}

#[test]
fn tuple_bindings_read_all_values_before_shadowing() {
    let repo = tempfile::tempdir().unwrap();
    write_all(
        repo.path(),
        &[(
            "main.rs",
            br#"
        fn first() {}
        fn second() {}
        fn make() -> fn() { first }
        fn swapped() {
            let (first, second) = (second, first);
            first();
        }
        fn ignored() {
            let (_, callback) = (first, second);
            callback();
        }
        fn opaque(callback: fn()) {
            let (first, _) = (callback, second);
            first();
        }
        fn initializer_calls() {
            let (_, _) = (make(), make());
        }
    "#,
        )],
    );
    let env = Env::with_limits(SupportLang::Rust, Limits::UNLIMITED).unwrap();
    let graph = link_repo(&env, repo.path());
    assert_eq!(
        edges(&env, &graph.state, EdgeKind::Calls),
        [
            pair("ignored", "second"),
            pair("initializer_calls", "make"),
            pair("initializer_calls", "make"),
            pair("swapped", "second")
        ]
    );
}

#[test]
fn loop_calls_keep_their_reaching_values_at_each_site() {
    let repo = tempfile::tempdir().unwrap();
    write_all(
        repo.path(),
        &[(
            "main.rs",
            br#"
        fn first() {}
        fn second() {}
        fn before(again: bool) {
            let mut callback: fn() = first;
            while again {
                callback();
                callback = second;
            }
        }
        fn after(again: bool) {
            let mut callback: fn() = first;
            while again {
                callback = second;
                callback();
            }
        }
        fn unchanged(again: bool) {
            let callback: fn() = first;
            while again { callback(); }
        }
    "#,
        )],
    );
    let env = Env::with_limits(SupportLang::Rust, Limits::UNLIMITED).unwrap();
    let graph = link_repo(&env, repo.path());
    assert_eq!(
        edges(&env, &graph.state, EdgeKind::Calls),
        [
            pair("after", "second"),
            pair("before", "first"),
            pair("before", "second"),
            pair("unchanged", "first")
        ]
    );
}

#[test]
fn record_callback_uses_its_local_field() {
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[("main.rs", b"struct H { callback: fn() } fn first() {} fn run() { let h = H { callback: first }; (h.callback)(); }")]);
    let env = Env::with_limits(SupportLang::Rust, Limits::UNLIMITED).unwrap();
    let graph = link_repo(&env, repo.path());
    assert!(edges(&env, &graph.state, EdgeKind::Calls).contains(&pair("run", "first")));
}

#[test]
fn field_updates_keep_copies_and_siblings_separate() {
    let repo = tempfile::tempdir().unwrap();
    write_all(
        repo.path(),
        &[(
            "main.rs",
            br#"
        #[derive(Clone, Copy)]
        struct H { callback: fn(), other: fn() }
        fn first() {}
        fn second() {}
        fn copied() {
            let mut h = H { callback: first, other: second };
            let saved = h;
            h.callback = second;
            (saved.callback)();
        }
        fn selected() {
            let h = H { callback: first, other: second };
            let callback = h.other;
            callback();
        }
        fn unknown(callback: fn()) {
            let mut h = H { callback: first, other: second };
            h.callback = callback;
            (h.callback)();
        }
        fn separate(h: H) {
            (h.callback)();
        }
    "#,
        )],
    );
    let env = Env::with_limits(SupportLang::Rust, Limits::UNLIMITED).unwrap();
    let graph = link_repo(&env, repo.path());
    let calls: Vec<_> = edges(&env, &graph.state, EdgeKind::Calls)
        .into_iter()
        .filter(|(_, target)| target == "first" || target == "second")
        .collect();
    assert_eq!(calls, [pair("copied", "first"), pair("selected", "second")]);
}

#[test]
fn every_file_gets_a_tree_and_unparsed_ones_carry_their_reason() {
    let repo = tempfile::tempdir().unwrap();
    write_all(
        repo.path(),
        &[
            ("main.py", MAIN.as_bytes()),
            ("README.md", b"# hi\n"),
            ("logo.png", b"\x89PNG\x00\x00"),
        ],
    );
    let env = python_env(Limits::UNLIMITED);

    let graph = link_repo(&env, repo.path());

    let reason_key = env.lang.syms.lookup("reason");
    let mut rows: Vec<(String, usize, Option<&str>)> = graph
        .state
        .trees
        .iter()
        .map(|t| {
            let reason = t.get_tag(0, reason_key).map(|v| env.lang.syms.resolve(v));
            (t.label.clone(), t.root().descendants().count(), reason)
        })
        .collect();
    rows.sort();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], ("README.md".into(), 0, None));
    assert_eq!(
        rows[1],
        ("logo.png".into(), 0, Some("skip_excluded_extension"))
    );
    assert_eq!(rows[2].0, "main.py");
    assert!(rows[2].1 > 0);
    assert_eq!(graph.dirty.len(), 1, "only the parsed file needs resolving");
}

#[test]
fn a_killed_file_keeps_a_row_tagged_with_the_timeout() {
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[("main.py", MAIN.as_bytes())]);
    let env = python_env(Limits {
        file_rewrite_ms: 0,
        ..Limits::UNLIMITED
    });

    let graph = link_repo(&env, repo.path());

    let reason_key = env.lang.syms.lookup("reason");
    let tree = &graph.state.trees[0];
    assert_eq!(tree.label, "main.py");
    assert_eq!(tree.root().descendants().count(), 0);
    assert_eq!(
        tree.root().end(),
        MAIN.len() as u32,
        "the row spans the file"
    );
    assert_eq!(
        tree.get_tag(0, reason_key)
            .map(|v| env.lang.syms.resolve(v)),
        Some("skip_timeout_walk")
    );
    assert!(graph.state.edges.is_empty());
}
