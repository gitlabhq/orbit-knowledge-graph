use std::path::Path;

use code_graph_incremental::tree::{Compact, EdgeKind};
type Cursor<'a> = code_graph_incremental::tree::Cursor<'a, Compact>;
use code_graph_incremental::language::SupportLang;
use code_graph_incremental::{Env, Limits, Sentinel, State, driver};
mod common;

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

fn link_repo(env: &Env, root: &Path) -> State {
    driver::index(env, &common::walk(root), &Sentinel::disabled())
        .unwrap()
        .0
}

fn python_env(limits: Limits) -> Env {
    Env::builder(SupportLang::Python)
        .limits(limits)
        .build()
        .unwrap()
}

fn label(env: &Env, node: Cursor) -> String {
    let sym = node
        .child_sym_of_kind(env.lang.lookup_kind("__defname"))
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

    let state = link_repo(&env, repo.path());

    assert!(state.edges.iter().all(|e| e.from_fi() == e.to_fi()));
    assert_eq!(
        edges(&env, &state, env.lang.edge_kind("defines").unwrap()),
        [pair("Greeter", "greet")]
    );
    assert_eq!(
        edges(&env, &state, env.lang.edge_kind("calls").unwrap()),
        [pair("run", "Greeter"), pair("run", "greet")],
        "g = Greeter(); g.greet() resolves through the local type"
    );
    assert_eq!(
        edges(&env, &state, env.lang.edge_kind("imports").unwrap()),
        [pair("greet", "helper"), pair("run", "os")],
        "each use of an imported name points at the import"
    );
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

    let state = link_repo(&env, repo.path());

    let reason_key = env.lang.syms.lookup("reason");
    let mut rows: Vec<(String, usize, Option<&str>)> = state
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
}

#[test]
fn a_killed_file_keeps_a_row_tagged_with_the_timeout() {
    let repo = tempfile::tempdir().unwrap();
    write_all(repo.path(), &[("main.py", MAIN.as_bytes())]);
    let env = python_env(Limits {
        file_rewrite_ms: 0,
        ..Limits::UNLIMITED
    });

    let state = link_repo(&env, repo.path());

    let reason_key = env.lang.syms.lookup("reason");
    let tree = &state.trees[0];
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
    assert!(state.edges.is_empty());
}

fn link_nested(source: String, file_link_ms: u64) -> Result<(), String> {
    link_one(SupportLang::JavaScript, "deep.js", source, file_link_ms)
}

fn link_one(
    language: SupportLang,
    path: &str,
    source: String,
    file_link_ms: u64,
) -> Result<(), String> {
    let path = path.to_string();
    std::thread::Builder::new()
        .stack_size(driver::WORKER_STACK_BYTES)
        .spawn(move || {
            let env = Env::builder(language)
                .limits(Limits {
                    file_link_ms,
                    ..Limits::UNLIMITED
                })
                .build()
                .unwrap();
            let repo = tempfile::tempdir().unwrap();
            write_all(repo.path(), &[(&path, source.as_bytes())]);
            let (_, report) =
                driver::index(&env, &common::walk(repo.path()), &Sentinel::disabled()).unwrap();
            match report.skipped.first() {
                None => Ok(()),
                Some(killed) => Err(killed.to_string()),
            }
        })
        .unwrap()
        .join()
        .unwrap()
}

#[test]
fn linking_deeply_nested_blocks_stays_within_the_link_budget() {
    let levels = 8_000;
    let mut source = String::from("function f(x) {\nlet y = 0;\n");
    for i in 0..levels {
        source += &format!("if (x) {{\nlet v{i} = {i};\ny = y + 1;\n");
    }
    source += &"}\n".repeat(levels);
    source += "return y;\n}\n";
    assert_eq!(link_nested(source, 3_000), Ok(()));
}

#[test]
fn linking_deeply_nested_loops_stays_within_the_link_budget() {
    let levels = 3_000;
    let mut source = String::from("function a() {}\nfunction f(c) {\n");
    for i in 0..levels {
        source += &format!("while (c) {{\nlet x{i} = c;\n");
    }
    source += "a();\n";
    source += &"}\n".repeat(levels);
    source += "}\n";
    assert_eq!(link_nested(source, 1_000), Ok(()));
}

#[test]
fn nested_member_writes_in_many_functions_stay_within_the_link_budget() {
    let source: String = (0..40)
        .map(|i| {
            format!(
                "def test_{i}(m):\n    c = AsyncMock()\n    c.a = 1\n    c.send = AsyncMock()\n    m.r.e = c\n    w = Workflow(m, c)\n"
            )
        })
        .collect();
    assert_eq!(
        link_one(SupportLang::Python, "test_workflow.py", source, 2_000),
        Ok(())
    );
}
