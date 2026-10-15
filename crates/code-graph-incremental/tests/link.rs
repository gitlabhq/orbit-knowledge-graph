use std::path::Path;

use code_graph_incremental::canonical::Canonical as C;
use code_graph_incremental::pipeline::{
    Canonicalize, DirtyGraph, Each, Insert, Link, Parse, Prepare, Rewrite,
};
use code_graph_incremental::tree::{Compact, EdgeKind};
type Cursor<'a> = code_graph_incremental::tree::Cursor<'a, Compact>;
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, ItemPhase, Limits, Pipeline, State};
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

fn link_repo(env: &Env, root: &Path) -> DirtyGraph {
    let sources = common::walk(root);
    Pipeline::new(Context::new(env, sources), ())
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

#[test]
fn linking_deeply_nested_blocks_grows_linearly() {
    use code_graph_incremental::pipeline::SourceFile;
    use code_graph_incremental::{Sentinel, templates};
    use std::time::Instant;

    fn link_ms(levels: usize) -> f64 {
        let mut source = String::from("function f(x) {\nlet y = 0;\n");
        for i in 0..levels {
            source += &format!("if (x) {{\nlet v{i} = {i};\ny = y + 1;\n");
        }
        source += &"}\n".repeat(levels);
        source += "return y;\n}\n";
        let env = Env::with_limits(SupportLang::JavaScript, Limits::UNLIMITED).unwrap();
        let free = Sentinel::disabled();
        let file = SourceFile::from(("deep.js".to_string(), source));
        let parsed = Parse.run(&env, &free, file).unwrap();
        let rewritten = Rewrite.run(&env, &free, parsed).unwrap();
        let canonical = Canonicalize.run(&env, &free, rewritten).unwrap();
        let started = Instant::now();
        Link.run(&env, &free, canonical).unwrap();
        started.elapsed().as_secs_f64() * 1000.0
    }

    let (small, large) = std::thread::Builder::new()
        .stack_size(templates::WORKER_STACK_BYTES)
        .spawn(|| (link_ms(2_000), link_ms(8_000)))
        .unwrap()
        .join()
        .unwrap();
    assert!(
        large < 8.0 * small.max(1.0),
        "linking 4x the nesting went from {small:.0} ms to {large:.0} ms"
    );
}
