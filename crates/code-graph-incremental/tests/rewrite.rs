use std::path::Path;

use code_graph_incremental::tree::Compact;
type Tree = code_graph_incremental::tree::Tree<Compact>;
type Cursor<'a> = code_graph_incremental::tree::Cursor<'a, Compact>;
use code_graph_incremental::language::SupportLang;
use code_graph_incremental::{Env, Killed, Limits, Sentinel, driver};
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

fn rewrite_repo(env: &Env, root: &Path) -> (Vec<Tree>, Vec<Killed>) {
    let (state, report) = driver::index(env, &common::walk(root), &Sentinel::disabled()).unwrap();
    (state.trees, report.skipped)
}

fn python_env(limits: Limits) -> Env {
    Env::builder(SupportLang::Python)
        .limits(limits)
        .build()
        .unwrap()
}

fn nodes<'a>(env: &Env, tree: &'a Tree, kind: &str) -> impl Iterator<Item = Cursor<'a>> {
    let kind = env.lang.lookup_kind(kind);
    tree.root().descendants().filter(move |c| c.is(kind))
}

fn name_of(env: &Env, node: Cursor) -> String {
    let sym = node
        .child_sym_of_kind(env.lang.lookup_kind("__name"))
        .unwrap_or(0);
    env.lang.syms.resolve(sym).to_string()
}

#[test]
fn a_python_file_rewrites_into_only_canonical_nodes() {
    let repo = tempfile::tempdir().unwrap();
    std::fs::write(repo.path().join("main.py"), MAIN).unwrap();
    let env = python_env(Limits::UNLIMITED);

    let (items, skipped) = rewrite_repo(&env, repo.path());

    assert!(skipped.is_empty());
    let tree = &items[0];
    assert_eq!(tree.label, "main.py");
    assert!(
        tree.source.is_empty(),
        "source text is dropped after rewriting"
    );
    let foreign: Vec<_> = tree
        .root()
        .descendants()
        .filter(|c| !env.lang.in_vocabulary(c.kind()))
        .map(|c| env.lang.kinds.resolve(c.kind() as u32).to_string())
        .collect();
    assert!(foreign.is_empty(), "language nodes survived: {foreign:?}");
}

#[test]
fn definitions_carry_their_type_and_name() {
    let repo = tempfile::tempdir().unwrap();
    std::fs::write(repo.path().join("main.py"), MAIN).unwrap();
    let env = python_env(Limits::UNLIMITED);

    let (items, _) = rewrite_repo(&env, repo.path());

    let tree = &items[0];
    let def_types = ["__class", "__method", "__function"];
    let mut defs: Vec<(String, &str)> = nodes(&env, tree, "__def")
        .map(|d| {
            let name = d
                .children_of(env.lang.lookup_kind("__defname"))
                .next()
                .and_then(|n| n.sym_opt())
                .map(|s| env.lang.syms.resolve(s).to_string())
                .unwrap_or_default();
            let def_type = def_types
                .into_iter()
                .find(|t| d.has(env.lang.lookup_kind(t)))
                .unwrap_or("");
            (name, def_type)
        })
        .collect();
    defs.sort();
    assert_eq!(
        defs,
        [
            ("Greeter".to_string(), "__class"),
            ("greet".to_string(), "__method"),
            ("run".to_string(), "__function"),
        ]
    );
}

#[test]
fn imports_and_calls_become_canonical_nodes() {
    let repo = tempfile::tempdir().unwrap();
    std::fs::write(repo.path().join("main.py"), MAIN).unwrap();
    let env = python_env(Limits::UNLIMITED);

    let (items, _) = rewrite_repo(&env, repo.path());

    let tree = &items[0];
    let mut imports: Vec<String> = nodes(&env, tree, "__import")
        .map(|i| name_of(&env, i))
        .collect();
    imports.sort();
    assert_eq!(imports, ["helper", "os"]);
    assert!(
        nodes(&env, tree, "__call").count() >= 3,
        "Greeter(), g.greet(), os.getcwd()"
    );
}

#[test]
fn a_file_over_the_rewrite_budget_is_skipped_and_reported() {
    let repo = tempfile::tempdir().unwrap();
    std::fs::write(repo.path().join("main.py"), MAIN).unwrap();
    let env = python_env(Limits {
        file_rewrite_ms: 0,
        ..Limits::UNLIMITED
    });

    let (items, skipped) = rewrite_repo(&env, repo.path());

    assert_eq!(items[0].root().descendants().count(), 0);
    assert_eq!(skipped.len(), 1);
    assert_eq!(
        (skipped[0].label.as_ref(), skipped[0].path.as_str()),
        ("rewrite", "main.py")
    );
}
