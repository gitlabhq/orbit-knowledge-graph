use std::path::Path;

use code_graph_incremental::canonical::{Canonical as C, def_type_of, is_canonical};
use code_graph_incremental::pipeline::{
    Canonical, Canonicalize, Each, Parse, Prepare, Rewrite, Sources,
};
use code_graph_incremental::tree::{Cursor, Tree};
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, ItemPhase, Killed, Limits, Pipeline, inventory};

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

fn rewrite_repo(env: &Env, root: &Path) -> (Vec<Canonical>, Vec<Killed>) {
    let entries = inventory::walk(root).unwrap().into_inner();
    let sources = Sources {
        root: root.to_path_buf(),
        entries,
    };
    let (context, workset) = Pipeline::new(Context::new(env), sources)
        .then(Prepare)
        .unwrap()
        .then(Each(Parse.pipe(Rewrite).pipe(Canonicalize)))
        .unwrap()
        .finish();
    (workset.items, context.report.skipped)
}

fn python_env(limits: Limits) -> Env {
    Env::with_limits(SupportLang::Python, limits).unwrap()
}

fn nodes<'a>(tree: &'a Tree, kind: C) -> impl Iterator<Item = Cursor<'a>> {
    tree.root().descendants().filter(move |c| c.is(kind))
}

fn name_of(env: &Env, node: Cursor) -> String {
    let sym = node.child_sym_of_kind(C::Name as u16).unwrap_or(0);
    env.lang.syms.resolve(sym).to_string()
}

#[test]
fn a_python_file_rewrites_into_only_canonical_nodes() {
    let repo = tempfile::tempdir().unwrap();
    std::fs::write(repo.path().join("main.py"), MAIN).unwrap();
    let env = python_env(Limits::UNLIMITED);

    let (items, skipped) = rewrite_repo(&env, repo.path());

    assert!(skipped.is_empty());
    let Canonical(tree) = &items[0];
    assert_eq!(tree.label, "main.py");
    assert!(
        tree.source.is_empty(),
        "source text is dropped after rewriting"
    );
    let foreign: Vec<_> = tree
        .root()
        .descendants()
        .filter(|c| !is_canonical(c.kind()))
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

    let Canonical(tree) = &items[0];
    let mut defs: Vec<(String, &str)> = nodes(tree, C::Def)
        .map(|d| {
            let name = d
                .children_of(C::DefName)
                .next()
                .and_then(|n| n.sym_opt())
                .map(|s| env.lang.syms.resolve(s).to_string())
                .unwrap_or_default();
            (name, def_type_of(d).map_or("", |t| t.display_name()))
        })
        .collect();
    defs.sort();
    assert_eq!(
        defs,
        [
            ("Greeter".to_string(), "Class"),
            ("greet".to_string(), "Method"),
            ("run".to_string(), "Function"),
        ]
    );
}

#[test]
fn imports_and_calls_become_canonical_nodes() {
    let repo = tempfile::tempdir().unwrap();
    std::fs::write(repo.path().join("main.py"), MAIN).unwrap();
    let env = python_env(Limits::UNLIMITED);

    let (items, _) = rewrite_repo(&env, repo.path());

    let Canonical(tree) = &items[0];
    let mut imports: Vec<String> = nodes(tree, C::Import).map(|i| name_of(&env, i)).collect();
    imports.sort();
    assert_eq!(imports, ["helper", "os"]);
    assert!(
        nodes(tree, C::Call).count() >= 3,
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

    assert!(items.is_empty());
    assert_eq!(skipped.len(), 1);
    assert_eq!(
        (skipped[0].label, skipped[0].path.as_str()),
        ("rewrite", "main.py")
    );
}
