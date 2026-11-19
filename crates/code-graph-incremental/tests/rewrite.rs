use std::path::Path;

use code_graph_incremental::canonical::{Canonical as C, def_type_of, is_canonical};
use code_graph_incremental::pipeline::{Canonical, Canonicalize, Each, Parse, Prepare, Rewrite};
use code_graph_incremental::tree::Compact;
type Tree = code_graph_incremental::tree::Tree<Compact>;
type Cursor<'a> = code_graph_incremental::tree::Cursor<'a, Compact>;
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, ItemPhase, Killed, Limits, Pipeline};
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

#[test]
fn standard_library_configuration_rejects_invalid_entries_and_legacy_fields() {
    let lang = code_graph_incremental::intern::Lang::new();
    for entry in [
        "{}",
        "json",
        "{module: ''}",
        "{module: json, symbols: ['']}",
        "{symbols: [print]}",
        "{symbols: [print], availability: imported}",
        "{symbols: [print], availability: implicit, precedence: runtime}",
        "{module: json, availability: implicit}",
        "{module: json, availability: imported}",
        "{module: json, availability: sometimes}",
        "{module: json, precedence: unknown}",
        "{module: json, implicit: true}",
        "{module: json, typo: true}",
    ] {
        let yaml = format!("config:\n  stdlib: [{entry}]\nstages: []\n");
        assert!(
            code_graph_incremental::rules::load_lang(&yaml, &lang).is_err(),
            "accepted {entry}"
        );
    }
    for legacy in [
        "link: {builtins: [print]}",
        "resolve: {external: [sys]}",
        "resolve: {stdlib: [json]}",
    ] {
        let yaml = format!("config:\n  {legacy}\nstages: []\n");
        assert!(
            code_graph_incremental::rules::load_lang(&yaml, &lang).is_err(),
            "accepted {legacy}"
        );
    }
}

fn rewrite_repo(env: &Env, root: &Path) -> (Vec<Canonical>, Vec<Killed>) {
    let sources = common::walk(root);
    let (context, workset) = Pipeline::new(Context::new(env, sources), ())
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
    let Canonical(tree, _) = &items[0];
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

    let Canonical(tree, _) = &items[0];
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

    let Canonical(tree, _) = &items[0];
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
        (skipped[0].label.as_ref(), skipped[0].path.as_str()),
        ("rewrite", "main.py")
    );
}

const GROUP_FUNCTIONS: &str = r#"
stages:
  - repeat: REPEAT
    rules:
      - match: '(module $$$PRE (function_definition $$$H) $$$BODY $G:function_definition $$$POST)'
        replace: '(module @$ROOT $$$PRE (__group (function_definition $$$H) $$$BODY) $G $$$POST)'
"#;

fn groups_after_grouping(repeat: bool) -> usize {
    let env = python_env(Limits::UNLIMITED);
    let source =
        "def a(): pass\nx = 1\ndef b(): pass\ny = 2\ndef c(): pass\nz = 3\ndef d(): pass\n";
    let run = code_graph_incremental::Sentinel::new("test", "main.py", u64::MAX);
    let (mut tree, _) = code_graph_incremental::treesitter::parse_with_sentinel(
        source,
        SupportLang::Python,
        &env.lang,
        "main.py",
        &run,
        usize::MAX,
    )
    .unwrap();
    let yaml = GROUP_FUNCTIONS.replace("REPEAT", &repeat.to_string());
    let stages = code_graph_incremental::rules::load_rules(&yaml, &env.lang).unwrap();
    for stage in &stages {
        code_graph_incremental::pattern::apply_stage(&mut tree, &env.lang, stage, &[]).unwrap();
    }
    tree.root()
        .children()
        .filter(|c| env.lang.kinds.resolve(c.kind() as u32) == "__group")
        .count()
}

#[test]
fn a_repeat_stage_runs_until_no_node_is_replaced() {
    assert_eq!(groups_after_grouping(false), 1);
    assert_eq!(groups_after_grouping(true), 3);
}

fn gitlab_ci_services(count: usize, limits: Limits) -> (Vec<Canonical>, Vec<Killed>, Env) {
    let repo = tempfile::tempdir().unwrap();
    let mut content = String::from("job:\n  services:\n");
    for index in 0..count {
        content.push_str(&format!("    - svc{index}:1\n"));
    }
    std::fs::write(repo.path().join(".gitlab-ci.yml"), content).unwrap();
    let env = Env::with_limits(SupportLang::Yaml, limits).unwrap();
    let (items, skipped) = rewrite_repo(&env, repo.path());
    (items, skipped, env)
}

#[test]
fn a_gitlab_ci_file_keeps_only_the_first_images_up_to_the_cap() {
    let (items, skipped, env) = gitlab_ci_services(1_100, Limits::UNLIMITED);

    assert!(skipped.is_empty());
    let Canonical(tree, _) = &items[0];
    let mut images: Vec<_> = nodes(tree, C::Import).map(|i| name_of(&env, i)).collect();
    assert_eq!(images.len(), 1_000);
    images.sort();
    assert!(images.contains(&"svc0".to_string()));
    assert!(!images.contains(&"svc1099".to_string()));
}

#[test]
fn a_gitlab_ci_file_over_the_rewrite_budget_is_skipped() {
    let (items, skipped, _) = gitlab_ci_services(
        200,
        Limits {
            file_rewrite_ms: 0,
            ..Limits::UNLIMITED
        },
    );

    assert!(items.is_empty());
    assert_eq!(skipped.len(), 1);
}
