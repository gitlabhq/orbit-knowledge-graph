use code_graph_incremental::canonical::Canonical as C;
use code_graph_incremental::pipeline::{
    Canonicalize, DirtyGraph, Each, Insert, Link, Parse, Prepare, Rewrite, Sources,
};
use code_graph_incremental::tree::EdgeKind;
use code_graph_incremental::treesitter::SupportLang;
use code_graph_incremental::{Context, Env, ItemPhase, Limits, Pipeline, inventory};

fn calls(language: SupportLang, path: &str, source: &str) -> Vec<String> {
    let repo = tempfile::tempdir().unwrap();
    std::fs::write(repo.path().join(path), source).unwrap();
    let env = Env::with_limits(language, Limits::UNLIMITED).unwrap();
    let sources = Sources {
        root: repo.path().to_path_buf(),
        entries: inventory::walk(repo.path()).unwrap().into_inner(),
    };
    let graph: DirtyGraph = Pipeline::new(Context::new(&env), sources)
        .then(Prepare)
        .unwrap()
        .then(Each(Parse.pipe(Rewrite).pipe(Canonicalize).pipe(Link)))
        .unwrap()
        .then(Insert)
        .unwrap()
        .into_value();
    let mut calls: Vec<_> = graph
        .state
        .edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Calls)
        .filter_map(|e| {
            let from = graph.state.trees[e.from_fi()].cursor(e.from_node);
            let to = graph.state.trees[e.to_fi()].cursor(e.to_node);
            (env.lang.syms.resolve(from.child_sym(C::DefName)?) == "run").then(|| {
                env.lang
                    .syms
                    .resolve(to.child_sym(C::DefName).unwrap())
                    .to_string()
            })
        })
        .collect();
    calls.sort();
    calls.dedup();
    calls
}

#[test]
fn rust_branch_declaration_does_not_escape() {
    assert_eq!(
        calls(
            SupportLang::Rust,
            "main.rs",
            "fn first() {} fn second() {} fn run(flag: bool) { let f: fn() = first; if flag { let f: fn() = second; } f(); }"
        ),
        ["first"]
    );
}

#[test]
fn rust_block_assignment_updates_outer_binding() {
    assert_eq!(
        calls(
            SupportLang::Rust,
            "main.rs",
            "fn first() {} fn second() {} fn run() { let mut f: fn() = first; { f = second; } f(); }"
        ),
        ["second"]
    );
}

#[test]
fn rust_block_import_does_not_escape() {
    assert_eq!(
        calls(
            SupportLang::Rust,
            "main.rs",
            "fn first() {} mod dep { pub fn second() {} } fn run() { let f = first; { use crate::dep::second as f; } f(); }"
        ),
        ["first"]
    );
}

#[test]
fn typescript_let_does_not_escape_block() {
    assert_eq!(
        calls(
            SupportLang::TypeScript,
            "main.ts",
            "function first() {} function second() {} function run() { let f = first; { let f = second; } f(); }"
        ),
        ["first"]
    );
}

#[test]
fn typescript_var_belongs_to_function() {
    assert_eq!(
        calls(
            SupportLang::TypeScript,
            "main.ts",
            "function first() {} function second() {} function run() { var f = first; { var f = second; } f(); }"
        ),
        ["second"]
    );
}

#[test]
fn python_assignment_makes_name_function_local() {
    assert_eq!(
        calls(
            SupportLang::Python,
            "main.py",
            "def first(): pass\ndef second(): pass\ndef run():\n    first()\n    first = second\n"
        ),
        Vec::<String>::new()
    );
}

#[test]
fn go_short_declaration_does_not_escape_block() {
    assert_eq!(
        calls(
            SupportLang::Go,
            "main.go",
            "package main\nfunc first() {}\nfunc second() {}\nfunc run() { f := first; { f := second; _ = f }; f() }"
        ),
        ["first"]
    );
}

#[test]
fn python_if_assignment_updates_function_binding() {
    assert_eq!(
        calls(
            SupportLang::Python,
            "main.py",
            "def first(): pass\ndef second(): pass\ndef run(flag):\n    f = first\n    if flag:\n        f = second\n    f()\n"
        ),
        ["first", "second"]
    );
}

#[test]
fn rust_branch_initializer_declares_after_evaluation() {
    assert_eq!(
        calls(
            SupportLang::Rust,
            "main.rs",
            "fn first() {} fn second() {} fn run(flag: bool) { let f: fn() = first; { let f = if flag { second } else { first }; } f(); }"
        ),
        ["first"]
    );
}

#[test]
fn typescript_parameter_blocks_import_fallback() {
    assert_eq!(
        calls(
            SupportLang::TypeScript,
            "main.ts",
            "function first() {} function run(first: () => void) { first(); }"
        ),
        Vec::<String>::new()
    );
}

#[test]
fn typescript_temporal_dead_zone_blocks_outer_lookup() {
    assert_eq!(
        calls(
            SupportLang::TypeScript,
            "main.ts",
            "function first() {} function run() { { first(); let first = () => {}; } }"
        ),
        Vec::<String>::new()
    );
}

#[test]
fn rust_initializer_reads_previous_declaration() {
    assert_eq!(
        calls(
            SupportLang::Rust,
            "main.rs",
            "fn first() {} fn run() { let f = first; let f = f; f(); }"
        ),
        ["first"]
    );
}

#[test]
fn rust_block_value_keeps_its_local_binding() {
    assert_eq!(
        calls(
            SupportLang::Rust,
            "main.rs",
            "fn first() {} fn run() { let f = { let local = first; local }; f(); }"
        ),
        ["first"]
    );
}

#[test]
fn rust_field_copy_reads_previous_declaration() {
    assert_eq!(
        calls(
            SupportLang::Rust,
            "main.rs",
            "struct H { callback: fn() } fn first() {} fn run() { let h = H { callback: first }; let h = h; (h.callback)(); }"
        ),
        ["H", "first"]
    );
}

#[test]
fn typescript_uninitialized_var_does_not_clear_parameter() {
    assert_eq!(
        calls(
            SupportLang::TypeScript,
            "main.ts",
            "function first() {} function second() {} function run() { var f = first; { var f; } f(); }"
        ),
        ["first"]
    );
}
