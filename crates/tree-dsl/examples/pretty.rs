//! Pretty-print a tree at each pipeline stage.
//!
//! Usage: cargo run --example pretty -- <file> <lang> [stage]
//!
//! Stages: cst, rewrite, ssa (default: ssa)
//! Languages: py, ts, js, rs

use std::io::IsTerminal;

use tree_dsl::Pipeline;
use tree_dsl::grammar::{self, SupportLang};
use tree_dsl::lang::Lang;
use tree_dsl::tree::pretty_print;

fn main() {
    let lang_id = match std::env::args().nth(2).as_deref() {
        Some("py") => SupportLang::Python,
        Some("ts") => SupportLang::TypeScript,
        Some("js") => SupportLang::JavaScript,
        Some("rs" | "rust") => SupportLang::Rust,
        Some(other) => {
            eprintln!("unknown lang: {other}");
            std::process::exit(1);
        }
        None => {
            eprintln!("usage: pretty <file> <lang> [cst|rewrite|ssa]");
            std::process::exit(1);
        }
    };

    let stage = std::env::args().nth(3).unwrap_or_else(|| "ssa".into());

    let source = std::env::args()
        .nth(1)
        .map(|f| std::fs::read_to_string(&f).unwrap())
        .unwrap_or_else(|| {
            use std::io::Read;
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf).unwrap();
            buf
        });

    let color = std::io::stdout().is_terminal();

    match stage.as_str() {
        "cst" => {
            let mut lang = Lang::new();
            let tree = grammar::parse(&source, lang_id, &mut lang, "test");
            println!("{}", pretty_print(&tree, &lang, color));
        }
        "rewrite" => {
            let (pipeline, mut lang) = Pipeline::for_lang(lang_id);
            let mut tree = grammar::parse(&source, lang_id, &mut lang, "test");
            for rules in &pipeline.rewrite_stages {
                tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, rules);
            }
            println!("{}", pretty_print(&tree, &lang, color));
        }
        "ssa" => {
            let (pipeline, mut lang) = Pipeline::for_lang(lang_id);
            let tree = tree_dsl::pipeline::process_file("test", &source, &mut lang, &pipeline);
            println!("{}", pretty_print(&tree, &lang, color));
            if !tree.edges.is_empty() {
                println!("edges:");
                for e in &tree.edges {
                    let from = lang.syms.resolve(tree.nodes[e.from.node as usize].sym);
                    let to = lang.syms.resolve(tree.nodes[e.to.node as usize].sym);
                    println!(
                        "  {} --[{}]--> {}",
                        truncate(from, 40),
                        e.kind,
                        truncate(to, 40)
                    );
                }
            }
        }
        other => {
            eprintln!("unknown stage: {other} (use cst, rewrite, or ssa)");
            std::process::exit(1);
        }
    }
}

fn truncate(s: &str, max: usize) -> &str {
    if s.len() > max { &s[..max] } else { s }
}
