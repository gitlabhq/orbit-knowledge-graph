use tree_dsl::grammar::{self, SupportLang};
use tree_dsl::lang::Lang;
use tree_dsl::run::Pipeline;

fn main() {
    let lang_id = match std::env::args().nth(2).as_deref() {
        Some("ts") => SupportLang::TypeScript,
        Some("rs" | "rust") => SupportLang::Rust,
        Some("py") => SupportLang::Python,
        Some("js") => SupportLang::JavaScript,
        _ => SupportLang::TypeScript,
    };

    // Third arg: "cst" (default), "rewrite", or "ssa"
    let stage = std::env::args().nth(3).unwrap_or_default();

    let source = std::env::args()
        .nth(1)
        .map(|f| std::fs::read_to_string(&f).unwrap())
        .unwrap_or_else(|| {
            use std::io::Read;
            let mut buf = String::new();
            std::io::stdin().read_to_string(&mut buf).unwrap();
            buf
        });

    if stage == "rewrite" || stage == "ssa" {
        let (pipeline, mut lang) = Pipeline::for_lang(lang_id);
        let mut tree = grammar::parse(&source, lang_id, &mut lang, "test");
        for (si, rules) in pipeline.rewrite_stages.iter().enumerate() {
            let before = tree.nodes.len();
            tree_dsl::pattern::apply_rewrites(&mut tree, &mut lang, rules);
            let after = tree.nodes.len();
            if before != after {
                eprintln!("--- stage {si}: {before} → {after} nodes ---");
            }
        }
        if stage == "ssa" {
            let tree = tree_dsl::run::process_file("test", &source, &mut lang, &pipeline);
            dump(&tree, &lang);
        } else {
            dump(&tree, &lang);
        }
    } else {
        let mut lang = Lang::new();
        let tree = grammar::parse(&source, lang_id, &mut lang, "test");
        dump(&tree, &lang);
    }
}

fn dump(tree: &tree_dsl::tree::Tree, lang: &Lang) {
    for (_i, n) in tree.nodes.iter().enumerate() {
        if n.dead {
            continue;
        }
        let mut depth = 0;
        let mut p = n.parent;
        while p != tree_dsl::tree::NONE {
            depth += 1;
            p = tree.nodes[p as usize].parent;
        }
        let kind = lang.kind_name(n.kind);
        let synth = if n.synth { "__" } else { "" };
        let field = if n.field != 0 {
            format!("{}:", lang.field_name(n.field))
        } else {
            String::new()
        };
        let sym = if n.sym != 0 {
            let s = lang.syms.resolve(n.sym);
            if s.len() > 50 {
                format!(" {:?}...", &s[..50])
            } else {
                format!(" {:?}", s)
            }
        } else {
            String::new()
        };
        println!(
            "{:indent$}{field}{synth}{kind}{sym}",
            "",
            indent = depth * 2
        );
    }
}
