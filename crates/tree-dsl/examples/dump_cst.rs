use tree_dsl::Pipeline;
use tree_dsl::grammar::{self, SupportLang};
use tree_dsl::lang::Lang;

fn main() {
    let lang_id = match std::env::args().nth(2).as_deref() {
        Some("ts") => SupportLang::TypeScript,
        Some("rs" | "rust") => SupportLang::Rust,
        Some("py") => SupportLang::Python,
        Some("js") => SupportLang::JavaScript,
        _ => SupportLang::TypeScript,
    };

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
        let mut tree = grammar::parse(&source, lang_id, &lang, "test", &pipeline.referenced_kinds);
        let before = tree.len();
        for rules in &pipeline.rewrite_stages {
            tree_dsl::pattern::apply_rewrites(&mut tree, &lang, rules);
        }
        let after = tree.len();
        if before != after {
            eprintln!("--- rewrites: {before} -> {after} nodes ---");
        }
        if stage == "ssa" {
            let tree = tree_dsl::pipeline::process_file("test", &source, &lang, &pipeline);
            dump(&tree, &lang);
            for e in tree.edges().iter() {
                let from_s = lang.syms.resolve(tree.cursor(e.from.node).sym());
                let to_s = lang.syms.resolve(tree.cursor(e.to.node).sym());
                let from = if from_s.len() > 30 {
                    &from_s[..30]
                } else {
                    from_s
                };
                let to = if to_s.len() > 30 { &to_s[..30] } else { to_s };
                eprintln!("  edge: {from} --[{}]--> {to}", e.kind);
            }
        } else {
            dump(&tree, &lang);
        }
    } else {
        let mut lang = Lang::new();
        let tree = grammar::parse(&source, lang_id, &lang, "test", &pipeline.referenced_kinds);
        dump(&tree, &lang);
    }
}

fn dump(tree: &tree_dsl::tree::Tree, lang: &Lang) {
    for c in std::iter::once(tree.root()).chain(tree.root().descendants()) {
        let depth = c.ancestors().count();
        let kind = lang.kind_name(c.kind());
        let synth = if c.is_synth() { "__" } else { "" };
        let field = if c.field() != 0 {
            format!("{}:", lang.field_name(c.field()))
        } else {
            String::new()
        };
        let sym = if c.sym() != 0 {
            let s = lang.syms.resolve(c.sym());
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
