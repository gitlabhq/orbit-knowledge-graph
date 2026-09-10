use tree_dsl::grammar::{self, SupportLang};
use tree_dsl::lang::{Lang, SYNTH};
use tree_dsl::pattern::apply_rewrites;
use tree_dsl::run::Pipeline;

fn main() {
    let source = "from os.path import join\nfrom ..models import User\nfrom . import utils\nfrom ...deep import thing\nimport json\ndef foo(): pass\n";

    let lang_id = SupportLang::Python;
    let (pipeline, mut lang) = Pipeline::for_lang(lang_id);
    let mut tree = grammar::parse(source, lang_id, &mut lang, "pkg/sub/file.py");

    for stage in &pipeline.rewrite_stages {
        apply_rewrites(&mut tree, &mut lang, stage);
    }

    for i in 0..tree.nodes.len() {
        let n = &tree.nodes[i];
        let raw_kind = n.kind;
        let kind_id = (raw_kind & !SYNTH) as u32;
        let kind_name = lang.kinds.resolve(kind_id);
        let synth = if raw_kind & SYNTH != 0 { "S:" } else { "" };
        let sym = if n.sym != 0 { lang.syms.resolve(n.sym).to_string() } else { String::new() };
        let short = if sym.len() > 50 { &sym[..50] } else { &sym };
        println!("{i:3}  p={:3}  {synth}{kind_name} \"{short}\"", n.parent);
    }
}
