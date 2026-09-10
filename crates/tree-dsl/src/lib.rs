pub mod file_tree;
pub mod grammar;
pub mod lang;
#[path = "../langs/mod.rs"]
pub mod langs;
pub mod pattern;
pub mod resolver;
pub mod rules;
pub mod run;
pub mod ssa;
pub mod tree;

use grammar::SupportLang;
use lang::Lang;
use run::Pipeline;
use tree::Tree;

pub struct IndexResult {
    pub trees: Vec<Tree>,
    pub cross_edges: Vec<resolver::CrossEdge>,
    pub lang: Lang,
    pub pipeline: Pipeline,
}

/// Unified indexing entrypoint. All files must be the same language.
pub fn index(lang_id: SupportLang, files: &[(String, String)]) -> IndexResult {
    let (pipeline, mut lang) = Pipeline::for_lang(lang_id);
    let mut trees: Vec<Tree> = Vec::new();
    for (path, content) in files {
        if SupportLang::from_path(path).is_some() {
            trees.push(run::process_file(path, content, &mut lang, &pipeline));
        }
    }
    let file_paths: Vec<String> = files.iter().map(|(p, _)| p.clone()).collect();
    let walk = file_tree::walk(&file_paths, &mut lang, &pipeline.resolve);
    let cross_edges =
        resolver::resolve(&mut trees, &mut lang, lang_id, &walk.source_roots).cross_edges;
    if !cross_edges.is_empty() {
        eprintln!("[resolver] {} cross-edges", cross_edges.len());
    }
    IndexResult {
        trees,
        cross_edges,
        lang,
        pipeline,
    }
}

/// Parse a single file through rewrites + SSA, no resolver.
pub fn parse(lang_id: SupportLang, path: &str, source: &str) -> (Tree, Lang, Pipeline) {
    let (pipeline, mut lang) = Pipeline::for_lang(lang_id);
    let tree = run::process_file(path, source, &mut lang, &pipeline);
    (tree, lang, pipeline)
}
