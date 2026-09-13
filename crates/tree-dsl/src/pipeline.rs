//! Pipeline orchestration for tree-dsl.
//!
//! Today: sequential `process_file` + `index` flow.
//!
//! Future: compile-time phase markers for observability and pre-emption.
//!
//! ```text
//! trait PhaseDef { const NAME: &'static str; }
//! struct Parse;  impl PhaseDef for Parse  { const NAME: &str = "parse"; }
//! struct Rewrite; struct Classify; struct Link; struct Prune; struct Resolve;
//!
//! fn phase<P: PhaseDef, T>(ctx: &PipelineCtx, f: impl FnOnce() -> T) -> Result<T, Cancelled>
//! ```
//!
//! `PipelineCtx` carries a cancel token, metrics collector, and mailbox handle.
//! Each `PhaseDef` is a ZST — zero cost, fully monomorphized.
//! `phase()` checks cancellation, opens a tracing span, records duration.
//! Threading model: per-file processing fans out; resolve phase joins.

use crate::grammar::{self, SupportLang};
use crate::lang::Lang;
use crate::tree::Tree;
use crate::{canonical, file_tree, linker, pattern, resolver};

pub struct IndexResult {
    pub trees: Vec<Tree>,
    pub cross_edges: Vec<crate::tree::Edge>,
    pub lang: Lang,
    pub pipeline: Pipeline,
}

pub struct Pipeline {
    pub lang_id: SupportLang,
    pub rewrite_stages: Vec<Vec<crate::pattern::Rewrite>>,
    pub resolve: crate::file_tree::ResolveConfig,
}

impl Pipeline {
    pub fn for_lang(lang_id: SupportLang) -> (Pipeline, Lang) {
        let mut lang = Lang::new();
        let (rewrite_stages, resolve) = match grammar::lang_yaml(lang_id) {
            Some(yaml) => crate::rules::load_lang(yaml, &mut lang),
            None => (vec![], crate::file_tree::ResolveConfig::default()),
        };
        (
            Pipeline {
                lang_id,
                rewrite_stages,
                resolve,
            },
            lang,
        )
    }
}

pub fn process_file(path: &str, source: &str, lang: &mut Lang, pipeline: &Pipeline) -> Tree {
    let mut tree = grammar::parse(source, pipeline.lang_id, lang, path);
    for stage in &pipeline.rewrite_stages {
        pattern::apply_rewrites(&mut tree, lang, stage);
    }
    tree.compact();
    canonical::classify_methods(&mut tree, lang);
    linker::link(&tree, lang);
    tree.prune();
    tree.compact();
    tree
}

pub fn process_file_timed(
    path: &str,
    source: &str,
    lang: &mut Lang,
    pipeline: &Pipeline,
) -> (Tree, [std::time::Duration; 4]) {
    use std::time::Instant;
    let t0 = Instant::now();
    let mut tree = grammar::parse(source, pipeline.lang_id, lang, path);
    let t1 = Instant::now();
    for stage in &pipeline.rewrite_stages {
        pattern::apply_rewrites(&mut tree, lang, stage);
    }
    tree.compact();
    let t2 = Instant::now();
    canonical::classify_methods(&mut tree, lang);
    linker::link(&tree, lang);
    let t3 = Instant::now();
    tree.prune();
    tree.compact();
    let t4 = Instant::now();
    (tree, [t1 - t0, t2 - t1, t3 - t2, t4 - t3])
}

/// Unified indexing entrypoint. All files must be the same language.
pub fn index(lang_id: SupportLang, files: &[(String, String)]) -> IndexResult {
    let (pipeline, mut lang) = Pipeline::for_lang(lang_id);
    let mut trees: Vec<Tree> = Vec::new();
    let mut phase_totals = [std::time::Duration::ZERO; 4];
    for (path, content) in files {
        if SupportLang::from_path(path).is_some() {
            let (tree, times) = process_file_timed(path, content, &mut lang, &pipeline);
            trees.push(tree);
            for (i, t) in times.iter().enumerate() {
                phase_totals[i] += *t;
            }
        }
    }
    eprintln!(
        "[phases] parse={:.2}s rewrite={:.2}s link={:.2}s prune={:.2}s",
        phase_totals[0].as_secs_f64(),
        phase_totals[1].as_secs_f64(),
        phase_totals[2].as_secs_f64(),
        phase_totals[3].as_secs_f64(),
    );
    let file_paths: Vec<String> = files.iter().map(|(p, _)| p.clone()).collect();
    let walk = file_tree::walk(&file_paths, &mut lang, &pipeline.resolve);
    let cross_edges = resolver::resolve(
        &mut trees,
        &mut lang,
        lang_id,
        &walk.lookup_prefixes,
        &pipeline.resolve.external,
    )
    .cross_edges;
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
    let tree = process_file(path, source, &mut lang, &pipeline);
    (tree, lang, pipeline)
}
