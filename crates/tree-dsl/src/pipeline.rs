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
use crate::{file_tree, linker, pattern, resolver};

pub struct IndexResult {
    pub trees: Vec<crate::tree::LockedTree>,
    pub cross_edges: Vec<crate::tree::Edge>,
    pub lang: Lang,
    pub pipeline: Pipeline,
    pub timings: IndexTimings,
}

#[derive(Default, Clone, Copy)]
pub struct IndexTimings {
    pub parse_s: f64,
    pub resolve_s: f64,
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
    linker::link(&tree, lang);
    tree.prune();
    tree.compact();
    tree.release_buffers();
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
    linker::link(&tree, lang);
    let t3 = Instant::now();
    tree.prune();
    tree.compact();
    let t4 = Instant::now();
    (tree, [t1 - t0, t2 - t1, t3 - t2, t4 - t3])
}

/// Unified indexing entrypoint. All files must be the same language.
pub fn index(lang_id: SupportLang, files: &[(String, String)]) -> IndexResult {
    use std::time::Instant;

    let (pipeline, mut lang) = Pipeline::for_lang(lang_id);

    let parseable: Vec<&(String, String)> = files
        .iter()
        .filter(|(p, _)| SupportLang::from_path(p).is_some())
        .collect();

    let t0 = Instant::now();

    let chunks: Vec<(Vec<Tree>, Lang)> = {
        use rayon::prelude::*;
        parseable
            .par_iter()
            .fold(
                || (Vec::new(), lang.thread_fork()),
                |(mut trees, mut tl), (path, content)| {
                    let tree = process_file(path, content, &mut tl, &pipeline);
                    trees.push(tree);
                    (trees, tl)
                },
            )
            .collect()
    };

    let mut trees: Vec<Tree> = Vec::with_capacity(parseable.len());
    for (mut chunk_trees, chunk_lang) in chunks {
        let remap = lang.thread_merge(&chunk_lang);
        for tree in &mut chunk_trees {
            tree.remap_syms(&remap);
        }
        trees.extend(chunk_trees);
    }

    let parse_s = t0.elapsed().as_secs_f64();
    let t1 = Instant::now();

    let file_paths: Vec<String> = files.iter().map(|(p, _)| p.clone()).collect();
    let walk = file_tree::walk(&file_paths, files, &mut lang, &pipeline.resolve);
    let cross_edges = resolver::resolve(
        &mut trees,
        &mut lang,
        lang_id,
        &walk.lookup_prefixes,
        &pipeline.resolve.external,
    )
    .cross_edges;

    let resolve_s = t1.elapsed().as_secs_f64();

    let locked: Vec<crate::tree::LockedTree> = trees.into_iter().map(|t| t.into()).collect();

    IndexResult {
        trees: locked,
        cross_edges,
        lang,
        pipeline,
        timings: IndexTimings { parse_s, resolve_s },
    }
}

/// Parse a single file through rewrites + SSA, no resolver.
pub fn parse(lang_id: SupportLang, path: &str, source: &str) -> (Tree, Lang, Pipeline) {
    let (pipeline, mut lang) = Pipeline::for_lang(lang_id);
    let tree = process_file(path, source, &mut lang, &pipeline);
    (tree, lang, pipeline)
}
