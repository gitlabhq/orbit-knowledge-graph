//! Pipeline orchestration for tree-dsl.
//!
//! Sequential `process_file` + `index` flow.
//! Threading model: per-file processing fans out; resolve phase joins.

use std::time::Instant;

use rayon::prelude::*;

use crate::intern::Lang;
use crate::pattern::Rewrite;
use crate::rules::ResolveConfig;
use crate::tree::{Edge, Tree};
use crate::treesitter::{self as treesitter, SupportLang};
use crate::{file_tree, linker, pattern, resolver, rules};

pub struct IndexResult {
    pub trees: Vec<Tree>,
    pub edges: Vec<Edge>,
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
    pub rewrite_stages: Vec<Vec<Rewrite>>,
    pub resolve: ResolveConfig,
}

impl Pipeline {
    pub fn for_lang(lang_id: SupportLang) -> (Pipeline, Lang) {
        let lang = Lang::new();
        let (rewrite_stages, resolve) = match treesitter::lang_yaml(lang_id) {
            Some(yaml) => rules::load_lang(yaml, &lang),
            None => (vec![], ResolveConfig::default()),
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

pub fn process_file(
    path: &str,
    source: &str,
    lang: &Lang,
    pipeline: &Pipeline,
) -> (Tree, Vec<Edge>) {
    let mut tree = treesitter::parse(source, pipeline.lang_id, lang, path);
    for stage in &pipeline.rewrite_stages {
        pattern::apply_rewrites(&mut tree, lang, stage);
    }
    tree.prune();
    tree.compact();
    let edges = linker::link(&tree, lang);
    (tree, edges)
}

pub fn process_file_timed(
    path: &str,
    source: &str,
    lang: &Lang,
    pipeline: &Pipeline,
) -> (Tree, Vec<Edge>, [std::time::Duration; 4]) {
    let t0 = Instant::now();
    let mut tree = treesitter::parse(source, pipeline.lang_id, lang, path);
    let t1 = Instant::now();
    for stage in &pipeline.rewrite_stages {
        pattern::apply_rewrites(&mut tree, lang, stage);
    }
    let t2 = Instant::now();
    tree.prune();
    tree.compact();
    let t3 = Instant::now();
    let edges = linker::link(&tree, lang);
    let t4 = Instant::now();
    (tree, edges, [t1 - t0, t2 - t1, t3 - t2, t4 - t3])
}

/// Unified indexing entrypoint. All files must be the same language.
pub fn index(lang_id: SupportLang, files: &[(String, String)]) -> IndexResult {
    let (pipeline, lang) = Pipeline::for_lang(lang_id);

    let parseable: Vec<&(String, String)> = files
        .iter()
        .filter(|(p, _)| SupportLang::from_path(p).is_some())
        .collect();

    let t0 = Instant::now();

    let results: Vec<(Tree, Vec<Edge>)> = parseable
        .par_iter()
        .map(|(path, content)| process_file(path, content, &lang, &pipeline))
        .collect();
    let (mut trees, intra_edges): (Vec<Tree>, Vec<Vec<Edge>>) = results.into_iter().unzip();
    let mut edges: Vec<Edge> = intra_edges
        .into_iter()
        .enumerate()
        .flat_map(|(fi, file_edges)| {
            file_edges.into_iter().map(move |mut e| {
                e.from_tree = fi as u32;
                e.to_tree = fi as u32;
                e
            })
        })
        .collect();

    let parse_s = t0.elapsed().as_secs_f64();
    let t1 = Instant::now();

    let file_paths: Vec<String> = files.iter().map(|(p, _)| p.clone()).collect();
    let walk = file_tree::walk(&file_paths, files, &lang, &pipeline.resolve);
    let result = resolver::resolve(
        &trees,
        &edges,
        &lang,
        lang_id,
        &walk.lookup_prefixes,
        &pipeline.resolve.external,
    );
    for rsp in &result.resolved_source_paths {
        let nid = trees[rsp.fi].to_id(rsp.node);
        trees[rsp.fi].node_mut(nid).sym = rsp.sym;
    }
    edges.extend(result.cross_edges);

    let resolve_s = t1.elapsed().as_secs_f64();

    IndexResult {
        trees,
        edges,
        lang,
        pipeline,
        timings: IndexTimings { parse_s, resolve_s },
    }
}

pub fn reindex(
    mut base: IndexResult,
    added: &[(String, String)],
    modified: &[(String, String)],
    removed: &[String],
) -> IndexResult {
    let t0 = Instant::now();
    let old_labels: Vec<String> = base.trees.iter().map(|t| t.label.clone()).collect();
    let dirty: rustc_hash::FxHashSet<&str> = removed
        .iter()
        .map(|s| s.as_str())
        .chain(modified.iter().map(|(p, _)| p.as_str()))
        .collect();

    base.trees.retain(|t| !dirty.contains(t.label.as_str()));

    let label_to_fi: rustc_hash::FxHashMap<&str, u32> = base
        .trees
        .iter()
        .enumerate()
        .map(|(i, t)| (t.label.as_str(), i as u32))
        .collect();

    let mut edges: Vec<Edge> = base
        .edges
        .into_iter()
        .filter(|e| {
            old_labels
                .get(e.from_tree as usize)
                .is_some_and(|l| !dirty.contains(l.as_str()))
                && old_labels
                    .get(e.to_tree as usize)
                    .is_some_and(|l| !dirty.contains(l.as_str()))
        })
        .map(|e| {
            Edge::new(
                label_to_fi[old_labels[e.from_tree as usize].as_str()],
                e.from_node,
                label_to_fi[old_labels[e.to_tree as usize].as_str()],
                e.to_node,
                e.kind,
            )
        })
        .collect();

    for (path, source) in modified.iter().chain(added.iter()) {
        let fi = base.trees.len() as u32;
        let (tree, intra) = process_file(path, source, &base.lang, &base.pipeline);
        base.trees.push(tree);
        edges.extend(intra.into_iter().map(|mut e| {
            e.from_tree = fi;
            e.to_tree = fi;
            e
        }));
    }

    let parse_s = t0.elapsed().as_secs_f64();
    let t1 = Instant::now();

    let paths: Vec<String> = base.trees.iter().map(|t| t.label.clone()).collect();
    let dummy: Vec<(String, String)> = paths.iter().map(|p| (p.clone(), String::new())).collect();
    let walk = file_tree::walk(&paths, &dummy, &base.lang, &base.pipeline.resolve);
    let result = resolver::resolve(
        &base.trees,
        &edges,
        &base.lang,
        base.pipeline.lang_id,
        &walk.lookup_prefixes,
        &base.pipeline.resolve.external,
    );
    for rsp in &result.resolved_source_paths {
        let nid = base.trees[rsp.fi].to_id(rsp.node);
        base.trees[rsp.fi].node_mut(nid).sym = rsp.sym;
    }
    edges.extend(result.cross_edges);

    let resolve_s = t1.elapsed().as_secs_f64();

    IndexResult {
        trees: base.trees,
        edges,
        lang: base.lang,
        pipeline: base.pipeline,
        timings: IndexTimings { parse_s, resolve_s },
    }
}

pub fn parse(lang_id: SupportLang, path: &str, source: &str) -> (Tree, Vec<Edge>, Lang, Pipeline) {
    let (pipeline, lang) = Pipeline::for_lang(lang_id);
    let (tree, edges) = process_file(path, source, &lang, &pipeline);
    (tree, edges, lang, pipeline)
}
