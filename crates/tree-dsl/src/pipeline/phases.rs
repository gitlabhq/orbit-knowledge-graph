use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::file_tree::ProjectTree;
use crate::pattern::EdgeCtx;
use crate::tree::{Edge, Tree};
use crate::treesitter::SupportLang;
use crate::{linker, pattern, treesitter};

use super::types::{Env, State};

pub fn process_file(env: &Env, path: &str, source: &str) -> (Tree, Vec<Edge>) {
    let mut tree = treesitter::parse(source, env.lang_id, &env.lang, path);
    for stage in &env.rewrite_stages {
        pattern::apply_rewrites(&mut tree, &env.lang, stage);
    }
    tree.prune();
    tree.compact();
    let edges = linker::link(&tree, &env.lang, &env.config.link);
    (tree, edges)
}

pub fn parse(env: &Env, state: &mut State, files: Vec<(String, String)>) {
    let results: Vec<(Tree, Vec<Edge>)> = files
        .par_iter()
        .filter(|(p, _)| SupportLang::from_path(p).is_some())
        .map(|(path, content)| process_file(env, path, content))
        .collect();
    let base_fi = state.trees.len();
    for (i, (tree, intra)) in results.into_iter().enumerate() {
        let fi = (base_fi + i) as u32;
        state.trees.push(tree);
        state.edges.extend(intra.into_iter().map(|mut e| {
            e.from_tree = fi;
            e.to_tree = fi;
            e
        }));
    }
}

pub fn resolve(
    env: &Env,
    state: &mut State,
    dirty_fis: FxHashSet<usize>,
    files: Option<&[(String, String)]>,
) {
    let paths: Vec<&str> = state.trees.iter().map(|t| t.label.as_str()).collect();
    let walk = ProjectTree::build(
        &env.lang,
        &env.config.resolve,
        &env.resolve_stages,
        &paths,
        files,
    );
    let result = state.resolver.resolve(
        &state.trees,
        &state.edges,
        &env.lang,
        &dirty_fis,
        env.lang_id,
        &walk.prefixes,
        &env.config.resolve.external,
        &walk.aliases,
    );
    for rsp in &result.resolved_source_paths {
        let nid = state.trees[rsp.fi].to_id(rsp.node);
        state.trees[rsp.fi].node_mut(nid).sym = rsp.sym;
    }
    state.edges.extend(result.cross_edges);
}

pub fn display(env: &Env, state: &mut State) {
    let yaml = treesitter::lang_yaml(env.lang_id).expect("no lang yaml");
    let config = crate::rules::load_lang_full(yaml, &env.lang);
    for (fi, tree) in state.trees.iter_mut().enumerate() {
        let ctx = EdgeCtx {
            tree_index: fi as u32,
            edges: &state.edges,
        };
        pattern::apply_rewrites_with_edges(tree, &env.lang, &config.display_rules, true, &ctx);
    }
}

pub fn remap(
    state: &mut State,
    old_labels: &[String],
    dirty: &FxHashSet<&str>,
) -> FxHashSet<usize> {
    state.trees.retain(|t| !dirty.contains(t.label.as_str()));

    let label_to_fi: FxHashMap<&str, u32> = state
        .trees
        .iter()
        .enumerate()
        .map(|(i, t)| (t.label.as_str(), i as u32))
        .collect();

    state.edges = state
        .edges
        .iter()
        .filter(|e| {
            old_labels
                .get(e.from_fi())
                .is_some_and(|l| !dirty.contains(l.as_str()))
                && old_labels
                    .get(e.to_fi())
                    .is_some_and(|l| !dirty.contains(l.as_str()))
        })
        .map(|e| Edge {
            from_tree: label_to_fi[old_labels[e.from_fi()].as_str()],
            to_tree: label_to_fi[old_labels[e.to_fi()].as_str()],
            ..*e
        })
        .collect();

    let old_dirty_fis: FxHashSet<usize> = old_labels
        .iter()
        .enumerate()
        .filter(|(_, l)| dirty.contains(l.as_str()))
        .map(|(i, _)| i)
        .collect();
    let reverse_dirty: FxHashSet<usize> = state
        .resolver
        .reqs()
        .iter()
        .filter(|r| old_dirty_fis.contains(&r.target_fi))
        .filter_map(|r| label_to_fi.get(old_labels[r.fi].as_str()))
        .map(|&fi| fi as usize)
        .collect();

    state.resolver.remap(old_labels, &label_to_fi);
    reverse_dirty
}
