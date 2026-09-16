//! Cross-file resolver. Reads canonical __import/__source/__name nodes.
//!
//! Two-wave parallel resolution:
//! Wave 1: per-import-req, produces import edges and direct call edges.
//! Wave 2: per-call-edge, produces type-inferred and field-typed edges.

use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::{self as canonical, Canonical as C};
use crate::intern::Lang;
use crate::tree::{Cursor, Edge, EdgeKind, Tree, find_method_in, infer_return_type};
use crate::treesitter::SupportLang;

use crate::constants::WILDCARD;
use crate::paths;

type VisibleMap = Vec<FxHashMap<u32, (usize, u32)>>;

pub struct ResolveResult {
    pub cross_edges: Vec<Edge>,
}

pub fn resolve(
    trees: &mut [Tree],
    lang: &Lang,
    support_lang: SupportLang,
    lookup_prefixes: &[String],
    external: &[String],
) -> ResolveResult {
    let index_names = support_lang.index_names();
    let labels: Vec<String> = trees.iter().map(|t| t.label.clone()).collect();
    let file_index = paths::build_file_index(&labels, support_lang, index_names);
    let mut visible = build_visible_names(trees);
    let (reqs, mut cross_edges) =
        gather_imports(trees, lang, &file_index, lookup_prefixes, external);

    let wildcard_sym = lang.syms.intern(WILDCARD);
    let ambiguous = propagate_reexports(
        trees,
        &reqs,
        &mut visible,
        support_lang,
        index_names,
        wildcard_sym,
    );

    for req in &reqs {
        let resolved_sym = lang.syms.intern(&req.target_path);
        let sp_idx = trees[req.fi]
            .cursor(req.node)
            .child(C::SourcePath)
            .map(|n| n.index());
        if let Some(sn) = sp_idx {
            let nid = trees[req.fi].to_id(sn);
            trees[req.fi].node_mut(nid).sym = resolved_sym;
        }
    }

    let reverse_visible: FxHashMap<(usize, u32), u32> = visible
        .iter()
        .flat_map(|names| names.iter().map(|(&sym, &(vfi, vn))| ((vfi, vn), sym)))
        .collect();

    let ctx = ResolveCtx {
        trees,
        lang,
        visible: &visible,
        ambiguous: &ambiguous,
        reqs: &reqs,
        file_index: &file_index,
        reverse_visible: &reverse_visible,
        support_lang,
        index_names,
        wildcard_sym: lang.syms.intern(WILDCARD),
    };

    let wave1: Vec<Edge> = reqs
        .par_iter()
        .flat_map(|req| resolve_one_import(&ctx, req))
        .collect();
    cross_edges.extend(&wave1);

    let wave2: Vec<Edge> = wave1
        .par_iter()
        .filter(|e| e.kind == EdgeKind::Calls)
        .flat_map(|ce| resolve_type_edges(&ctx, ce, &cross_edges))
        .collect();
    cross_edges.extend(wave2);

    let wave2b: Vec<Edge> = cross_edges
        .par_iter()
        .filter(|e| e.kind == EdgeKind::Imports)
        .flat_map(|ce| resolve_field_edges(&ctx, ce))
        .collect();
    cross_edges.extend(wave2b);

    ResolveResult { cross_edges }
}

struct ResolveCtx<'a> {
    trees: &'a [Tree],
    lang: &'a Lang,
    visible: &'a VisibleMap,
    ambiguous: &'a FxHashSet<(usize, u32)>,
    reqs: &'a [ImportReq],
    file_index: &'a FxHashMap<String, usize>,
    reverse_visible: &'a FxHashMap<(usize, u32), u32>,
    support_lang: SupportLang,
    index_names: &'a [String],
    wildcard_sym: u32,
}

unsafe impl<'a> Sync for ResolveCtx<'a> {}

fn resolve_one_import(ctx: &ResolveCtx, req: &ImportReq) -> Vec<Edge> {
    let corpus = Cursor::new(ctx.trees, 0, 0);
    let (fi, tfi) = (req.fi, req.target_fi);
    let import = corpus.jump(fi as u32, req.node);
    let mut edges = Vec::new();

    for c in import.names() {
        let ns = c.sym();
        if ns == ctx.wildcard_sym {
            if c.child_sym(C::Alias).is_some() {
                edges.push(c.edge_to(c.jump(tfi as u32, 0), EdgeKind::Imports));
            } else {
                for (&dn, &(rfi, rn)) in &ctx.visible[tfi] {
                    if !ctx.ambiguous.contains(&(tfi, dn)) {
                        edges.push(c.edge_to(c.jump(rfi as u32, rn), EdgeKind::Imports));
                    }
                }
            }
            continue;
        }
        if ctx.ambiguous.contains(&(tfi, ns)) {
            continue;
        }
        if let Some(&(rfi, rn)) = ctx.visible[tfi].get(&ns) {
            edges.push(c.edge_to(c.jump(rfi as u32, rn), EdgeKind::Imports));
        } else {
            let results = follow_import_chain(corpus, ctx.reqs, ctx.visible, ns, tfi);
            if results.len() == 1 {
                edges.push(c.edge_to(c.jump(results[0].0 as u32, results[0].1), EdgeKind::Imports));
            } else {
                let target_path = ctx.lang.syms.resolve(corpus.jump(tfi as u32, 0).sym());
                let name_resolved = ctx.lang.syms.resolve(ns);
                if let Some(sub_fi) = paths::resolve_submodule(
                    target_path,
                    name_resolved,
                    ctx.support_lang,
                    ctx.index_names,
                    ctx.file_index,
                ) {
                    edges.push(c.edge_to(c.jump(sub_fi as u32, 0), EdgeKind::Imports));
                }
            }
        }
    }

    let import_edges: Vec<Edge> = edges
        .iter()
        .filter(|e| e.kind == EdgeKind::Imports)
        .copied()
        .collect();
    let import_node = req.node;
    let nodes = &ctx.trees[fi];
    let mut target_files = vec![req.target_fi];
    for ie in &import_edges {
        let tfi = ie.to.tree as usize;
        if !target_files.contains(&tfi) {
            target_files.push(tfi);
        }
    }

    for edge in nodes.edges().iter() {
        if edge.kind != EdgeKind::Imports {
            continue;
        }
        let et = edge.to.node;
        if et != import_node
            && nodes
                .cursor(et)
                .parent()
                .is_none_or(|p| p.index() != import_node)
        {
            continue;
        }
        let caller = corpus.jump(fi as u32, edge.from.node);
        for d in caller.descendants().filter(|d| d.is(C::Call)) {
            if let Some(mn) = d.child(C::Callee).and_then(|cn| cn.child(C::Member)) {
                let ms = mn.sym();
                if ms != 0 {
                    for &tfi in &target_files {
                        if let Some(&(dfi, dn)) = ctx.visible[tfi].get(&ms) {
                            let tgt = caller.jump(dfi as u32, dn);
                            if is_callable(tgt) {
                                edges.push(caller.edge_to(tgt, EdgeKind::Calls));
                            }
                            break;
                        }
                    }
                }
            }
        }
    }

    for ie in import_edges {
        let target_name = ctx
            .reverse_visible
            .get(&(ie.to.tree as usize, ie.to.node))
            .copied()
            .unwrap_or(0);
        let ft = &ctx.trees[ie.from.tree as usize];
        let import_parent = ft.cursor(ie.from.node).parent().map(|p| p.index());
        for intra in ft.edges().iter() {
            if intra.kind != EdgeKind::Imports {
                continue;
            }
            let ei = intra.to.node;
            let direct = ei == ie.from.node
                || ft.cursor(ei).parent().map(|p| p.index()) == Some(ie.from.node)
                || import_parent == Some(ei);
            if !direct {
                continue;
            }
            if ft.cursor(ie.from.node).sym() == ctx.wildcard_sym && target_name != 0 {
                let found = corpus
                    .jump(ie.from.tree, intra.from.node)
                    .descendants()
                    .any(|d| d.is(C::Call) && d.child_sym(C::Callee) == Some(target_name));
                if !found {
                    continue;
                }
            }
            let target = corpus.jump(ie.to.tree, ie.to.node);
            if !is_callable(target) {
                continue;
            }
            let from = corpus.jump(ie.from.tree, intra.from.node);
            edges.push(from.edge_to(target, EdgeKind::Calls));
        }
    }

    edges
}

fn resolve_type_edges(ctx: &ResolveCtx, ce: &Edge, all_cross: &[Edge]) -> Vec<Edge> {
    let corpus = Cursor::new(ctx.trees, 0, 0);
    let target = corpus.follow(ce);
    let caller = corpus.jump(ce.from.tree, ce.from.node);
    let mut edges = Vec::new();

    let Some(ret_sym) = infer_return_type(target) else {
        return edges;
    };
    let Some((type_fi, type_node)) =
        resolve_type(ret_sym, ce.to.tree as usize, corpus, ctx.visible, all_cross)
    else {
        return edges;
    };

    let target_name = target.child_sym(C::DefName).unwrap_or(0);
    let bound: Vec<u32> = caller
        .descendants()
        .filter(|d| d.is(C::Binding) && d.sym() != 0)
        .filter_map(|d| {
            let callee = d.child(C::Rhs)?.child(C::Call)?.child_sym(C::Callee)?;
            (callee == target_name).then_some(d.sym())
        })
        .collect();

    let class = caller.jump(type_fi as u32, type_node);
    for d in caller.descendants().filter(|d| d.is(C::Call)) {
        if let Some(mn) = d.child(C::Callee).and_then(|cn| cn.child(C::Member)) {
            let obj = mn.child_sym(C::Object).unwrap_or(0);
            if !bound.contains(&obj) {
                continue;
            }
            let method = mn.sym();
            if method != 0
                && let Some(m) = find_method_in(class, method)
            {
                edges.push(caller.edge_to(m, EdgeKind::Calls));
            }
        }
    }
    edges
}

fn resolve_field_edges(ctx: &ResolveCtx, ce: &Edge) -> Vec<Edge> {
    let corpus = Cursor::new(ctx.trees, 0, 0);
    let target = corpus.follow(ce);
    let target_dt = canonical::def_type_of(target);
    if !target_dt.is_some_and(|k| matches!(k, C::Class | C::Struct)) {
        return vec![];
    }
    let target_name = target.child_sym(C::DefName).unwrap_or(0);
    if target_name == 0 {
        return vec![];
    }
    let ft = &ctx.trees[ce.from.tree as usize];
    let mut edges = Vec::new();
    for nr in ft.root().descendants() {
        if !nr.is(C::Binding) {
            continue;
        }
        let Some(ivar) = nr.child(C::Ivar) else {
            continue;
        };
        let callee_sym = nr
            .child(C::Rhs)
            .and_then(|r| r.child(C::Call))
            .and_then(|c| c.child_sym(C::Callee));
        if callee_sym != Some(target_name) {
            continue;
        }
        let ivar_sym = ivar.sym();
        if ivar_sym == 0 {
            continue;
        }
        let class = nr.enclosing(|a| {
            canonical::def_type_of(a)
                .is_some_and(|k| matches!(k, C::Class | C::Struct | C::ImplBlock))
        });
        let Some(cls) = class else {
            continue;
        };
        for d in cls.descendants().filter(|d| d.is(C::Call)) {
            let Some(callee) = d.child(C::Callee) else {
                continue;
            };
            let Some(member) = callee.child(C::Member) else {
                continue;
            };
            let obj_ivar = member.child(C::Object).and_then(|o| o.child(C::Ivar));
            if obj_ivar.is_none_or(|iv| iv.sym() != ivar_sym) {
                continue;
            }
            let method_sym = member.sym();
            if method_sym == 0 {
                continue;
            }
            let caller_def = d.enclosing(|a| canonical::has_def_type(a));
            let Some(caller_def) = caller_def else {
                continue;
            };
            if let Some(m) = find_method_in(target, method_sym) {
                edges.push(
                    corpus
                        .jump(ce.from.tree, caller_def.index())
                        .edge_to(m, EdgeKind::Calls),
                );
            }
        }
    }
    edges
}

fn is_callable(def: Cursor) -> bool {
    canonical::is_callable_def(def)
}

fn resolve_type(
    ret_sym: u32,
    target_fi: usize,
    corpus: Cursor,
    visible: &VisibleMap,
    cross_edges: &[Edge],
) -> Option<(usize, u32)> {
    if let Some(&loc) = visible[target_fi].get(&ret_sym) {
        return Some(loc);
    }
    for ce in cross_edges {
        if ce.from.tree as usize == target_fi
            && ce.kind == EdgeKind::Imports
            && corpus.follow(ce).child_sym(C::DefName) == Some(ret_sym)
        {
            return Some((ce.to.tree as usize, ce.to.node));
        }
    }
    None
}

fn build_visible_names(trees: &[Tree]) -> VisibleMap {
    trees
        .par_iter()
        .enumerate()
        .map(|(fi, tree)| {
            let mut names = FxHashMap::with_capacity_and_hasher(16, Default::default());
            for c in tree.root().descendants() {
                if canonical::has_def_type(c) {
                    if let Some(ns) = c.child_sym(C::DefName) {
                        names.insert(ns, (fi, c.index()));
                    }
                    if let Some(ds) = c.child_sym(C::DefaultExport) {
                        names.insert(ds, (fi, c.index()));
                    }
                }
            }
            names
        })
        .collect()
}

fn gather_imports(
    trees: &[Tree],
    lang: &Lang,
    file_index: &FxHashMap<String, usize>,
    lookup_prefixes: &[String],
    external: &[String],
) -> (Vec<ImportReq>, Vec<Edge>) {
    let per_tree: Vec<(Vec<ImportReq>, Vec<Edge>)> = trees
        .par_iter()
        .enumerate()
        .map(|(fi, tree)| {
            let mut reqs = Vec::new();
            let mut edges = Vec::new();
            for cur in tree.root().descendants() {
                if cur.kind() != C::Import && cur.kind() != C::ImportType {
                    continue;
                }
                let Some(source_sym) = cur.child_sym(C::SourcePath) else {
                    continue;
                };
                let source_str = lang.syms.resolve(source_sym).to_string();
                if paths::is_external(&source_str, external) {
                    continue;
                }
                let current_file = lang.syms.resolve(tree.root().sym());
                let target_path = paths::resolve_import_source(&source_str, current_file);
                let node_idx = cur.index();
                if let Some(tfi) = paths::resolve_path(&target_path, file_index, lookup_prefixes) {
                    reqs.push(ImportReq {
                        fi,
                        node: node_idx,
                        target_fi: tfi,
                        target_path,
                    });
                } else {
                    for c in cur.names() {
                        let submod = paths::join(&target_path, lang.syms.resolve(c.sym()));
                        if let Some(sub_fi) =
                            paths::resolve_path(&submod, file_index, lookup_prefixes)
                        {
                            edges.push(Edge::new(fi, node_idx, sub_fi, 0, EdgeKind::Imports));
                            reqs.push(ImportReq {
                                fi,
                                node: node_idx,
                                target_fi: sub_fi,
                                target_path: submod,
                            });
                        }
                    }
                }
            }
            (reqs, edges)
        })
        .collect();
    let mut all_reqs = Vec::new();
    let mut all_edges = Vec::new();
    for (reqs, edges) in per_tree {
        all_reqs.extend(reqs);
        all_edges.extend(edges);
    }
    (all_reqs, all_edges)
}

fn propagate_reexports(
    trees: &[Tree],
    reqs: &[ImportReq],
    visible: &mut VisibleMap,
    support_lang: SupportLang,
    index_names: &[String],
    wildcard_sym: u32,
) -> FxHashSet<(usize, u32)> {
    let mut ambiguous: FxHashSet<(usize, u32)> = FxHashSet::default();
    for _round in 0..3 {
        let mut new_exports: Vec<(usize, u32, usize, u32)> = Vec::new();
        for req in reqs {
            if !paths::is_index_file(&trees[req.fi].label, support_lang, index_names) {
                continue;
            }
            for c in trees[req.fi]
                .cursor(req.node)
                .children()
                .filter(|c| c.is(C::Name) && c.sym() != 0)
            {
                let ns = c.sym();
                if ns == wildcard_sym {
                    let target_entries: Vec<_> = visible[req.target_fi]
                        .iter()
                        .map(|(&s, &v)| (s, v))
                        .collect();
                    for (ds, (tfi, tn)) in target_entries {
                        if !visible[req.fi].contains_key(&ds) {
                            new_exports.push((req.fi, ds, tfi, tn));
                        }
                    }
                } else if let Some(&(tfi, tn)) = visible[req.target_fi].get(&ns)
                    && !visible[req.fi].contains_key(&ns)
                {
                    new_exports.push((req.fi, ns, tfi, tn));
                }
            }
        }
        if new_exports.is_empty() {
            break;
        }
        for (fi, ns, tfi, tn) in new_exports {
            if let Some(&(efi, en)) = visible[fi].get(&ns) {
                if efi != tfi || en != tn {
                    ambiguous.insert((fi, ns));
                }
                continue;
            }
            visible[fi].insert(ns, (tfi, tn));
        }
    }
    ambiguous
}

fn follow_import_chain(
    corpus: Cursor,
    reqs: &[ImportReq],
    visible: &VisibleMap,
    wanted: u32,
    start_fi: usize,
) -> Vec<(usize, u32)> {
    let mut results = Vec::new();
    let mut visited = Vec::new();
    let mut stack = vec![(start_fi, wanted)];
    while let Some((fi, ws)) = stack.pop() {
        if visited.contains(&(fi, ws)) {
            continue;
        }
        visited.push((fi, ws));
        if let Some(&loc) = visible[fi].get(&ws) {
            if !results.contains(&loc) {
                results.push(loc);
            }
            continue;
        }
        let tree = &corpus.trees_ref()[fi];
        for nc in tree.root().descendants() {
            if nc.kind() != C::Import {
                continue;
            }
            for c in nc.children().filter(|c| c.is(C::Name)) {
                let import_name = c.sym();
                let alias = c.child_sym(C::Alias).unwrap_or(0);
                if import_name != ws && alias != ws {
                    continue;
                }
                for req in reqs {
                    if req.fi == fi && req.node == nc.index() {
                        stack.push((req.target_fi, import_name));
                    }
                }
            }
        }
        if visited.len() > 10 {
            break;
        }
    }
    results
}

struct ImportReq {
    fi: usize,
    node: u32,
    target_fi: usize,
    target_path: String,
}
