use either::Either;
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::{self as canonical, Canonical as C};
use crate::intern::Lang;
use crate::tree::{
    CLASS_LIKE, Cursor, Edge, EdgeKind, Tree, find_method_in, infer_return_type, reachable,
};
use crate::treesitter::SupportLang;

use crate::constants::WILDCARD;
use crate::paths;

type VisibleMap = Vec<FxHashMap<u32, (usize, u32)>>;

pub struct ResolvedSourcePath {
    pub fi: usize,
    pub node: u32,
    pub sym: u32,
}

pub struct ResolveResult {
    pub cross_edges: Vec<Edge>,
    pub resolved_source_paths: Vec<ResolvedSourcePath>,
}

pub fn resolve(
    trees: &[Tree],
    edges: &[Edge],
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

    let resolved_source_paths: Vec<ResolvedSourcePath> = reqs
        .iter()
        .filter_map(|req| {
            let sym = lang.syms.intern(&req.target_path);
            let node = trees[req.fi].cursor(req.node).child(C::SourcePath)?.index();
            Some(ResolvedSourcePath {
                fi: req.fi,
                node,
                sym,
            })
        })
        .collect();

    let reverse_visible: FxHashMap<(usize, u32), u32> = visible
        .iter()
        .flat_map(|names| names.iter().map(|(&sym, &(vfi, vn))| ((vfi, vn), sym)))
        .collect();

    let ctx = ResolveCtx {
        trees,
        edges,
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

    ResolveResult {
        cross_edges,
        resolved_source_paths,
    }
}

struct ResolveCtx<'a> {
    trees: &'a [Tree],
    edges: &'a [Edge],
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

fn name_targets(ctx: &ResolveCtx, corpus: Cursor, tfi: usize, c: Cursor) -> Vec<(usize, u32)> {
    let ns = c.sym();
    if ns == ctx.wildcard_sym {
        return if c.child_sym(C::Alias).is_some() {
            vec![(tfi, 0)]
        } else {
            ctx.visible[tfi]
                .iter()
                .filter(|&(&dn, _)| !ctx.ambiguous.contains(&(tfi, dn)))
                .map(|(_, &loc)| loc)
                .collect()
        };
    }
    if ctx.ambiguous.contains(&(tfi, ns)) {
        return vec![];
    }
    if let Some(&loc) = ctx.visible[tfi].get(&ns) {
        return vec![loc];
    }
    if let Ok(one) = follow_import_chain(corpus, ctx.reqs, ctx.visible, ns, tfi)
        .into_iter()
        .exactly_one()
    {
        return vec![one];
    }
    let target_path = ctx.lang.syms.resolve(corpus.jump(tfi as u32, 0).sym());
    paths::resolve_submodule(
        target_path,
        ctx.lang.syms.resolve(ns),
        ctx.support_lang,
        ctx.index_names,
        ctx.file_index,
    )
    .map(|fi| (fi, 0))
    .into_iter()
    .collect()
}

fn targets_import(nodes: &Tree, e: &Edge, import_node: u32) -> bool {
    e.kind == EdgeKind::Imports
        && (e.to_node == import_node
            || nodes
                .cursor(e.to_node)
                .parent()
                .is_some_and(|p| p.index() == import_node))
}

fn is_direct(ft: &Tree, ei: u32, import_name: u32) -> bool {
    let parent = |n: u32| ft.cursor(n).parent().map(|p| p.index());
    ei == import_name || parent(ei) == Some(import_name) || parent(import_name) == Some(ei)
}

fn resolve_one_import(ctx: &ResolveCtx, req: &ImportReq) -> Vec<Edge> {
    let corpus = Cursor::new(ctx.trees, 0, 0);
    let (fi, tfi) = (req.fi, req.target_fi);
    let import = corpus.jump(fi as u32, req.node);

    let import_edges: Vec<Edge> = import
        .names()
        .flat_map(|c| {
            name_targets(ctx, corpus, tfi, c)
                .into_iter()
                .map(move |(rfi, rn)| c.edge_to(c.jump(rfi as u32, rn), EdgeKind::Imports))
        })
        .collect();

    let import_node = req.node;
    let nodes = &ctx.trees[fi];
    let target_files: Vec<usize> = std::iter::once(req.target_fi)
        .chain(import_edges.iter().map(|e| e.to_tree as usize))
        .unique()
        .collect();

    let mut edges: Vec<Edge> = import_edges.clone();

    for edge in ctx
        .edges
        .iter()
        .filter(|e| e.from_tree == fi as u32 && targets_import(nodes, e, import_node))
    {
        let caller = corpus.jump(fi as u32, edge.from_node);
        for (_, m) in caller.member_calls() {
            let Some(&(dfi, dn)) = target_files
                .iter()
                .find_map(|&t| ctx.visible[t].get(&m.sym()))
            else {
                continue;
            };
            let tgt = caller.jump(dfi as u32, dn);
            if is_callable(tgt) {
                edges.push(caller.edge_to(tgt, EdgeKind::Calls));
            }
        }
    }

    for ie in &import_edges {
        let target = corpus.jump(ie.to_tree, ie.to_node);
        if !is_callable(target) {
            continue;
        }
        let target_name = ctx
            .reverse_visible
            .get(&(ie.to_tree as usize, ie.to_node))
            .copied()
            .unwrap_or(0);
        let ft = &ctx.trees[ie.from_tree as usize];
        let is_wild = ft.cursor(ie.from_node).sym() == ctx.wildcard_sym && target_name != 0;

        for intra in ctx.edges.iter().filter(|i| {
            i.from_tree == ie.from_tree
                && i.kind == EdgeKind::Imports
                && is_direct(ft, i.to_node, ie.from_node)
        }) {
            let from = corpus.jump(ie.from_tree, intra.from_node);
            let used = !is_wild
                || from
                    .calls()
                    .any(|d| d.child_sym(C::Callee) == Some(target_name));
            if used {
                edges.push(from.edge_to(target, EdgeKind::Calls));
            }
        }
    }

    edges
}

fn resolve_type_edges(ctx: &ResolveCtx, ce: &Edge, all_cross: &[Edge]) -> Vec<Edge> {
    let corpus = Cursor::new(ctx.trees, 0, 0);
    let target = corpus.follow(ce);
    let caller = corpus.jump(ce.from_tree, ce.from_node);

    let Some(ret_sym) = infer_return_type(target) else {
        return vec![];
    };
    let Some((type_fi, type_node)) =
        resolve_type(ret_sym, ce.to_tree as usize, corpus, ctx.visible, all_cross)
    else {
        return vec![];
    };

    let target_name = target.child_sym(C::DefName).unwrap_or(0);
    let bound: Vec<u32> = caller
        .descendants()
        .filter(|d| d.is(C::Binding) && d.sym_opt().is_some())
        .filter_map(|d| {
            let callee = d.rhs_callee()?;
            (callee == target_name).then_some(d.sym())
        })
        .collect();

    let class = caller.jump(type_fi as u32, type_node);
    caller
        .member_calls()
        .filter(|(_, mn)| {
            mn.child_sym(C::Object)
                .is_some_and(|obj| bound.contains(&obj))
        })
        .filter_map(|(_, mn)| {
            mn.sym_opt()
                .and_then(|method| find_method_in(class, method))
        })
        .map(|m| caller.edge_to(m, EdgeKind::Calls))
        .collect()
}

fn resolve_field_edges(ctx: &ResolveCtx, ce: &Edge) -> Vec<Edge> {
    let corpus = Cursor::new(ctx.trees, 0, 0);
    let target = corpus.follow(ce);
    if !canonical::def_type_of(target).is_some_and(|k| matches!(k, C::Class | C::Struct)) {
        return vec![];
    }
    let Some(target_name) = target.child_sym(C::DefName) else {
        return vec![];
    };
    let ft = &ctx.trees[ce.from_tree as usize];
    ft.root().fold_tree(Vec::new(), |edges, n, _w| {
        if !n.is(C::Binding) {
            return;
        }
        let Some(ivar) = n.child(C::Ivar) else { return };
        if n.rhs_callee() != Some(target_name) {
            return;
        }
        let Some(ivar_sym) = ivar.sym_opt() else {
            return;
        };
        let Some(cls) = n.enclosing_def(CLASS_LIKE) else {
            return;
        };
        for (call, member) in cls.member_calls() {
            if member.object_ivar().map(|iv| iv.sym()) != Some(ivar_sym) {
                continue;
            }
            let Some(caller_def) = call.enclosing(canonical::has_def_type) else {
                continue;
            };
            if let Some(m) = find_method_in(target, member.sym()) {
                edges.push(
                    corpus
                        .jump(ce.from_tree, caller_def.index())
                        .edge_to(m, EdgeKind::Calls),
                );
            }
        }
    })
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
        if ce.from_tree as usize == target_fi
            && ce.kind == EdgeKind::Imports
            && corpus.follow(ce).child_sym(C::DefName) == Some(ret_sym)
        {
            return Some((ce.to_tree as usize, ce.to_node));
        }
    }
    None
}

fn build_visible_names(trees: &[Tree]) -> VisibleMap {
    trees
        .par_iter()
        .enumerate()
        .map(|(fi, tree)| {
            tree.root().fold_tree(
                FxHashMap::with_capacity_and_hasher(16, Default::default()),
                |names, c, _w| {
                    if canonical::has_def_type(c) {
                        if let Some(ns) = c.child_sym(C::DefName) {
                            names.insert(ns, (fi, c.index()));
                        }
                        if let Some(ds) = c.child_sym(C::DefaultExport) {
                            names.insert(ds, (fi, c.index()));
                        }
                    }
                },
            )
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
            tree.root()
                .fold_tree((Vec::new(), Vec::new()), |(reqs, edges), cur, _w| {
                    if cur.kind() != C::Import && cur.kind() != C::ImportType {
                        return;
                    }
                    let Some(source_sym) = cur.child_sym(C::SourcePath) else {
                        return;
                    };
                    let source_str = lang.syms.resolve(source_sym).to_string();
                    if paths::is_external(&source_str, external) {
                        return;
                    }
                    let current_file = lang.syms.resolve(tree.root().sym());
                    let target_path = paths::resolve_import_source(&source_str, current_file);
                    let node_idx = cur.index();
                    let candidates =
                        match paths::resolve_path(&target_path, file_index, lookup_prefixes) {
                            Some(tfi) => {
                                Either::Left(std::iter::once((tfi, target_path.clone(), false)))
                            }
                            None => Either::Right(cur.names().filter_map(|c| {
                                let submod = paths::join(&target_path, lang.syms.resolve(c.sym()));
                                paths::resolve_path(&submod, file_index, lookup_prefixes)
                                    .map(|sub_fi| (sub_fi, submod, true))
                            })),
                        };
                    for (tfi, path, is_sub) in candidates {
                        if is_sub {
                            edges.push(Edge::new(
                                fi as u32,
                                node_idx,
                                tfi as u32,
                                0,
                                EdgeKind::Imports,
                            ));
                        }
                        reqs.push(ImportReq {
                            fi,
                            node: node_idx,
                            target_fi: tfi,
                            target_path: path,
                        });
                    }
                })
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
        let new_exports: Vec<(usize, u32, usize, u32)> = reqs
            .iter()
            .filter(|req| paths::is_index_file(&trees[req.fi].label, support_lang, index_names))
            .flat_map(|req| {
                let mut out = Vec::new();
                for c in trees[req.fi].cursor(req.node).names() {
                    let ns = c.sym();
                    if ns == wildcard_sym {
                        for (&ds, &(vfi, vn)) in &visible[req.target_fi] {
                            if !visible[req.fi].contains_key(&ds) {
                                out.push((req.fi, ds, vfi, vn));
                            }
                        }
                    } else if let Some(&(vfi, vn)) = visible[req.target_fi].get(&ns)
                        && !visible[req.fi].contains_key(&ns)
                    {
                        out.push((req.fi, ns, vfi, vn));
                    }
                }
                out
            })
            .collect();
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
    let succ = |(fi, ws): (usize, u32)| -> Vec<(usize, u32)> {
        let tree = &corpus.trees_ref()[fi];
        tree.root().fold_tree(Vec::new(), |next, nc, _w| {
            if nc.kind() != C::Import {
                return;
            }
            for c in nc.children().filter(|c| c.is(C::Name)) {
                let import_name = c.sym();
                let alias = c.child_sym(C::Alias).unwrap_or(0);
                if import_name != ws && alias != ws {
                    continue;
                }
                for req in reqs {
                    if req.fi == fi && req.node == nc.index() {
                        next.push((req.target_fi, import_name));
                    }
                }
            }
        })
    };
    reachable((start_fi, wanted), succ)
        .take(10)
        .filter_map(|(fi, ws)| visible[fi].get(&ws).copied())
        .unique()
        .collect()
}

struct ImportReq {
    fi: usize,
    node: u32,
    target_fi: usize,
    target_path: String,
}
