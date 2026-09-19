use either::Either;
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::{self as canonical, Canonical as C};
use crate::intern::Lang;
use crate::tree::{CLASS_LIKE, Cursor, Edge, EdgeKind, Tree, find_method_in, infer_return_type};
use crate::treesitter::SupportLang;

use crate::constants::WILDCARD;
use crate::paths;

type VisibleMap = Vec<FxHashMap<u32, Loc>>;

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, Hash, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize,
)]
pub struct Loc {
    pub fi: usize,
    pub node: u32,
}

pub struct ResolvedSourcePath {
    pub fi: usize,
    pub node: u32,
    pub sym: u32,
}

pub struct ResolveResult {
    pub cross_edges: Vec<Edge>,
    pub resolved_source_paths: Vec<ResolvedSourcePath>,
}

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ImportReq {
    pub fi: usize,
    pub node: u32,
    pub target_fi: usize,
    pub target_path: String,
}

#[derive(rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ResolverSnapshot {
    pub visible: Vec<Vec<(u32, Loc)>>,
    pub reqs: Vec<ImportReq>,
}

pub struct Resolver {
    visible: VisibleMap,
    reqs: Vec<ImportReq>,
    file_index: FxHashMap<String, usize>,
    wildcard_sym: u32,
}

impl Resolver {
    pub fn new(lang: &Lang) -> Self {
        Self {
            visible: Vec::new(),
            reqs: Vec::new(),
            file_index: FxHashMap::default(),
            wildcard_sym: lang.syms.intern(WILDCARD),
        }
    }

    pub fn from_snapshot(snap: ResolverSnapshot, lang: &Lang) -> Self {
        let visible = snap
            .visible
            .into_iter()
            .map(|entries| entries.into_iter().collect())
            .collect();
        Self {
            visible,
            reqs: snap.reqs,
            file_index: FxHashMap::default(),
            wildcard_sym: lang.syms.intern(WILDCARD),
        }
    }

    pub fn to_snapshot(&self) -> ResolverSnapshot {
        let visible = self
            .visible
            .iter()
            .map(|map| map.iter().map(|(&sym, &loc)| (sym, loc)).collect())
            .collect();
        ResolverSnapshot {
            visible,
            reqs: self.reqs.clone(),
        }
    }

    pub fn reqs(&self) -> &[ImportReq] {
        &self.reqs
    }

    pub fn remap(&mut self, old_labels: &[String], label_to_fi: &FxHashMap<&str, u32>) {
        let n = label_to_fi.len();
        let mut remapped: VisibleMap = (0..n)
            .map(|_| FxHashMap::with_capacity_and_hasher(16, Default::default()))
            .collect();
        for (old_fi, names) in self.visible.iter().enumerate() {
            let Some(&new_fi) = old_labels
                .get(old_fi)
                .and_then(|l| label_to_fi.get(l.as_str()))
            else {
                continue;
            };
            for (&sym, &loc) in names {
                if let Some(&target_new_fi) = old_labels
                    .get(loc.fi)
                    .and_then(|l| label_to_fi.get(l.as_str()))
                {
                    remapped[new_fi as usize].insert(
                        sym,
                        Loc {
                            fi: target_new_fi as usize,
                            node: loc.node,
                        },
                    );
                }
            }
        }
        self.visible = remapped;

        self.reqs = self
            .reqs
            .iter()
            .filter_map(|r| {
                let new_fi = *old_labels
                    .get(r.fi)
                    .and_then(|l| label_to_fi.get(l.as_str()))?
                    as usize;
                let new_tfi = *old_labels
                    .get(r.target_fi)
                    .and_then(|l| label_to_fi.get(l.as_str()))?
                    as usize;
                Some(ImportReq {
                    fi: new_fi,
                    node: r.node,
                    target_fi: new_tfi,
                    target_path: r.target_path.clone(),
                })
            })
            .collect();
    }

    pub fn resolve(
        &mut self,
        trees: &[Tree],
        edges: &[Edge],
        lang: &Lang,
        dirty_fis: &FxHashSet<usize>,
        support_lang: SupportLang,
        lookup_prefixes: &[String],
        external: &[String],
    ) -> ResolveResult {
        let index_names = support_lang.index_names();
        let labels: Vec<String> = trees.iter().map(|t| t.label.clone()).collect();
        self.file_index = paths::build_file_index(&labels, support_lang, index_names);

        self.visible.resize_with(trees.len(), Default::default);
        for &fi in dirty_fis {
            if fi < trees.len() {
                self.visible[fi] = gather_visible_one(&trees[fi], fi);
            }
        }

        self.reqs.retain(|r| !dirty_fis.contains(&r.fi));
        let (new_reqs, mut cross_edges) = gather_imports_for(
            trees,
            lang,
            &self.file_index,
            lookup_prefixes,
            external,
            dirty_fis,
        );
        self.reqs.extend(new_reqs);

        let ambiguous =
            propagate_reexports(trees, &self.reqs, &mut self.visible, self.wildcard_sym);

        let resolved_source_paths: Vec<ResolvedSourcePath> = self
            .reqs
            .iter()
            .filter(|req| dirty_fis.contains(&req.fi))
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

        let reverse_dirty: FxHashSet<usize> = self
            .reqs
            .iter()
            .filter(|r| !dirty_fis.contains(&r.fi) && dirty_fis.contains(&r.target_fi))
            .map(|r| r.fi)
            .collect();
        let active_fis: FxHashSet<usize> = dirty_fis.union(&reverse_dirty).copied().collect();
        let active_reqs: Vec<&ImportReq> = self
            .reqs
            .iter()
            .filter(|r| active_fis.contains(&r.fi) || active_fis.contains(&r.target_fi))
            .collect();

        let reverse_visible: FxHashMap<Loc, u32> = self
            .visible
            .iter()
            .flat_map(|names| names.iter().map(|(&sym, &loc)| (loc, sym)))
            .collect();

        let mut edges_by_tree: Vec<Vec<&Edge>> = vec![vec![]; trees.len()];
        for e in edges {
            edges_by_tree[e.from_fi()].push(e);
        }

        let ctx = ResolveCtx {
            trees,
            corpus: Cursor::new(trees, 0, 0),
            edges_by_tree: &edges_by_tree,
            lang,
            visible: &self.visible,
            ambiguous: &ambiguous,
            file_index: &self.file_index,
            reverse_visible: &reverse_visible,
            support_lang,
            index_names,
            wildcard_sym: self.wildcard_sym,
        };

        let wave1: Vec<Edge> = active_reqs
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
}

struct ResolveCtx<'a> {
    trees: &'a [Tree],
    corpus: Cursor<'a>,
    edges_by_tree: &'a [Vec<&'a Edge>],
    lang: &'a Lang,
    visible: &'a VisibleMap,
    ambiguous: &'a FxHashSet<(usize, u32)>,
    file_index: &'a FxHashMap<String, usize>,
    reverse_visible: &'a FxHashMap<Loc, u32>,
    support_lang: SupportLang,
    index_names: &'a [String],
    wildcard_sym: u32,
}

impl ResolveCtx<'_> {
    fn edges_for(&self, fi: usize) -> &[&Edge] {
        &self.edges_by_tree[fi]
    }
}

fn gather_visible_one(tree: &Tree, fi: usize) -> FxHashMap<u32, Loc> {
    tree.root().fold_tree(
        FxHashMap::with_capacity_and_hasher(16, Default::default()),
        |names, c, _w| {
            if canonical::has_def_type(c) {
                if let Some(ns) = c.child_sym(C::DefName) {
                    names.insert(
                        ns,
                        Loc {
                            fi,
                            node: c.index(),
                        },
                    );
                }
                if let Some(ds) = c.child_sym(C::DefaultExport) {
                    names.insert(
                        ds,
                        Loc {
                            fi,
                            node: c.index(),
                        },
                    );
                }
            }
        },
    )
}

fn gather_imports_for(
    trees: &[Tree],
    lang: &Lang,
    file_index: &FxHashMap<String, usize>,
    lookup_prefixes: &[String],
    external: &[String],
    dirty_fis: &FxHashSet<usize>,
) -> (Vec<ImportReq>, Vec<Edge>) {
    let dirty_vec: Vec<usize> = dirty_fis.iter().copied().collect();
    let per_tree: Vec<(Vec<ImportReq>, Vec<Edge>)> = dirty_vec
        .par_iter()
        .map(|&fi| {
            let tree = &trees[fi];
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
    wildcard_sym: u32,
) -> FxHashSet<(usize, u32)> {
    let mut ambiguous: FxHashSet<(usize, u32)> = FxHashSet::default();
    loop {
        let new_exports: Vec<(usize, u32, Loc)> = reqs
            .iter()
            .flat_map(|req| {
                let mut out = Vec::new();
                for c in trees[req.fi].cursor(req.node).names() {
                    let ns = c.sym();
                    if ns == wildcard_sym {
                        for (&ds, &loc) in &visible[req.target_fi] {
                            if !visible[req.fi].contains_key(&ds) {
                                out.push((req.fi, ds, loc));
                            }
                        }
                    } else if let Some(&loc) = visible[req.target_fi].get(&ns) {
                        let export_as = c.child_sym(C::Alias).unwrap_or(ns);
                        if !visible[req.fi].contains_key(&export_as) {
                            out.push((req.fi, export_as, loc));
                        }
                    }
                }
                out
            })
            .collect();
        if new_exports.is_empty() {
            break;
        }
        for (fi, ns, loc) in new_exports {
            if let Some(&existing) = visible[fi].get(&ns) {
                if existing != loc {
                    ambiguous.insert((fi, ns));
                }
                continue;
            }
            visible[fi].insert(ns, loc);
        }
    }
    ambiguous
}

fn name_targets(ctx: &ResolveCtx, tfi: usize, c: Cursor) -> Vec<Loc> {
    let ns = c.sym();
    if ns == ctx.wildcard_sym {
        return if c.child_sym(C::Alias).is_some() {
            vec![Loc { fi: tfi, node: 0 }]
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
    let target_path = ctx.lang.syms.resolve(ctx.corpus.jump(tfi as u32, 0).sym());
    paths::resolve_submodule(
        target_path,
        ctx.lang.syms.resolve(ns),
        ctx.support_lang,
        ctx.index_names,
        ctx.file_index,
    )
    .map(|fi| Loc { fi, node: 0 })
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
    let (fi, tfi) = (req.fi, req.target_fi);
    let import = ctx.corpus.jump(fi as u32, req.node);

    let import_edges: Vec<Edge> = import
        .names()
        .flat_map(|c| {
            name_targets(ctx, tfi, c)
                .into_iter()
                .map(move |loc| c.edge_to(c.jump(loc.fi as u32, loc.node), EdgeKind::Imports))
        })
        .collect();

    let import_node = req.node;
    let nodes = &ctx.trees[fi];
    let target_files: Vec<usize> = std::iter::once(tfi)
        .chain(import_edges.iter().map(|e| e.to_fi()))
        .unique()
        .collect();

    let mut edges: Vec<Edge> = import_edges.clone();

    for edge in ctx
        .edges_for(fi)
        .iter()
        .filter(|e| targets_import(nodes, e, import_node))
    {
        let caller = ctx.corpus.jump(fi as u32, edge.from_node);
        for (_, m) in caller.member_calls() {
            let Some(&loc) = target_files
                .iter()
                .find_map(|&t| ctx.visible[t].get(&m.sym()))
            else {
                continue;
            };
            let tgt = ctx.corpus.jump(loc.fi as u32, loc.node);
            if canonical::is_callable_def(tgt) {
                edges.push(caller.edge_to(tgt, EdgeKind::Calls));
            }
        }
    }

    for ie in &import_edges {
        let target = ctx.corpus.follow(ie);
        if !canonical::is_callable_def(target) {
            continue;
        }
        let target_loc = Loc {
            fi: ie.to_fi(),
            node: ie.to_node,
        };
        let target_name = ctx.reverse_visible.get(&target_loc).copied().unwrap_or(0);
        let ft = &ctx.trees[ie.from_fi()];
        let is_wild = ft.cursor(ie.from_node).sym() == ctx.wildcard_sym && target_name != 0;

        for intra in ctx
            .edges_for(ie.from_fi())
            .iter()
            .filter(|i| i.kind == EdgeKind::Imports && is_direct(ft, i.to_node, ie.from_node))
        {
            let from = ctx.corpus.jump(ie.from_tree, intra.from_node);
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
    let target = ctx.corpus.follow(ce);
    let caller = ctx.corpus.jump(ce.from_tree, ce.from_node);

    let Some(ret_sym) = infer_return_type(target) else {
        return vec![];
    };
    let Some(type_loc) = resolve_type(ret_sym, ce.to_fi(), ctx.corpus, ctx.visible, all_cross)
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

    let class = ctx.corpus.jump(type_loc.fi as u32, type_loc.node);
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
    let target = ctx.corpus.follow(ce);
    if !canonical::def_type_of(target).is_some_and(|k| matches!(k, C::Class | C::Struct)) {
        return vec![];
    }
    let Some(target_name) = target.child_sym(C::DefName) else {
        return vec![];
    };
    let ft = &ctx.trees[ce.from_fi()];
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
                    ctx.corpus
                        .jump(ce.from_tree, caller_def.index())
                        .edge_to(m, EdgeKind::Calls),
                );
            }
        }
    })
}

fn resolve_type(
    ret_sym: u32,
    target_fi: usize,
    corpus: Cursor,
    visible: &VisibleMap,
    cross_edges: &[Edge],
) -> Option<Loc> {
    if let Some(&loc) = visible[target_fi].get(&ret_sym) {
        return Some(loc);
    }
    for ce in cross_edges {
        if ce.from_fi() == target_fi
            && ce.kind == EdgeKind::Imports
            && corpus.follow(ce).child_sym(C::DefName) == Some(ret_sym)
        {
            return Some(Loc {
                fi: ce.to_fi(),
                node: ce.to_node,
            });
        }
    }
    None
}
