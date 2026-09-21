use either::Either;
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical as C;
use crate::constants::{PATH_SEP, WILDCARD};
use crate::intern::Lang;
use crate::tree::{Cursor, Edge, EdgeKind, Tree, find_method_in, infer_return_type};
use crate::treesitter::SupportLang;

pub const CLASS_LIKE: &[C] = &[
    C::Class,
    C::Struct,
    C::ImplBlock,
    C::Interface,
    C::Trait,
    C::Enum,
];

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

#[derive(Default)]
pub struct FileIndex {
    keys: FxHashMap<String, usize>,
    dirs: FxHashMap<String, Vec<usize>>,
}

impl FileIndex {
    fn insert(&mut self, key: String, fi: usize) {
        let dir = key.rsplit_once(PATH_SEP).map_or("", |(d, _)| d);
        self.dirs.entry(dir.to_string()).or_default().push(fi);
        self.keys.insert(key, fi);
    }
}

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ImportReq {
    pub fi: usize,
    pub node: u32,
    pub target_fi: usize,
    pub target_path: String,
}

pub struct Resolver {
    visible: VisibleMap,
    reqs: Vec<ImportReq>,
    file_index: FileIndex,
    wildcard_sym: u32,
    exports_key: u32,
    visible_from_key: u32,
}

impl Resolver {
    pub fn new(lang: &Lang) -> Self {
        Self {
            visible: Vec::new(),
            reqs: Vec::new(),
            file_index: FileIndex::default(),
            wildcard_sym: lang.syms.intern(WILDCARD),
            exports_key: lang.syms.intern("exports"),
            visible_from_key: lang.syms.intern("visible_from"),
        }
    }

    pub fn from_parts(visible: VisibleMap, reqs: Vec<ImportReq>, lang: &Lang) -> Self {
        Self {
            visible,
            reqs,
            file_index: FileIndex::default(),
            wildcard_sym: lang.syms.intern(WILDCARD),
            exports_key: lang.syms.intern("exports"),
            visible_from_key: lang.syms.intern("visible_from"),
        }
    }

    pub fn visible(&self) -> &VisibleMap {
        &self.visible
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

    #[allow(clippy::too_many_arguments)]
    pub fn resolve(
        &mut self,
        trees: &[Tree],
        edges: &[Edge],
        lang: &Lang,
        dirty_fis: &FxHashSet<usize>,
        support_lang: SupportLang,
        lookup_prefixes: &[String],
        external: &[String],
        aliases: &[(String, String)],
    ) -> ResolveResult {
        let index_names = support_lang.index_names();
        self.file_index = build_file_index(trees, lang, support_lang, index_names);

        self.visible.resize_with(trees.len(), Default::default);
        for &fi in dirty_fis {
            if fi < trees.len() {
                self.visible[fi] = gather_visible_one(&trees[fi], fi, self.exports_key);
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
            aliases,
        );
        self.reqs.extend(new_reqs);

        let ambiguous = propagate_reexports(
            trees,
            &self.reqs,
            &mut self.visible,
            self.wildcard_sym,
            self.visible_from_key,
        );

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
            callable_key: lang.syms.intern("callable"),
            returns_key: lang.syms.intern("returns"),
        };

        let inherit =
            |&fi: &usize| [resolve_inheritance(&ctx, fi), resolve_receivers(&ctx, fi)].concat();
        let wave1: Vec<Edge> = active_reqs
            .par_iter()
            .flat_map(|req| resolve_one_import(&ctx, req))
            .chain(active_fis.par_iter().flat_map(inherit))
            .collect();
        cross_edges.extend(&wave1);

        let mut imports_by_from: Vec<Vec<&Edge>> = vec![vec![]; trees.len()];
        for e in cross_edges.iter().filter(|e| e.kind == EdgeKind::Imports) {
            imports_by_from[e.from_fi()].push(e);
        }

        let wave2: Vec<Edge> = wave1
            .par_iter()
            .filter(|e| e.kind == EdgeKind::Calls)
            .flat_map(|ce| resolve_type_edges(&ctx, ce, &imports_by_from))
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
    file_index: &'a FileIndex,
    reverse_visible: &'a FxHashMap<Loc, u32>,
    support_lang: SupportLang,
    index_names: &'a [String],
    wildcard_sym: u32,
    callable_key: u32,
    returns_key: u32,
}

impl ResolveCtx<'_> {
    fn edges_for(&self, fi: usize) -> &[&Edge] {
        &self.edges_by_tree[fi]
    }
}

fn gather_visible_one(tree: &Tree, fi: usize, exports_key: u32) -> FxHashMap<u32, Loc> {
    tree.root().fold_tree(
        FxHashMap::with_capacity_and_hasher(16, Default::default()),
        |names, c, _w| {
            if c.is(C::Def) {
                let loc = Loc {
                    fi,
                    node: c.index(),
                };
                if let Some(ns) = c.child_sym(C::DefName) {
                    names.insert(ns, loc);
                }
                if let Some(ds) = c.child_sym(C::DefaultExport) {
                    names.insert(ds, loc);
                }
                if let Some(es) = c.tag(exports_key) {
                    names.insert(es, loc);
                }
            }
        },
    )
}

fn gather_imports_for(
    trees: &[Tree],
    lang: &Lang,
    file_index: &FileIndex,
    lookup_prefixes: &[String],
    external: &[String],
    dirty_fis: &FxHashSet<usize>,
    aliases: &[(String, String)],
) -> (Vec<ImportReq>, Vec<Edge>) {
    let resolved_tag_key = lang.syms.intern("resolved_source");
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
                    let source_str = lang.syms.resolve(source_sym);
                    if is_external(source_str, external) {
                        return;
                    }
                    let Some(resolved_sym) = tree.get_tag(cur.index(), resolved_tag_key) else {
                        return;
                    };
                    let raw_path = lang.syms.resolve(resolved_sym);
                    let target_path = apply_aliases(raw_path, aliases);
                    let node_idx = cur.index();
                    let direct = resolve_glob(&target_path, file_index, lookup_prefixes);
                    let candidates = match direct.is_empty() {
                        false => Either::Left(
                            direct
                                .into_iter()
                                .map(|tfi| (tfi, target_path.clone(), false)),
                        ),
                        true => Either::Right(cur.names().flat_map(|c| {
                            let submod =
                                format!("{target_path}{PATH_SEP}{}", lang.syms.resolve(c.sym()));
                            resolve_glob(&submod, file_index, lookup_prefixes)
                                .into_iter()
                                .map(move |sub_fi| (sub_fi, submod.clone(), true))
                        })),
                    };
                    for (tfi, path, is_sub) in candidates.filter(|c| c.0 != fi) {
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
    visible_from_key: u32,
) -> FxHashSet<(usize, u32)> {
    let mut ambiguous: FxHashSet<(usize, u32)> = FxHashSet::default();

    let visible_from_directives: Vec<(usize, u32)> = trees
        .iter()
        .enumerate()
        .flat_map(|(fi, tree)| {
            tree.root().fold_tree(Vec::new(), |out, c, _w| {
                if let Some(source_sym) = c.tag(visible_from_key) {
                    out.push((fi, source_sym));
                }
            })
        })
        .collect();

    let declared: Vec<Vec<u32>> = trees
        .iter()
        .map(|t| {
            t.root()
                .descendants()
                .filter(|c| c.is(C::Decl))
                .filter_map(|c| c.child_sym(C::DefName))
                .collect()
        })
        .collect();
    loop {
        let mut new_exports: Vec<(usize, u32, Loc)> = reqs
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

        for &(fi, source_sym) in &visible_from_directives {
            if let Some(&loc) = visible
                .iter()
                .enumerate()
                .filter(|&(tfi, _)| tfi != fi)
                .find_map(|(_, v)| v.get(&source_sym))
            {
                let source_fi = loc.fi;
                for (&ds, &dloc) in &visible[source_fi] {
                    if !visible[fi].contains_key(&ds) {
                        new_exports.push((fi, ds, dloc));
                    }
                }
            }
        }

        for req in reqs {
            for &n in &declared[req.target_fi] {
                let own = visible[req.fi].get(&n).filter(|l| l.fi == req.fi);
                if let Some(&loc) = own.filter(|_| !visible[req.target_fi].contains_key(&n)) {
                    new_exports.push((req.target_fi, n, loc));
                }
            }
        }
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
    let hint = c.child_sym(C::SsaHint).filter(|&h| h == ctx.wildcard_sym);
    let ns = hint.unwrap_or(c.sym());
    if ns == ctx.wildcard_sym {
        return if c.child_sym(C::Alias).is_some() || hint.is_some() {
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
    resolve_submodule(
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
                .filter(move |loc| loc.fi != fi)
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
        .filter(|e| e.kind == EdgeKind::Imports && is_direct(nodes, e.to_node, import_node))
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
            if tgt.has_tag(ctx.callable_key) {
                edges.push(caller.edge_to(tgt, EdgeKind::Calls));
            }
        }
    }

    for ie in &import_edges {
        let target = ctx.corpus.follow(ie);
        if !target.has_tag(ctx.callable_key) {
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

fn resolve_inheritance(ctx: &ResolveCtx, fi: usize) -> Vec<Edge> {
    let mut out = Vec::new();
    for d in ctx.trees[fi].root().descendants().filter(|d| d.is(C::Def)) {
        let child = ctx.corpus.jump(fi as u32, d.index());
        let supers = d.children_of(C::SuperType).map(|s| s.sym());
        for s in supers.filter(|&s| !ctx.ambiguous.contains(&(fi, s))) {
            if let Some(loc) = ctx.visible[fi].get(&s).filter(|l| l.fi != fi) {
                let parent = ctx.corpus.jump(loc.fi as u32, loc.node);
                out.push(child.edge_to(parent, EdgeKind::Extends));
                for call in child.calls() {
                    let owner = call.enclosing(|e| CLASS_LIKE.iter().any(|&k| e.has(k)));
                    if owner.map(|o| o.index()) != Some(d.index()) {
                        continue;
                    }
                    let callee = call
                        .child(C::Callee)
                        .and_then(|k| k.child(C::Ivar)?.sym_opt());
                    let Some((from, name)) = call.enclosing(|e| e.is(C::Def)).zip(callee) else {
                        continue;
                    };
                    if find_method_in(child, name).is_none()
                        && let Some(m) = method_up(ctx, parent, name, 0)
                    {
                        out.push(from.edge_to(m, EdgeKind::Calls));
                    }
                }
            }
        }
    }
    out
}

fn resolve_type_edges(ctx: &ResolveCtx, ce: &Edge, imports_by_from: &[Vec<&Edge>]) -> Vec<Edge> {
    let target = ctx.corpus.follow(ce);
    let caller = ctx.corpus.jump(ce.from_tree, ce.from_node);

    let Some(ret_sym) = target
        .tag(ctx.returns_key)
        .or_else(|| infer_return_type(target))
    else {
        return vec![];
    };
    let Some(type_loc) = resolve_type(
        ret_sym,
        ce.to_fi(),
        ctx.corpus,
        ctx.visible,
        imports_by_from,
    ) else {
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

fn method_up<'a>(ctx: &'a ResolveCtx, cls: Cursor<'a>, name: u32, depth: u8) -> Option<Cursor<'a>> {
    find_method_in(cls, name).or_else(|| {
        let supers = cls
            .children_of(C::SuperType)
            .filter_map(|s| ctx.visible[cls.fi() as usize].get(&s.sym()));
        supers
            .filter(|_| depth < 8)
            .find_map(|l| method_up(ctx, ctx.corpus.jump(l.fi as u32, l.node), name, depth + 1))
    })
}

fn resolve_receivers(ctx: &ResolveCtx, fi: usize) -> Vec<Edge> {
    let mut out = Vec::new();
    for (call, m) in ctx.trees[fi].root().member_calls() {
        let loc = m
            .child_sym(C::Object)
            .and_then(|s| ctx.visible[fi].get(&s))
            .filter(|l| l.fi != fi);
        let Some(target) = loc.map(|l| ctx.corpus.jump(l.fi as u32, l.node)) else {
            continue;
        };
        if !CLASS_LIKE.iter().any(|&k| target.has(k)) {
            continue;
        }
        if let (Some(from), Some(method)) = (
            call.enclosing(|c| c.is(C::Def)),
            method_up(ctx, target, m.sym(), 0),
        ) {
            out.push(
                ctx.corpus
                    .jump(fi as u32, from.index())
                    .edge_to(method, EdgeKind::Calls),
            );
        }
    }
    out
}

fn resolve_field_edges(ctx: &ResolveCtx, ce: &Edge) -> Vec<Edge> {
    let target = ctx.corpus.follow(ce);
    if !CLASS_LIKE.iter().any(|&k| target.has(k)) {
        return vec![];
    }
    let ft = &ctx.trees[ce.from_fi()];
    let resolved = |c: u32| ctx.visible[ce.from_fi()].get(&c).map(|l| (l.fi, l.node));
    ft.root().fold_tree(Vec::new(), |edges, n, _w| {
        let ivar = n.child(C::Ivar);
        let Some(var) = ivar
            .map_or(n.sym_opt(), |iv| iv.sym_opt())
            .filter(|_| n.is(C::Binding))
        else {
            return;
        };
        let typed = n.child_sym(C::SsaTyped).or_else(|| n.rhs_callee());
        if typed.and_then(resolved) != Some((ce.to_fi(), ce.to_node)) {
            return;
        }
        let scope = if ivar.is_some() {
            n.enclosing_def(CLASS_LIKE)
        } else {
            n.enclosing(|c| c.is(C::Def))
        };
        let Some(scope) = scope else { return };
        for (call, member) in scope.member_calls() {
            let obj = member
                .object_ivar()
                .map_or(member.child_sym(C::Object), |iv| iv.sym_opt());
            let Some(caller) = call
                .enclosing(|c| c.is(C::Def))
                .filter(|_| obj == Some(var))
            else {
                continue;
            };
            if let Some(m) = method_up(ctx, target, member.sym(), 0) {
                edges.push(
                    ctx.corpus
                        .jump(ce.from_tree, caller.index())
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
    imports_by_from: &[Vec<&Edge>],
) -> Option<Loc> {
    if let Some(&loc) = visible[target_fi].get(&ret_sym) {
        return Some(loc);
    }
    for ce in &imports_by_from[target_fi] {
        if corpus.follow(ce).child_sym(C::DefName) == Some(ret_sym) {
            return Some(Loc {
                fi: ce.to_fi(),
                node: ce.to_node,
            });
        }
    }
    None
}

fn build_file_index(
    trees: &[Tree],
    lang: &Lang,
    support_lang: SupportLang,
    index_names: &[String],
) -> FileIndex {
    let mut idx = FileIndex::default();
    let sep = support_lang.fqn_separator();
    for (fi, (tree, path)) in trees.iter().map(|t| (t, t.label.as_str())).enumerate() {
        let file_lang = SupportLang::from_path(path).unwrap_or(support_lang);
        let stem = file_lang.strip_extension(path);
        idx.insert(path.to_string(), fi);
        idx.insert(stem.to_string(), fi);
        for name in index_names {
            if let Some(pkg) = stem
                .strip_suffix(name.as_str())
                .and_then(|s| s.strip_suffix(PATH_SEP))
            {
                if !pkg.is_empty() {
                    idx.insert(pkg.to_string(), fi);
                }
            } else if stem == name.as_str() {
                idx.insert(String::new(), fi);
            }
        }
        let root = tree.root();
        let pkg = root.child_sym(C::Package).map_or(String::new(), |s| {
            lang.syms.resolve(s).replace(sep, PATH_SEP) + PATH_SEP
        });
        let defs = root.children().filter(|d| d.is(C::Def));
        for name in defs.filter_map(|d| d.child_sym(C::DefName)) {
            let key = format!("{pkg}{}", lang.syms.resolve(name).replace(sep, PATH_SEP));
            idx.insert(key, fi);
        }
    }
    idx
}

fn resolve_glob(target: &str, idx: &FileIndex, prefixes: &[String]) -> Vec<usize> {
    let Some(dir) = target
        .strip_suffix(WILDCARD)
        .map(|d| d.trim_end_matches(PATH_SEP))
    else {
        return resolve_path(target, idx, prefixes).into_iter().collect();
    };
    let dirs = std::iter::once(dir.to_string())
        .chain(prefixes.iter().map(|p| format!("{p}{PATH_SEP}{dir}")));
    dirs.filter_map(|d| idx.dirs.get(&d))
        .flatten()
        .copied()
        .unique()
        .collect()
}

fn is_external(source_str: &str, external: &[String]) -> bool {
    external
        .iter()
        .any(|e| e == source_str.split(PATH_SEP).next().unwrap_or(source_str))
}

fn resolve_path(target: &str, file_index: &FileIndex, prefixes: &[String]) -> Option<usize> {
    file_index.keys.get(target).copied().or_else(|| {
        prefixes.iter().find_map(|p| {
            if p.is_empty() {
                file_index.keys.get(target).copied()
            } else {
                file_index
                    .keys
                    .get(&format!("{p}{PATH_SEP}{target}"))
                    .copied()
            }
        })
    })
}

fn resolve_submodule(
    target_path: &str,
    name: &str,
    support_lang: SupportLang,
    index_names: &[String],
    file_index: &FileIndex,
) -> Option<usize> {
    let stem = support_lang.strip_extension(target_path);
    let dir = index_names.iter().find_map(|idx| {
        stem.strip_suffix(idx.as_str())
            .and_then(|s| s.strip_suffix(PATH_SEP))
    })?;
    file_index
        .keys
        .get(&format!("{dir}{PATH_SEP}{name}"))
        .copied()
}

fn apply_aliases(path: &str, aliases: &[(String, String)]) -> String {
    for (key, val) in aliases {
        if let Some(rest) = path.strip_prefix(key.as_str()) {
            let rest = rest.strip_prefix('/').unwrap_or(rest);
            return if rest.is_empty() {
                val.clone()
            } else {
                format!("{val}/{rest}")
            };
        }
    }
    path.to_string()
}
