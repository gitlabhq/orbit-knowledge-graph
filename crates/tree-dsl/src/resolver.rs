use either::Either;
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical as C;
use crate::constants::{PATH_SEP, WILDCARD};
use crate::intern::Lang;
use crate::tree::{
    Cursor, Edge, EdgeKind, Tree, find_method_in, infer_return_type, members_by_level, reachable,
};
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

impl Loc {
    fn new(fi: usize, node: u32) -> Self {
        Self { fi, node }
    }
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

    fn get(&self, key: &str) -> Option<usize> {
        self.keys.get(key).copied()
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
            ..Self::new(lang)
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
                    remapped[new_fi as usize]
                        .insert(sym, Loc::new(target_new_fi as usize, loc.node));
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

        let reverse_visible: FxHashMap<Loc, u32> = self
            .visible
            .iter()
            .flat_map(|names| names.iter().map(|(&sym, &loc)| (loc, sym)))
            .collect();

        let mut type_uses: FxHashMap<(u32, u32), Vec<&Edge>> = FxHashMap::default();
        let mut producers: FxHashMap<(u32, u32), Vec<&Edge>> = FxHashMap::default();
        let mut edges_by_tree: Vec<Vec<&Edge>> = vec![vec![]; trees.len()];
        for e in edges {
            edges_by_tree[e.from_fi()].push(e);
            if e.kind == EdgeKind::TypeFlow {
                type_uses.entry((e.to_tree, e.to_node)).or_default().push(e);
                if let Some(site) = e.site {
                    producers.entry((e.from_tree, site)).or_default().push(e);
                }
            }
        }

        let (partials, extensions) = gather_members(trees);
        let exporters: Vec<FxHashSet<usize>> = self
            .visible
            .iter()
            .map(|names| names.values().map(|l| l.fi).collect())
            .collect();
        let imports: Vec<FxHashMap<u32, (u32, u32)>> = trees
            .iter()
            .map(|t| {
                let imports = t.root().descendants().filter(|c| c.is(C::Import));
                imports
                    .flat_map(|i| i.names())
                    .fold(FxHashMap::default(), |mut m, n| {
                        let local = n.child_sym(C::Alias).unwrap_or(n.sym());
                        m.entry(local).or_insert_with(|| import_identity(n));
                        m
                    })
            })
            .collect();
        let mut ctx = ResolveCtx {
            extends: &[],
            corpus: Cursor::new(trees, 0, 0),
            edges_by_tree: &edges_by_tree,
            type_uses,
            producers,
            lang,
            visible: &self.visible,
            ambiguous: &ambiguous,
            file_index: &self.file_index,
            reverse_visible: &reverse_visible,
            support_lang,
            index_names,
            wildcard_sym: self.wildcard_sym,
            callable_key: lang.syms.intern("callable"),
            implicit_self_key: lang.syms.intern("implicit_self"),
            partials: &partials,
            extensions: &extensions,
            exporters: &exporters,
            imports: &imports,
        };

        let inherit =
            |&fi: &usize| [resolve_inheritance(&ctx, fi), resolve_receivers(&ctx, fi)].concat();
        let wave1: Vec<Edge> = self
            .reqs
            .par_iter()
            .filter(|r| active_fis.contains(&r.fi) || active_fis.contains(&r.target_fi))
            .flat_map(|req| resolve_one_import(&ctx, req))
            .chain(active_fis.par_iter().flat_map(inherit))
            .filter(|e| {
                !(e.kind == EdgeKind::Calls
                    && e.site
                        .is_some_and(|s| ctx.corpus.jump(e.from_tree, s).has(C::Property))
                    && ctx.corpus.follow(e).is_class())
            })
            .collect();
        ctx.extends = &wave1;
        cross_edges.extend(&wave1);

        let wave2: Vec<Edge> = ctx
            .type_uses
            .par_iter()
            .map(|(&(tree, node), uses)| (ctx.corpus.jump(tree, node), uses))
            .filter(|(producer, _)| {
                producer.is(C::Binding) || external_of(&ctx, *producer).is_some()
            })
            .flat_map(|(producer, uses)| {
                dispatch(&ctx, producer, producer_class(&ctx, producer), uses)
            })
            .collect();
        cross_edges.extend(&wave2);

        let mut seen = FxHashSet::default();
        let mut wave: Vec<Edge> = edges
            .iter()
            .chain(&cross_edges)
            .filter(|e| e.kind == EdgeKind::Calls && active_fis.contains(&e.from_fi()))
            .copied()
            .collect();
        let key = |e: &Edge| (e.from_tree, e.from_node, e.site, e.to_tree, e.to_node);
        seen.extend(edges.iter().filter(|e| e.kind == EdgeKind::Calls).map(key));
        cross_edges.retain(|e| e.kind != EdgeKind::Calls || seen.insert(key(e)));
        let mut type_edges: Vec<Edge> = Vec::new();
        while !wave.is_empty() {
            wave = wave
                .par_iter()
                .flat_map(|ce| resolve_type_edges(&ctx, ce))
                .collect::<Vec<_>>()
                .into_iter()
                .filter(|e| seen.insert(key(e)))
                .collect();
            type_edges.extend(&wave);
        }
        cross_edges.extend(type_edges);

        ResolveResult {
            cross_edges,
            resolved_source_paths,
        }
    }
}

struct ResolveCtx<'a> {
    extends: &'a [Edge],
    corpus: Cursor<'a>,
    edges_by_tree: &'a [Vec<&'a Edge>],
    type_uses: FxHashMap<(u32, u32), Vec<&'a Edge>>,
    producers: FxHashMap<(u32, u32), Vec<&'a Edge>>,
    lang: &'a Lang,
    visible: &'a VisibleMap,
    ambiguous: &'a FxHashSet<(usize, u32)>,
    file_index: &'a FileIndex,
    reverse_visible: &'a FxHashMap<Loc, u32>,
    support_lang: SupportLang,
    index_names: &'a [String],
    wildcard_sym: u32,
    callable_key: u32,
    implicit_self_key: u32,
    partials: &'a FxHashMap<(u32, u32, usize), Vec<Loc>>,
    extensions: &'a FxHashMap<u32, Vec<Loc>>,
    exporters: &'a [FxHashSet<usize>],
    imports: &'a [FxHashMap<u32, (u32, u32)>],
}

impl ResolveCtx<'_> {
    fn imports_to(&self, fi: usize, target: u32) -> impl Iterator<Item = &Edge> {
        let parent = move |n| self.corpus.jump(fi as u32, n).parent().map(|p| p.index());
        self.edges_by_tree[fi].iter().copied().filter(move |e| {
            e.kind == EdgeKind::Imports
                && (e.to_node == target
                    || parent(e.to_node) == Some(target)
                    || parent(target) == Some(e.to_node))
        })
    }
}

fn gather_visible_one(tree: &Tree, fi: usize, exports_key: u32) -> FxHashMap<u32, Loc> {
    tree.root().fold_tree(FxHashMap::default(), |names, c, _w| {
        if !c.is(C::Def) || c.has(C::ImplBlock) {
            return;
        }
        let loc = Loc::new(fi, c.index());
        let nested = c.is_class() && c.enclosing(|p| p.is(C::Def)).is_some_and(|p| p.is_class());
        for name in c
            .child_sym(C::DefName)
            .filter(|_| !nested)
            .into_iter()
            .chain(c.child_sym(C::DefaultExport))
            .chain(c.tag(exports_key))
        {
            if names
                .get(&name)
                .is_none_or(|old: &Loc| !tree.cursor(old.node).is_class())
            {
                names.insert(name, loc);
            }
        }
    })
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

    let mut visible_from_directives = Vec::new();
    let mut declared = vec![Vec::new(); trees.len()];
    for (fi, tree) in trees.iter().enumerate() {
        for c in tree.root().descendants() {
            if let Some(source_sym) = c.tag(visible_from_key) {
                visible_from_directives.push((fi, source_sym));
            }
            if c.is(C::Decl)
                && let Some(name) = c.child_sym(C::DefName)
            {
                declared[fi].push(name);
            }
        }
    }
    loop {
        let mut new_exports = Vec::new();
        let mut export = |fi: usize, name, loc| {
            if !visible[fi].contains_key(&name) {
                new_exports.push((fi, name, loc));
            }
        };
        for req in reqs {
            for c in trees[req.fi].cursor(req.node).names() {
                let hint = c.child_sym(C::SsaHint).filter(|&h| h == wildcard_sym);
                let ns = hint.unwrap_or(c.sym());
                if ns == wildcard_sym {
                    for (&ds, &loc) in &visible[req.target_fi] {
                        export(req.fi, ds, loc);
                    }
                } else if let Some(&loc) = visible[req.target_fi].get(&ns) {
                    export(req.fi, c.child_sym(C::Alias).unwrap_or(ns), loc);
                }
            }
        }

        for &(fi, source_sym) in &visible_from_directives {
            if let Some(&loc) = visible
                .iter()
                .enumerate()
                .filter(|&(tfi, _)| tfi != fi)
                .find_map(|(_, v)| v.get(&source_sym))
            {
                let source_fi = loc.fi;
                for (&ds, &dloc) in &visible[source_fi] {
                    export(fi, ds, dloc);
                }
            }
        }

        for req in reqs {
            for &n in &declared[req.target_fi] {
                let own = visible[req.fi].get(&n).filter(|l| l.fi == req.fi);
                if let Some(&loc) = own {
                    export(req.target_fi, n, loc);
                }
            }
        }
        if new_exports.is_empty() {
            break;
        }
        for (fi, ns, loc) in new_exports {
            if let Some(&existing) = visible[fi].get(&ns) {
                let key = |l: Loc| partial_key(trees[l.fi].cursor(l.node));
                if existing != loc && (key(existing).is_none() || key(existing) != key(loc)) {
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

    let target_files: Vec<usize> = std::iter::once(tfi)
        .chain(import_edges.iter().map(|e| e.to_fi()))
        .unique()
        .collect();

    let mut edges: Vec<Edge> = import_edges.clone();

    for edge in ctx.imports_to(fi, req.node) {
        let caller = ctx.corpus.jump(fi as u32, edge.from_node);
        let Some(m) = edge
            .site
            .map(|site| caller.jump(fi as u32, site))
            .and_then(|c| c.member())
        else {
            continue;
        };
        let local = |n: Cursor| n.child_sym(C::Alias).unwrap_or(n.sym());
        if !import
            .names()
            .any(|n| m.child_sym(C::Object) == Some(local(n)))
        {
            continue;
        }
        let members = target_files
            .iter()
            .filter_map(|&t| ctx.visible[t].get(&m.sym()))
            .unique()
            .map(|loc| ctx.corpus.jump(loc.fi as u32, loc.node))
            .filter(|tgt| tgt.has_tag(ctx.callable_key))
            .collect();
        edges.extend(call_edges(caller, members, edge.site));
    }

    for ie in &import_edges {
        let target = ctx.corpus.follow(ie);
        if !target.has_tag(ctx.callable_key) {
            continue;
        }
        let target_loc = Loc::new(ie.to_fi(), ie.to_node);
        let target_name = ctx.reverse_visible.get(&target_loc).copied().unwrap_or(0);
        let name = ctx.corpus.jump(ie.from_tree, ie.from_node);
        let is_wild = (name.sym() == ctx.wildcard_sym
            || name.child_sym(C::SsaHint) == Some(ctx.wildcard_sym))
            && target_name != 0;

        for intra in ctx.imports_to(ie.from_fi(), ie.from_node) {
            let from = ctx.corpus.jump(ie.from_tree, intra.from_node);
            let used = !is_wild
                || from
                    .calls()
                    .any(|d| d.child_sym(C::Callee) == Some(target_name));
            if used {
                let site = intra.site.filter(|&site| {
                    !is_wild
                        || ctx.corpus.jump(intra.from_tree, site).child_sym(C::Callee)
                            == Some(target_name)
                });
                edges.push(call_edge(from, target, site));
            }
        }
    }

    edges
}

fn call_edge(from: Cursor, target: Cursor, site: Option<u32>) -> Edge {
    Edge {
        site,
        ..from.edge_to(target, EdgeKind::Calls)
    }
}

fn call_edges<'a>(
    from: Cursor<'a>,
    targets: Vec<Cursor<'a>>,
    site: Option<u32>,
) -> impl Iterator<Item = Edge> + 'a {
    targets.into_iter().map(move |t| call_edge(from, t, site))
}

fn resolve_inheritance(ctx: &ResolveCtx, fi: usize) -> Vec<Edge> {
    let mut out = Vec::new();
    let root = ctx.corpus.jump(fi as u32, 0);
    for child in root.descendants().filter(|d| d.is(C::Def)) {
        for s in child.children().filter(|s| s.is(C::SuperType)) {
            if let Some(parent) = resolve_chain(ctx, s).filter(|p| p.fi() != fi as u32) {
                out.push(child.edge_to(parent, EdgeKind::Extends));
                for call in child.calls() {
                    let owner = call.enclosing(|e| e.is_class());
                    let owned = owner.is_some_and(|o| o.index() == child.index());
                    let callee = call.child(C::Callee).and_then(|k| {
                        k.child_sym(C::Ivar)
                            .or_else(|| call.has_tag(ctx.implicit_self_key).then(|| k.sym()))
                    });
                    let from = call.enclosing(|e| e.is(C::Def)).filter(|_| owned);
                    let Some((from, name)) = from.zip(callee) else {
                        continue;
                    };
                    if find_method_in(child, name).is_none() {
                        let site = Some(call.index());
                        out.extend(call_edges(from, method_up(ctx, parent, name, fi), site));
                    }
                }
            }
        }
    }
    out
}

fn resolve_type_edges(ctx: &ResolveCtx, ce: &Edge) -> Vec<Edge> {
    let uses = |site| Some((site, ctx.type_uses.get(&(ce.from_tree, site))?));
    let Some((site, uses)) = ce.site.and_then(uses) else {
        return vec![];
    };
    let producer = ctx.corpus.jump(ce.from_tree, site);
    dispatch(ctx, producer, class_of(ctx, ctx.corpus.follow(ce)), uses)
}

fn dispatch(
    ctx: &ResolveCtx,
    producer: Cursor,
    class: Option<Cursor>,
    uses: &[&Edge],
) -> Vec<Edge> {
    uses.iter()
        .filter_map(|usage| {
            let call = ctx.corpus.jump(usage.from_tree, usage.site?);
            let reaching = ctx.producers.get(&(usage.from_tree, usage.site?));
            let class = match reaching {
                Some(reaching) if reaching.len() > 1 => lub(
                    ctx,
                    reaching
                        .iter()
                        .map(|e| producer_class(ctx, ctx.corpus.follow(e))),
                ),
                _ => class,
            };
            let from = ctx.corpus.jump(usage.from_tree, usage.from_node);
            let Some(class) = class else {
                let owner = Owner::External(external_of(ctx, producer)?);
                let targets = extension_members(ctx, owner, call.member()?.sym(), usage.from_fi());
                return Some(call_edges(from, targets, usage.site));
            };
            let class = match call.member().and_then(|m| m.child(C::Object)) {
                Some(object) if object.child(C::Member).is_some() => {
                    chain(ctx, object, &|_| Some(class))?
                }
                _ => class,
            };
            let name = call
                .member()
                .map(|m| m.sym())
                .or_else(|| class.child_sym(C::Callable));
            let targets = match name {
                Some(name) => method_up(ctx, class, name, usage.from_fi()),
                None => vec![class],
            };
            Some(call_edges(from, targets, usage.site))
        })
        .flatten()
        .collect()
}

fn import_identity(name: Cursor) -> (u32, u32) {
    let source = name.parent().and_then(|i| i.child_sym(C::SourcePath));
    (source.unwrap_or(0), name.sym())
}

fn external_of(ctx: &ResolveCtx, producer: Cursor) -> Option<(u32, u32)> {
    let named = match producer.is(C::Binding) {
        true => producer.typed(),
        false => producer.child(C::Callee),
    };
    let named = named.filter(|t| !t.has(C::Member))?;
    if visible_type(ctx, named.fi(), named.sym()).is_some() {
        return None;
    }
    ctx.imports[named.fi() as usize].get(&named.sym()).copied()
}

enum Owner<'a> {
    Class(Cursor<'a>),
    External((u32, u32)),
}

fn producer_class<'a>(ctx: &'a ResolveCtx, producer: Cursor<'a>) -> Option<Cursor<'a>> {
    let callee = if producer.is(C::Binding) {
        resolve_chain(ctx, producer.typed()?)?
    } else {
        callee_of(ctx, producer)?
    };
    class_of(ctx, callee)
}

fn callee_of<'a>(ctx: &'a ResolveCtx, call: Cursor<'a>) -> Option<Cursor<'a>> {
    let local = ctx.edges_by_tree[call.fi() as usize]
        .iter()
        .find(|e| e.kind == EdgeKind::Calls && e.site == Some(call.index()))
        .map(|e| ctx.corpus.follow(e));
    local.or_else(|| resolve_chain(ctx, call.child(C::Callee)?))
}

fn class_of<'a>(ctx: &'a ResolveCtx, callee: Cursor<'a>) -> Option<Cursor<'a>> {
    let target = value_type(ctx, callee)?;
    if target.is_class() {
        Some(target)
    } else if let Some(ty) = target.child(C::SsaReturnType) {
        resolve_chain(ctx, ty)
    } else if let Some(branch) = target
        .descendants_pruned(|n| n.is(C::Def))
        .find(|n| n.is(C::SsaReturn))
        .and_then(|r| r.child(C::SsaBranch))
    {
        branch_type(ctx, branch)
    } else {
        infer_return_type(target).and_then(|sym| visible_type(ctx, target.fi(), sym))
    }
}

fn visible_type<'a>(ctx: &'a ResolveCtx, fi: u32, sym: u32) -> Option<Cursor<'a>> {
    let loc = ctx.visible[fi as usize]
        .get(&sym)
        .filter(|_| !ctx.ambiguous.contains(&(fi as usize, sym)))?;
    Some(ctx.corpus.jump(loc.fi as u32, loc.node))
}

fn resolve_chain<'a>(ctx: &'a ResolveCtx, c: Cursor<'a>) -> Option<Cursor<'a>> {
    chain(ctx, c, &|r| visible_type(ctx, r.fi(), r.sym()))
}

fn chain<'a>(
    ctx: &'a ResolveCtx,
    c: Cursor<'a>,
    root: &dyn Fn(Cursor<'a>) -> Option<Cursor<'a>>,
) -> Option<Cursor<'a>> {
    let c = c.reference();
    let Some(m) = c.has(C::Object).then_some(c).or_else(|| c.child(C::Member)) else {
        return root(c);
    };
    let receiver = chain(ctx, m.child(C::Object)?, root)?;
    let member = method_up(ctx, receiver, m.sym(), c.fi() as usize)
        .into_iter()
        .exactly_one()
        .ok()?;
    value_type(ctx, member)
}

fn value_type<'a>(ctx: &'a ResolveCtx, d: Cursor<'a>) -> Option<Cursor<'a>> {
    if d.has(C::EnumVariant) {
        d.enclosing_def(&[C::Enum])
    } else if d.has(C::FieldDef) || d.has(C::Property) {
        let declared = d.child(C::Binding).and_then(Cursor::typed);
        match declared.or_else(|| d.child(C::SsaReturnType)) {
            Some(ty) => resolve_chain(ctx, ty),
            None => Some(d),
        }
    } else {
        Some(d)
    }
}

fn partial_key(d: Cursor) -> Option<(u32, u32, usize)> {
    let pkg = d
        .ancestors()
        .find_map(|a| a.children().find_map(|c| c.child_sym(C::Package)));
    let arity = d.child(C::Partial)?.children().count();
    Some((pkg.unwrap_or(0), d.child_sym(C::DefName)?, arity))
}

type Members = (
    FxHashMap<(u32, u32, usize), Vec<Loc>>,
    FxHashMap<u32, Vec<Loc>>,
);

fn gather_members(trees: &[Tree]) -> Members {
    let (mut parts, mut extensions) = Members::default();
    for (fi, tree) in trees.iter().enumerate() {
        let defs = tree
            .root()
            .descendants_pruned(|n| n.is(C::Def) && !n.has(C::ImplBlock))
            .filter(|d| d.is(C::Def));
        for d in defs {
            let loc = Loc::new(fi, d.index());
            if let Some(key) = partial_key(d) {
                parts.entry(key).or_default().push(loc);
            }
            let wrapped = d
                .parent()
                .is_some_and(|p| p.is(C::Def) && p.has(C::ImplBlock));
            if let Some(name) = d.child_sym(C::DefName).filter(|_| wrapped) {
                extensions.entry(name).or_default().push(loc);
            }
        }
    }
    (parts, extensions)
}

fn supertypes<'a>(ctx: &'a ResolveCtx, cls: Cursor<'a>) -> Vec<(u32, u32)> {
    cls.children()
        .filter(|s| s.is(C::SuperType))
        .filter_map(|s| resolve_chain(ctx, s))
        .map(|c| (c.fi(), c.index()))
        .chain(
            ctx.edges_by_tree[cls.fi() as usize]
                .iter()
                .copied()
                .chain(ctx.extends)
                .filter(|e| {
                    e.kind == EdgeKind::Extends
                        && e.from_tree == cls.fi()
                        && e.from_node == cls.index()
                })
                .map(|e| (e.to_tree, e.to_node)),
        )
        .collect()
}

fn lub<'a>(
    ctx: &'a ResolveCtx,
    classes: impl Iterator<Item = Option<Cursor<'a>>>,
) -> Option<Cursor<'a>> {
    let ancestors = |id: (u32, u32)| {
        reachable(id, |(fi, n)| supertypes(ctx, ctx.corpus.jump(fi, n))).collect::<FxHashSet<_>>()
    };
    let mut sets = classes.map(|c| {
        c.filter(|t| t.is_class())
            .map(|t| ancestors((t.fi(), t.index())))
    });
    let mut common = sets.next()??;
    for set in sets {
        common.retain(|t| set.as_ref().is_some_and(|a| a.contains(t)));
    }
    let mut least = common
        .iter()
        .filter(|t| !common.iter().any(|u| u != *t && ancestors(*u).contains(*t)));
    let &(fi, n) = least.next()?;
    least.next().is_none().then(|| ctx.corpus.jump(fi, n))
}

fn branch_type<'a>(ctx: &'a ResolveCtx, branch: Cursor<'a>) -> Option<Cursor<'a>> {
    let arms = branch
        .children()
        .filter(|a| a.is(C::SsaArm))
        .filter_map(|a| {
            let tail = a.tail_expr();
            if tail.is(C::SsaReturn)
                || tail.is(C::SsaBranch) && !tail.children().any(|c| c.is(C::SsaArm))
            {
                return None;
            }
            Some(if tail.is(C::SsaBranch) {
                branch_type(ctx, tail)
            } else {
                resolve_chain(ctx, tail.child(C::Callee)?)
            })
        });
    lub(ctx, arms)
}

fn method_up<'a>(ctx: &'a ResolveCtx, cls: Cursor<'a>, name: u32, fi: usize) -> Vec<Cursor<'a>> {
    let jump = |(sf, n): (u32, u32)| ctx.corpus.jump(sf, n);
    let inherited = members_by_level(
        vec![(cls.fi(), cls.index())],
        |id| supertypes(ctx, jump(id)),
        |id| declared_member(ctx, jump(id), name).map(|m| (m.fi(), m.index())),
    );
    if !inherited.is_empty() {
        return inherited.into_iter().map(jump).collect();
    }
    extension_members(ctx, Owner::Class(cls), name, fi)
}

fn extension_members<'a>(
    ctx: &'a ResolveCtx,
    owner: Owner<'a>,
    name: u32,
    fi: usize,
) -> Vec<Cursor<'a>> {
    let extends_owner = |method: Cursor<'a>| {
        let receiver = method
            .enclosing_def(&[C::ImplBlock])?
            .child_sym(C::DefName)?;
        let same = match owner {
            Owner::Class(cls) => {
                let target = visible_type(ctx, method.fi(), receiver)?;
                (target.fi(), target.index()) == (cls.fi(), cls.index())
            }
            Owner::External(id) => {
                visible_type(ctx, method.fi(), receiver).is_none()
                    && ctx.imports[method.fi() as usize].get(&receiver) == Some(&id)
            }
        };
        same.then_some(method)
    };
    let candidates = ctx.extensions.get(&name).into_iter().flatten();
    candidates
        .filter(|l| l.fi == fi || ctx.exporters[fi].contains(&l.fi))
        .filter_map(|l| extends_owner(ctx.corpus.jump(l.fi as u32, l.node)))
        .collect()
}

fn declared_member<'a>(ctx: &'a ResolveCtx, cls: Cursor<'a>, name: u32) -> Option<Cursor<'a>> {
    let parts = partial_key(cls).and_then(|k| ctx.partials.get(&k));
    let parts = parts
        .into_iter()
        .flatten()
        .map(|l| cls.jump(l.fi as u32, l.node));
    std::iter::once(cls)
        .chain(cls.jump(cls.fi(), 0).descendants().filter(|d| {
            d.is(C::Def)
                && (d.has(C::ImplBlock) || cls.has(C::ImplBlock))
                && d.child_sym(C::DefName) == cls.child_sym(C::DefName)
        }))
        .chain(parts.filter(|d| (d.fi(), d.index()) != (cls.fi(), cls.index())))
        .find_map(|b| find_method_in(b, name))
}

fn resolve_receivers(ctx: &ResolveCtx, fi: usize) -> Vec<Edge> {
    let mut out = Vec::new();
    let root = ctx.corpus.jump(fi as u32, 0);
    let local = |n: Cursor| {
        n.child_sym(C::Alias)
            .or(n.child_sym(C::SsaHint))
            .unwrap_or(n.sym())
    };
    let imports = root
        .descendants()
        .filter(|c| c.is(C::Import))
        .flat_map(|i| i.names());
    let (wild, named): (Vec<Cursor>, Vec<Cursor>) =
        imports.partition(|n| local(*n) == ctx.wildcard_sym);
    let wild: Vec<u32> = wild.iter().map(|n| n.index()).collect();
    let named: FxHashSet<u32> = named.iter().map(|n| local(*n)).collect();
    let unbound = |from: Cursor, sym: u32| -> Vec<Edge> {
        let wild = wild.iter().filter(|_| !named.contains(&sym));
        wild.map(|&w| from.edge_to(from.jump(fi as u32, w), EdgeKind::Imports))
            .collect()
    };
    for d in root.descendants().filter(|d| d.is(C::Def)) {
        for dec in d.children_of(C::Decorator) {
            match resolve_chain(ctx, dec) {
                Some(t) if t.fi() != fi as u32 => out.push(d.edge_to(t, EdgeKind::Calls)),
                Some(_) => {}
                None => out.extend(unbound(d, dec.sym())),
            }
        }
    }
    for d in root.descendants().filter(|d| d.is(C::Destructure)) {
        let class = d
            .child(C::Call)
            .and_then(|c| c.child(C::Callee))
            .and_then(|k| resolve_chain(ctx, k));
        let Some((from, class)) = d.enclosing(|e| e.is(C::Def)).zip(class) else {
            continue;
        };
        let slots: Vec<Cursor> = d
            .children()
            .filter(|s| s.is(C::Binding) || s.is(C::Destructure))
            .collect();
        let Some(positional) = class
            .child(C::Positional)
            .filter(|p| p.children().count() == slots.len())
        else {
            continue;
        };
        for (slot, component) in slots.iter().zip(positional.children()) {
            let members = method_up(ctx, class, component.sym(), fi);
            out.extend(call_edges(from, members, Some(slot.index())));
        }
    }
    for (call, m) in root.member_calls() {
        let Some(from) = call.enclosing(|c| c.is(C::Def)) else {
            continue;
        };
        let bound = |s: u32| from.any_desc(|b| b.is(C::Binding) && b.sym_opt() == Some(s));
        let Some(object) = m.child(C::Object) else {
            continue;
        };
        let Some(obj) = object.reference().chain_root().sym_opt() else {
            continue;
        };
        match resolve_chain(ctx, object) {
            Some(target) if target.fi() != fi as u32 && target.is_class() => {
                let members = method_up(ctx, target, m.sym(), fi);
                out.extend(call_edges(from, members, Some(call.index())));
            }
            None if !bound(obj) => out.extend(unbound(from, obj)),
            _ => {}
        }
    }
    out
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
    let prefixed = prefixes.iter().map(|p| match p.is_empty() {
        true => target.to_string(),
        false => format!("{p}{PATH_SEP}{target}"),
    });
    std::iter::once(target.to_string())
        .chain(prefixed)
        .find_map(|key| file_index.get(&key))
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
    file_index.get(&format!("{dir}{PATH_SEP}{name}"))
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
