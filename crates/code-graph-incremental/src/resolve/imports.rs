use std::borrow::Cow;

use either::Either;
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;

use super::{Ctx, Tree, VisibleMap, partial_key};
use crate::canonical::Canonical as C;
use crate::constants::{PATH_SEP, WILDCARD};
use crate::engine::Cx;
use crate::engine::Loc;
use crate::file_tree::LookupConfig;
use crate::intern::Lang;
use crate::rules::ResolveConfig;
use crate::sentinel::{Killed, Sentinel};
use crate::tags::ReservedTags;
use crate::tree::{CallResolution, Compact, Edge, EdgeKind};
use crate::treesitter::SupportLang;

type Cursor<'a> = crate::tree::Cursor<'a, Compact>;
const PROPAGATE_CHECK_INTERVAL: usize = 100_000;

#[derive(Default)]
pub struct FileIndex {
    pub(super) keys: FxHashMap<String, Loc>,
    dirs: FxHashMap<String, Vec<u32>>,
}

impl FileIndex {
    pub(super) fn insert(&mut self, key: String, loc: Loc) {
        let dir = key.rsplit_once(PATH_SEP).map_or("", |(d, _)| d);
        self.dirs.entry(dir.to_string()).or_default().push(loc.fi);
        self.keys.entry(key).or_insert(loc);
    }

    pub(super) fn get(&self, key: &str) -> Option<Loc> {
        self.keys.get(key).copied()
    }

    pub(super) fn insert_entrypoints(&mut self, entrypoints: &[(String, String)]) {
        for (directory, path) in entrypoints {
            if let Some(target) = self.get(path) {
                self.keys.insert(directory.clone(), target);
            }
        }
    }
}

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ImportReq {
    pub fi: u32,
    pub node: u32,
    pub target_fi: u32,
    pub anchor: u32,
    pub target_path: String,
}

impl ImportReq {
    pub(super) fn exports<'a>(
        &self,
        trees: &[Tree],
        visible: &'a VisibleMap,
    ) -> Cow<'a, FxHashMap<u32, Loc>> {
        if self.anchor == 0 {
            return Cow::Borrowed(&visible[self.target_fi as usize]);
        }
        let mut names = visible[self.target_fi as usize].clone();
        let module = trees[self.target_fi as usize].cursor(self.anchor);
        for d in module
            .children()
            .filter(|d| d.is(C::Def) && !d.has(C::ImplBlock))
        {
            if let Some(name) = d.child_sym(C::DefName) {
                names.insert(
                    name,
                    Loc {
                        fi: self.target_fi,
                        node: d.index(),
                    },
                );
            }
        }
        Cow::Owned(names)
    }
}

pub(super) fn gather_visible_one(tree: &Tree, fi: usize, exports_key: u32) -> FxHashMap<u32, Loc> {
    let mut names = tree.root().fold_tree(FxHashMap::default(), |names, c, _w| {
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
    });
    for export in tree
        .root()
        .children()
        .filter(|node| node.is(C::ModuleExport))
    {
        let bindings = export
            .names()
            .map(|name| (name.sym(), name.child_sym(C::Alias).unwrap_or(name.sym())))
            .chain(
                export
                    .child_sym(C::DefaultExport)
                    .map(|alias| (export.sym(), alias)),
            );
        for (name, alias) in bindings {
            if let Some(target) = tree
                .root()
                .children()
                .find(|node| node.is(C::Def) && node.child_sym(C::DefName) == Some(name))
            {
                names.insert(alias, Loc::new(fi, target.index()));
            }
        }
    }
    names
}

pub(super) fn gather_imports_for(
    trees: &[Tree],
    lang: &Lang,
    file_index: &FileIndex,
    lookup: &LookupConfig,
    config: &ResolveConfig,
    dirty_fis: &FxHashSet<u32>,
) -> (Vec<ImportReq>, Vec<Edge>) {
    let tags = ReservedTags::new(lang);
    let resolved_tag_key = tags.resolved_source;
    let root_relative = lang.syms.lookup("source_root_rel");
    let declared_roots: FxHashSet<_> = trees
        .iter()
        .filter_map(|tree| {
            let relative = lang.syms.resolve(tree.root().tag(root_relative)?);
            tree.label
                .strip_suffix(relative)
                .map(|root| root.trim_end_matches(PATH_SEP))
        })
        .collect();
    let stdlib_prefixes: Vec<_> = lookup
        .prefixes
        .iter()
        .filter(|prefix| declared_roots.contains(prefix.as_str()))
        .cloned()
        .collect();
    let dirty_vec: Vec<u32> = dirty_fis.iter().copied().collect();
    let per_tree: Vec<(Vec<ImportReq>, Vec<Edge>)> = dirty_vec
        .par_iter()
        .map(|&fi| {
            let tree = &trees[fi as usize];
            tree.root()
                .fold_tree((Vec::new(), Vec::new()), |(reqs, edges), cur, _w| {
                    if cur.kind() != C::Import && cur.kind() != C::ImportType {
                        return;
                    }
                    let Some(source_sym) = cur
                        .tag(tags.original_source_path)
                        .or_else(|| cur.child_sym(C::SourcePath))
                    else {
                        return;
                    };
                    let source_str = lang.syms.resolve(source_sym);
                    let Some(resolved_sym) = tree.get_tag(cur.index(), resolved_tag_key) else {
                        return;
                    };
                    let raw_path = lang.syms.resolve(resolved_sym);
                    let mapped = lookup.aliases.iter().find_map(|alias| {
                        if alias.scope.is_some() && alias.scope != cur.tag(tags.alias_scope) {
                            return None;
                        }
                        let path = apply_alias(source_str, &alias.pattern, &alias.replacement)?;
                        (!alias.if_exists || !resolve_glob(&path, file_index, &[]).is_empty())
                            .then_some(path)
                    });
                    if mapped.is_none() && is_external(source_str, &config.external) {
                        return;
                    }
                    let node_idx = cur.index();
                    let stdlib = mapped.is_none() && is_external(source_str, &config.stdlib);
                    let prefixes = if stdlib {
                        &stdlib_prefixes
                    } else {
                        &lookup.prefixes
                    };
                    let target_path = mapped.unwrap_or_else(|| raw_path.to_owned());
                    let mut direct = resolve_glob(&target_path, file_index, prefixes);
                    direct.retain(|loc| loc.fi != fi);
                    if direct.is_empty() && stdlib {
                        return;
                    }
                    let candidates = match direct.is_empty() {
                        false => Either::Left(
                            direct
                                .into_iter()
                                .map(|loc| (loc, target_path.clone(), false)),
                        ),
                        true => Either::Right(cur.names().flat_map(|c| {
                            let submod =
                                format!("{target_path}{PATH_SEP}{}", lang.syms.resolve(c.sym()));
                            resolve_glob(&submod, file_index, &lookup.prefixes)
                                .into_iter()
                                .map(move |loc| (loc, submod.clone(), true))
                        })),
                    };
                    for (loc, path, is_sub) in candidates.filter(|c| c.0.fi != fi) {
                        if is_sub {
                            edges.push(Edge::new(fi, node_idx, loc.fi, 0, EdgeKind::Imports));
                        }
                        reqs.push(ImportReq {
                            fi,
                            node: node_idx,
                            target_fi: loc.fi,
                            anchor: loc.node,
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

#[allow(clippy::too_many_arguments)]
pub(super) fn propagate_reexports(
    trees: &[Tree],
    reqs: &[ImportReq],
    visible: &mut VisibleMap,
    wildcard_sym: u32,
    tags: ReservedTags,
    owners: &[FxHashMap<u32, u32>],
    run: &Sentinel,
    max_names: usize,
) -> (FxHashSet<(u32, u32)>, Option<Killed>) {
    let mut ambiguous: FxHashSet<(u32, u32)> = FxHashSet::default();
    let mut names: usize = visible.iter().map(FxHashMap::len).sum();
    let mut too_many_names = None;
    let key = |l: Loc| partial_key(trees[l.fi as usize].cursor(l.node), owners);

    let mut visible_from_directives = Vec::new();
    let mut declared = vec![Vec::new(); trees.len()];
    for (fi, tree) in trees.iter().enumerate() {
        for c in tree.root().descendants() {
            if let Some(source_sym) = c.tag(tags.visible_from) {
                visible_from_directives.push((fi, source_sym));
            }
            if let Some(name) = c.tag(tags.exports) {
                declared[fi].push(name);
            }
        }
    }
    let (via, packages) = package_imports(trees, reqs, wildcard_sym);
    'propagate: loop {
        if run.check().is_err() {
            break;
        }
        let mut unions: Vec<Option<PackageExports>> = vec![None; packages.len()];
        let mut new_exports: indexmap::IndexMap<(usize, u32), Loc, rustc_hash::FxBuildHasher> =
            Default::default();
        let added = std::cell::Cell::new(0);
        let mut export = |fi: usize, name, loc: Loc| {
            if visible[fi].contains_key(&name) {
                return;
            }
            match new_exports.entry((fi, name)) {
                indexmap::map::Entry::Vacant(entry) => {
                    entry.insert(loc);
                    added.set(added.get() + 1);
                }
                indexmap::map::Entry::Occupied(entry) => {
                    let existing = *entry.get();
                    if existing != loc && (key(existing).is_none() || key(existing) != key(loc)) {
                        ambiguous
                            .insert((u32::try_from(fi).expect("file index exceeds u32"), name));
                    }
                }
            }
        };
        for (i, req) in reqs.iter().enumerate() {
            if i % PROPAGATE_CHECK_INTERVAL == 0 && run.check().is_err() {
                break 'propagate;
            }
            if max_names > 0 && names + added.get() > max_names {
                too_many_names = Some(Killed::limit("names", "", names + added.get(), max_names));
                break 'propagate;
            }
            match via[i] {
                Via::Own => {}
                Via::Package(package) => {
                    let union = unions[package]
                        .get_or_insert_with(|| package_exports(&packages[package], visible, trees));
                    for (&name, locs) in union.iter() {
                        for &loc in locs {
                            export(req.fi as usize, name, loc);
                        }
                    }
                    continue;
                }
                Via::Covered => continue,
            }
            let exports = req.exports(trees, visible);
            for c in trees[req.fi as usize].cursor(req.node).names() {
                let ns = wildcard_or_name(wildcard_sym, c);
                if ns == wildcard_sym {
                    if let Some(alias) = namespace_alias(wildcard_sym, c)
                        && c.ancestors().any(|node| node.is(C::ModuleExport))
                    {
                        export(
                            req.fi as usize,
                            alias,
                            Loc::new(req.target_fi as usize, req.anchor),
                        );
                        continue;
                    }
                    for (&ds, &loc) in exports.iter() {
                        export(req.fi as usize, ds, loc);
                    }
                } else if let Some(&loc) = exports.get(&ns) {
                    export(req.fi as usize, c.child_sym(C::Alias).unwrap_or(ns), loc);
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
                let source_fi = loc.fi as usize;
                for (&ds, &dloc) in &visible[source_fi] {
                    export(fi, ds, dloc);
                }
            }
        }

        for req in reqs {
            for &n in &declared[req.target_fi as usize] {
                let own = visible[req.fi as usize].get(&n).filter(|l| l.fi == req.fi);
                if let Some(&loc) = own {
                    export(req.target_fi as usize, n, loc);
                }
            }
        }
        if new_exports.is_empty() {
            break;
        }
        names += new_exports.len();
        for ((fi, ns), loc) in new_exports {
            visible[fi].insert(ns, loc);
        }
    }
    (ambiguous, too_many_names)
}

pub(super) type PackageExports =
    indexmap::IndexMap<u32, SmallVec<[Loc; 1]>, rustc_hash::FxBuildHasher>;

#[derive(Clone, Copy)]
pub(super) enum Via {
    Own,
    Package(usize),
    Covered,
}

pub(super) fn package_imports(
    trees: &[Tree],
    reqs: &[ImportReq],
    wildcard_sym: u32,
) -> (Vec<Via>, Vec<Vec<u32>>) {
    let wildcard_only = |req: &ImportReq| {
        let mut names = trees[req.fi as usize].cursor(req.node).names();
        let only = names.next().filter(|c| {
            wildcard_or_name(wildcard_sym, *c) == wildcard_sym
                && !(namespace_alias(wildcard_sym, *c).is_some()
                    && c.ancestors().any(|node| node.is(C::ModuleExport)))
        });
        req.anchor == 0 && only.is_some() && names.next().is_none()
    };
    let mut groups: Vec<std::ops::Range<usize>> = Vec::new();
    for (i, req) in reqs.iter().enumerate() {
        if !wildcard_only(req) {
            continue;
        }
        match groups.last_mut() {
            Some(group)
                if group.end == i
                    && (reqs[group.start].fi, reqs[group.start].node) == (req.fi, req.node) =>
            {
                group.end = i + 1;
            }
            _ => groups.push(i..i + 1),
        }
    }
    let mut members: FxHashMap<&str, Vec<u32>> = FxHashMap::default();
    for group in &groups {
        let path = reqs[group.start].target_path.as_str();
        members
            .entry(path)
            .or_default()
            .extend(reqs[group.clone()].iter().map(|r| r.target_fi));
    }
    for files in members.values_mut() {
        files.sort_unstable();
        files.dedup();
    }
    let mut via = vec![Via::Own; reqs.len()];
    let mut ids: FxHashMap<&str, usize> = FxHashMap::default();
    let mut packages = Vec::new();
    for group in groups {
        let first = &reqs[group.start];
        let package = &members[first.target_path.as_str()];
        let targets = reqs[group.clone()].iter().map(|r| r.target_fi);
        if !targets.eq(package.iter().copied().filter(|&f| f != first.fi)) {
            continue;
        }
        let id = *ids.entry(first.target_path.as_str()).or_insert_with(|| {
            packages.push(package.clone());
            packages.len() - 1
        });
        via[group.start] = Via::Package(id);
        for v in &mut via[group.start + 1..group.end] {
            *v = Via::Covered;
        }
    }
    (via, packages)
}

pub(super) fn package_exports(
    files: &[u32],
    visible: &VisibleMap,
    trees: &[Tree],
) -> PackageExports {
    let member = |loc: Loc| {
        let def = trees[loc.fi as usize].cursor(loc.node);
        !def.is_class()
            && def
                .enclosing(|p| p.is(C::Def))
                .is_some_and(|p| p.is_class())
    };
    let mut union = PackageExports::default();
    for &fi in files {
        for (&name, &loc) in &visible[fi as usize] {
            if member(loc) {
                continue;
            }
            let locs = union.entry(name).or_default();
            if !locs.contains(&loc) {
                locs.push(loc);
            }
        }
    }
    union
}

pub(super) fn name_target(ctx: &Ctx, req: &ImportReq, c: Cursor) -> Option<Loc> {
    let tfi = req.target_fi as usize;
    let exports = req.exports(ctx.trees, ctx.visible);
    let ns = wildcard_or_name(ctx.wildcard_sym, c);
    if ns == ctx.wildcard_sym {
        return Some(Loc::new(tfi, req.anchor));
    }
    if ctx.ambiguous.contains(&(req.target_fi, ns)) {
        return None;
    }
    if let Some(&loc) = exports.get(&ns) {
        return Some(loc);
    }
    if req.anchor != 0 {
        return None;
    }
    let target_path = ctx.lang.syms.resolve(ctx.corpus.jump(tfi as u32, 0).sym());
    resolve_submodule(
        target_path,
        ctx.lang.syms.resolve(ns),
        ctx.support_lang,
        ctx.index_names,
        ctx.file_index,
    )
    .map(|fi| Loc::new(fi, 0))
}

pub(super) fn resolve_one_import(ctx: &Ctx, req: &ImportReq) -> Vec<Edge> {
    let (fi, tfi) = (req.fi as usize, req.target_fi as usize);
    let import = ctx.corpus.jump(req.fi, req.node);

    let import_edges: Vec<Edge> = import
        .names()
        .flat_map(|c| {
            name_target(ctx, req, c)
                .into_iter()
                .filter(move |loc| loc.fi as usize != fi)
                .map(move |loc| c.edge_to(c.jump(loc.fi, loc.node), EdgeKind::Imports))
        })
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
        let Some(name) = import
            .names()
            .find(|n| m.child_sym(C::Object) == Some(local(*n)))
        else {
            continue;
        };
        let exports = req.exports(ctx.trees, ctx.visible);
        let target_file = name_target(ctx, req, name)
            .filter(|target| target.node == 0)
            .map_or(tfi, |target| target.fi as usize);
        let target = if target_file == tfi {
            exports.get(&m.sym())
        } else {
            ctx.visible[target_file].get(&m.sym())
        }
        .map(|loc| ctx.corpus.jump(loc.fi, loc.node))
        .filter(|tgt| tgt.has_tag(ctx.tags.callable));
        if let Some(target) = target {
            if namespace_alias(ctx.wildcard_sym, name).is_some() {
                edges.push(name.edge_to(target, EdgeKind::Imports));
            }
            edges.push(call_edge(caller, target, edge.site));
        }
    }

    for ie in &import_edges {
        let target = ctx.corpus.follow(ie);
        let name = ctx.corpus.jump(ie.from_tree, ie.from_node);
        if wildcard_or_name(ctx.wildcard_sym, name) == ctx.wildcard_sym
            || !target.has_tag(ctx.tags.callable)
        {
            continue;
        }
        for intra in ctx.imports_to(ie.from_fi(), ie.from_node) {
            if intra.call_resolution == CallResolution::Reference
                && intra.site.is_some_and(|site| {
                    let binding = ctx.corpus.jump(intra.from_tree, site);
                    binding.is(C::Binding) && binding.bare_rhs().is_some()
                })
            {
                continue;
            }
            let from = ctx.corpus.jump(ie.from_tree, intra.from_node);
            if intra.site.is_some_and(|site| {
                ctx.corpus
                    .jump(intra.from_tree, site)
                    .member()
                    .is_some_and(|member| {
                        member
                            .child(C::Dispatch)
                            .is_some_and(|dispatch| !dispatch.has(C::Object))
                            || member.has(C::Dispatch)
                                && target.enclosing(Cursor::is_dispatch_contract).is_some()
                    })
            }) {
                continue;
            }
            edges.push(call_edge(from, target, intra.site));
        }
    }

    edges
}

pub(super) fn wildcard_uses<'a>(
    r: &mut Cx<'a>,
    req: &ImportReq,
    name: Cursor<'a>,
    referrers: &FxHashSet<u32>,
) -> Vec<Edge> {
    let ctx = r.family.expect("a family phase");
    if namespace_alias(ctx.wildcard_sym, name).is_some() {
        return Vec::new();
    }
    let fi = req.fi as usize;
    let exports = req.exports(ctx.trees, ctx.visible);
    let provided = |sym: u32| {
        exports
            .get(&sym)
            .filter(|_| !ctx.ambiguous.contains(&(req.target_fi, sym)))
            .filter(|loc| loc.fi as usize != fi)
            .map(|loc| ctx.corpus.jump(loc.fi, loc.node))
    };
    let root = |r: &mut Cx<'a>, c: Cursor<'a>| r.qualified(c.sym(), &|_, sym| provided(sym));
    let mut imported: FxHashSet<Loc> = FxHashSet::default();
    let mut edges = Vec::new();
    let mut import = |target: Cursor, edges: &mut Vec<Edge>| {
        if imported.insert(target.into()) {
            edges.push(name.edge_to(target, EdgeKind::Imports));
        }
    };
    for from in referrers.iter().map(|&n| ctx.corpus.jump(fi as u32, n)) {
        for call in from.calls() {
            let Some(target) = call.child(C::Callee).and_then(|c| r.chain_from(c, &root)) else {
                continue;
            };
            import(target, &mut edges);
            if target.has_tag(ctx.tags.callable) {
                edges.push(call_edge(from, target, Some(call.index())));
            }
        }
        let headers = from
            .children_of(C::SuperType)
            .chain(from.children_of(C::Decorator));
        for target in headers.filter_map(|h| r.chain_from(h, &root)) {
            import(target, &mut edges);
        }
    }
    edges
}

pub(super) fn namespace_alias(wildcard_sym: u32, name: Cursor) -> Option<u32> {
    name.child_sym(C::Alias)
        .filter(|_| wildcard_or_name(wildcard_sym, name) == wildcard_sym)
}

pub(super) fn wildcard_or_name(wildcard_sym: u32, name: Cursor) -> u32 {
    name.child_sym(C::SsaHint)
        .filter(|&h| h == wildcard_sym)
        .unwrap_or(name.sym())
}

pub(super) fn call_edge(from: Cursor, target: Cursor, site: Option<u32>) -> Edge {
    Edge {
        site,
        ..from.edge_to(target, EdgeKind::Calls)
    }
}

pub(super) fn build_file_index(
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
        let file = Loc::new(fi, 0);
        idx.insert(path.to_string(), file);
        idx.insert(stem.to_string(), file);
        for name in index_names {
            if let Some(pkg) = stem
                .strip_suffix(name.as_str())
                .and_then(|s| s.strip_suffix(PATH_SEP))
            {
                if !pkg.is_empty() {
                    idx.insert(pkg.to_string(), file);
                }
            } else if stem == name.as_str() {
                idx.insert(String::new(), file);
            }
        }
        let root = tree.root();
        let pkg = root.child_sym(C::Package).map_or(String::new(), |s| {
            lang.syms.resolve(s).replace(sep, PATH_SEP) + PATH_SEP
        });
        let defs = root.children().filter(|d| d.is(C::Def));
        for (name, d) in defs.filter_map(|d| Some((d.child_sym(C::DefName)?, d))) {
            let key = format!("{pkg}{}", lang.syms.resolve(name).replace(sep, PATH_SEP));
            idx.insert(key, Loc::new(fi, d.index()));
        }
    }
    idx
}

pub(super) fn resolve_glob(target: &str, idx: &FileIndex, prefixes: &[String]) -> Vec<Loc> {
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
        .unique()
        .map(|&fi| Loc { fi, node: 0 })
        .collect()
}

pub(super) fn is_external(source_str: &str, external: &[String]) -> bool {
    external.iter().any(|module| {
        source_str == module
            || source_str
                .strip_prefix(module.as_str())
                .is_some_and(|suffix| suffix.starts_with(PATH_SEP))
    })
}

pub(super) fn resolve_path(
    target: &str,
    file_index: &FileIndex,
    prefixes: &[String],
) -> Option<Loc> {
    file_index.get(target).or_else(|| {
        let mut key = String::new();
        prefixes
            .iter()
            .filter(|prefix| !prefix.is_empty())
            .find_map(|prefix| {
                key.clear();
                key.push_str(prefix);
                key.push_str(PATH_SEP);
                key.push_str(target);
                file_index.get(&key)
            })
    })
}

pub(super) fn resolve_submodule(
    target_path: &str,
    name: &str,
    support_lang: SupportLang,
    index_names: &[String],
    file_index: &FileIndex,
) -> Option<usize> {
    let stem = SupportLang::from_path(target_path)
        .unwrap_or(support_lang)
        .strip_extension(target_path);
    let dir = index_names.iter().find_map(|idx| {
        stem.strip_suffix(idx.as_str())
            .and_then(|s| s.strip_suffix(PATH_SEP))
    })?;
    file_index
        .get(&format!("{dir}{PATH_SEP}{name}"))
        .map(|loc| loc.fi as usize)
}

pub(super) fn apply_alias(path: &str, pattern: &str, replacement: &str) -> Option<String> {
    if let (Some(prefix), Some(target)) = (pattern.strip_suffix('*'), replacement.strip_suffix('*'))
    {
        return path
            .strip_prefix(prefix)
            .map(|rest| format!("{target}{rest}"));
    }
    let rest = path.strip_prefix(pattern)?;
    (rest.is_empty() || rest.starts_with(PATH_SEP)).then(|| format!("{replacement}{rest}"))
}

#[cfg(test)]
mod alias_tests {
    use super::apply_alias;

    #[test]
    fn an_alias_matches_whole_path_components_only() {
        assert_eq!(
            apply_alias("app", "app", "src/app").as_deref(),
            Some("src/app")
        );
        assert_eq!(
            apply_alias("app/foo", "app", "src/app").as_deref(),
            Some("src/app/foo")
        );
        assert_eq!(apply_alias("application/foo", "app", "src/app"), None);
    }
}
