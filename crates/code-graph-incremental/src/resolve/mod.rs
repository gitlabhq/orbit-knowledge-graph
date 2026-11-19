mod imports;
mod types;

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use either::Either;
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

pub use imports::{FileIndex, ImportReq};
use imports::{
    build_file_index, gather_imports_for, gather_visible_one, name_target, propagate_reexports,
    resolve_glob, resolve_one_import, wildcard_or_name, wildcard_uses,
};
pub use types::Fun;
use types::R;

use crate::canonical::Canonical as C;
use crate::constants::WILDCARD;
use crate::env::Env;
use crate::file_tree::{LookupConfig, WalkResult};
use crate::intern::Lang;
use crate::rules::{Loc, Members, MemoMap};
use crate::sentinel::{Killed, Sentinel};
use crate::tags::ReservedTags;
use crate::tree::{CallResolution, Compact, Edge, EdgeKind, reachable};
use crate::treesitter::SupportLang;

type Tree = crate::tree::Tree<Compact>;
type Cursor<'a> = crate::tree::Cursor<'a, Compact>;

pub const CLASS_LIKE: &[C] = &[
    C::Class,
    C::Struct,
    C::ImplBlock,
    C::Interface,
    C::Trait,
    C::Enum,
];

pub(super) type VisibleMap = Vec<FxHashMap<u32, Loc>>;

pub struct ResolvedSourcePath {
    pub fi: u32,
    pub node: u32,
    pub sym: u32,
}

pub struct ResolveResult {
    pub cross_edges: Vec<Edge>,
    pub killed: Vec<Killed>,
    pub resolved_source_paths: Vec<ResolvedSourcePath>,
    pub file_timings: Vec<(u32, Duration)>,
}

pub struct Resolver {
    pub(crate) lookup: LookupConfig,
    visible: VisibleMap,
    reqs: Vec<ImportReq>,
    file_index: FileIndex,
    wildcard_sym: u32,
    tags: ReservedTags,
}

impl Resolver {
    pub fn new(lang: &Lang) -> Self {
        Self {
            lookup: LookupConfig::default(),
            visible: Vec::new(),
            reqs: Vec::new(),
            file_index: FileIndex::default(),
            wildcard_sym: lang.syms.intern(WILDCARD),
            tags: ReservedTags::new(lang),
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

    pub(crate) fn rebuild_file_index(
        &mut self,
        trees: &[Tree],
        env: &Env,
        entrypoints: &[(String, String)],
    ) -> FileIndex {
        let mut index = build_file_index(trees, &env.lang, env.lang_id, env.lang_id.index_names());
        index.insert_entrypoints(entrypoints);
        std::mem::replace(&mut self.file_index, index)
    }

    fn invalidate(
        &self,
        previous_index: &FileIndex,
        trees: &[Tree],
        dirty_fis: &FxHashSet<u32>,
        lookup: &LookupConfig,
        env: &Env,
    ) -> FxHashSet<u32> {
        if self.visible.is_empty() {
            return (0..trees.len() as u32).collect();
        }
        let mut invalidated: FxHashSet<u32> = self
            .reqs
            .par_iter()
            .filter(|req| {
                self.lookup != *lookup
                    || !resolve_glob(&req.target_path, &self.file_index, &lookup.prefixes)
                        .contains(&Loc::new(req.target_fi as usize, req.anchor))
            })
            .map(|req| req.fi)
            .collect();
        invalidated.extend(dirty_fis);
        let added_keys = self
            .file_index
            .keys
            .keys()
            .any(|key| !previous_index.keys.contains_key(key));
        if added_keys || self.lookup != *lookup {
            let known_imports: FxHashSet<_> =
                self.reqs.iter().map(|req| (req.fi, req.node)).collect();
            let discovery_files = (0..trees.len() as u32)
                .filter(|fi| !dirty_fis.contains(fi))
                .collect();
            let (discovered, _) = gather_imports_for(
                trees,
                &env.lang,
                &self.file_index,
                lookup,
                &env.resolve.config,
                &discovery_files,
            );
            invalidated.extend(
                discovered
                    .iter()
                    .filter(|req| !known_imports.contains(&(req.fi, req.node)))
                    .map(|req| req.fi),
            );
        }
        with_dependents(&self.reqs, invalidated)
    }

    pub fn remap(
        &mut self,
        old_labels: &[String],
        label_to_fi: &FxHashMap<&str, u32>,
    ) -> FxHashSet<u32> {
        if self.visible.is_empty() {
            return label_to_fi.values().copied().collect();
        }
        let n = label_to_fi.len();
        let mut remapped: VisibleMap = (0..n)
            .map(|_| FxHashMap::with_capacity_and_hasher(16, Default::default()))
            .collect();
        let mut lost_a_name = FxHashSet::default();
        for (old_fi, names) in self.visible.iter().enumerate() {
            let Some(&new_fi) = old_labels
                .get(old_fi)
                .and_then(|l| label_to_fi.get(l.as_str()))
            else {
                continue;
            };
            for (&sym, &loc) in names {
                match old_labels
                    .get(loc.fi as usize)
                    .and_then(|l| label_to_fi.get(l.as_str()))
                {
                    Some(&target_new_fi) => {
                        remapped[new_fi as usize]
                            .insert(sym, Loc::new(target_new_fi as usize, loc.node));
                    }
                    None => {
                        lost_a_name.insert(new_fi);
                    }
                }
            }
        }
        self.visible = remapped;

        self.reqs = self
            .reqs
            .iter()
            .filter_map(|r| {
                let new_fi = *old_labels
                    .get(r.fi as usize)
                    .and_then(|l| label_to_fi.get(l.as_str()))?;
                let new_tfi = *old_labels
                    .get(r.target_fi as usize)
                    .and_then(|l| label_to_fi.get(l.as_str()))?;
                Some(ImportReq {
                    fi: new_fi,
                    node: r.node,
                    target_fi: new_tfi,
                    anchor: r.anchor,
                    target_path: r.target_path.clone(),
                })
            })
            .collect();
        lost_a_name
    }

    fn importers_of_changed_names(
        &self,
        previous: &VisibleMap,
        dirty: &FxHashSet<u32>,
    ) -> FxHashSet<u32> {
        let none = FxHashMap::default();
        let changed = self
            .visible
            .iter()
            .enumerate()
            .filter(|(fi, names)| previous.get(*fi).unwrap_or(&none) != *names)
            .map(|(fi, _)| u32::try_from(fi).expect("file index exceeds u32"))
            .collect();
        with_dependents(&self.reqs, changed)
            .difference(dirty)
            .copied()
            .collect()
    }

    pub fn resolve(
        &mut self,
        trees: &[Tree],
        edges: &mut Vec<Edge>,
        dirty_fis: &FxHashSet<u32>,
        walk: &WalkResult,
        env: &Env,
        run: &Sentinel,
    ) -> Result<ResolveResult, Killed> {
        let (lookup, entrypoints) = (&walk.lookup, walk.entrypoints.as_slice());
        let (lang, config) = (&env.lang, &env.resolve.config);
        let previous_index = self.rebuild_file_index(trees, env, entrypoints);
        let mut dirty = self.invalidate(&previous_index, trees, dirty_fis, lookup, env);
        self.lookup = lookup.clone();

        self.reqs.retain(|r| !dirty.contains(&r.fi));
        let (new_reqs, mut cross_edges) =
            gather_imports_for(trees, lang, &self.file_index, lookup, config, &dirty);
        self.reqs.extend(new_reqs);
        let previous_visible = std::mem::replace(
            &mut self.visible,
            trees
                .iter()
                .enumerate()
                .map(|(fi, tree)| gather_visible_one(tree, fi, self.tags.exports))
                .collect(),
        );
        let packages: Vec<_> = match config.merge_same_named_types {
            true => trees.iter().map(package_owners).collect(),
            false => Vec::new(),
        };
        let (ambiguous, too_many_names) = propagate_reexports(
            trees,
            &self.reqs,
            &mut self.visible,
            self.wildcard_sym,
            self.tags,
            &packages,
            run,
            env.limits.max_cross_file_names,
        );
        if too_many_names.is_none() && run.check().is_ok() {
            let added = self.importers_of_changed_names(&previous_visible, &dirty);
            let (_, import_edges) =
                gather_imports_for(trees, lang, &self.file_index, lookup, config, &added);
            cross_edges.extend(import_edges);
            dirty.extend(added);
        }
        let dirty_fis = &dirty;
        edges.retain(|edge| edge.from_tree == edge.to_tree || !dirty_fis.contains(&edge.from_tree));
        let resolved_source_paths: Vec<ResolvedSourcePath> = self
            .reqs
            .iter()
            .filter(|req| dirty_fis.contains(&req.fi))
            .filter_map(|req| {
                let sym = lang.syms.intern(&req.target_path);
                let node = trees[req.fi as usize]
                    .cursor(req.node)
                    .child(C::SourcePath)?
                    .index();
                Some(ResolvedSourcePath {
                    fi: req.fi,
                    node,
                    sym,
                })
            })
            .collect();

        let mut ctx = Ctx::new(self, trees, edges, env, run, &ambiguous, &packages);
        let (outcomes, file_timings): (Vec<_>, Vec<(u32, Duration)>) = dirty_fis
            .par_iter()
            .map(|&fi| {
                let started = Instant::now();
                (R::resolve_file(&ctx, fi as usize), (fi, started.elapsed()))
            })
            .unzip();
        let (per_file, mut killed): (Vec<Vec<Edge>>, Vec<Killed>) =
            outcomes.into_iter().partition_map(|r| match r {
                Ok(v) => Either::Left(v),
                Err(k) => Either::Right(k),
            });
        let overrun = match expand(
            &mut ctx,
            &self.reqs,
            dirty_fis,
            edges,
            per_file,
            &mut cross_edges,
        ) {
            Err(k) if k.cancelled => return Err(k),
            outcome => too_many_names.or(outcome.err()),
        };
        let mut seen: FxHashSet<_> = edges
            .iter()
            .filter(|e| e.kind == EdgeKind::Calls)
            .map(call_key)
            .collect();
        cross_edges.retain(|e| e.kind != EdgeKind::Calls || seen.insert(call_key(e)));

        let mut import_sites: FxHashSet<_> = edges
            .iter()
            .filter(|edge| edge.kind == EdgeKind::Imports)
            .map(|edge| (edge.from(), edge.to(), edge.site))
            .collect();
        cross_edges.retain(|edge| {
            edge.kind != EdgeKind::Imports
                || edge.site.is_none()
                || import_sites.insert((edge.from(), edge.to(), edge.site))
        });

        let mut non_callable = FxHashMap::default();
        for edge in edges
            .iter()
            .chain(&cross_edges)
            .filter(|edge| edge.kind == EdgeKind::Imports)
        {
            let target = ctx.corpus.follow(edge);
            if target.is(C::Def) {
                non_callable
                    .entry(edge.from())
                    .and_modify(|known| *known &= target.has_tag(ctx.tags.non_callable))
                    .or_insert_with(|| target.has_tag(ctx.tags.non_callable));
            }
        }
        let callable_sites: FxHashSet<_> = edges
            .iter()
            .chain(&cross_edges)
            .filter(|edge| edge.kind == EdgeKind::Calls)
            .filter_map(|edge| edge.site.map(|site| (edge.from_tree, site)))
            .collect();
        drop(ctx);
        for edge in edges.iter_mut().chain(&mut cross_edges).filter(|edge| {
            edge.kind == EdgeKind::Imports && edge.call_resolution != CallResolution::Reference
        }) {
            let Some(site) = edge.site else {
                continue;
            };
            edge.call_resolution = CallResolution::Unknown;
            if callable_sites.contains(&(edge.from_tree, site)) {
                edge.call_resolution = CallResolution::Callable;
            } else if non_callable.get(&edge.to()) == Some(&true)
                && trees[edge.from_tree as usize]
                    .cursor(site)
                    .child(C::Callee)
                    .is_some_and(|callee| {
                        callee.sym_opt().is_some() && !callee.has(C::Member) && !callee.has(C::Ivar)
                    })
            {
                edge.call_resolution = CallResolution::NonCallable;
            }
        }

        if let Some(mut overrun) = overrun {
            overrun.family = Some(env.lang_id);
            killed.retain(|k| (&k.label, &k.path) != (&overrun.label, &overrun.path));
            killed.push(overrun);
            *self = Self::new(lang);
        }
        Ok(ResolveResult {
            cross_edges,
            killed,
            resolved_source_paths,
            file_timings,
        })
    }
}

fn with_dependents(reqs: &[ImportReq], mut files: FxHashSet<u32>) -> FxHashSet<u32> {
    let mut dependents: FxHashMap<u32, Vec<u32>> = FxHashMap::default();
    for req in reqs {
        dependents.entry(req.target_fi).or_default().push(req.fi);
    }
    let mut pending: VecDeque<_> = files.iter().copied().collect();
    while let Some(target) = pending.pop_front() {
        for &dependent in dependents.get(&target).into_iter().flatten() {
            if files.insert(dependent) {
                pending.push_back(dependent);
            }
        }
    }
    files
}

fn call_key(e: &Edge) -> (u32, u32, Option<u32>, u32, u32) {
    (e.from_tree, e.from_node, e.site, e.to_tree, e.to_node)
}

fn touches(ctx: &Ctx, dirty_fis: &FxHashSet<u32>, r: &ImportReq) -> bool {
    (dirty_fis.contains(&r.fi) || dirty_fis.contains(&r.target_fi)) && ctx.run.check().is_ok()
}

fn expand(
    ctx: &mut Ctx,
    reqs: &[ImportReq],
    dirty_fis: &FxHashSet<u32>,
    edges: &[Edge],
    per_file: Vec<Vec<Edge>>,
    cross_edges: &mut Vec<Edge>,
) -> Result<(), Killed> {
    let wave1 = import_wave(ctx, reqs, dirty_fis, per_file);
    for e in wave1.iter().filter(|e| e.kind == EdgeKind::Extends) {
        ctx.extends_of
            .entry(e.from_loc())
            .or_default()
            .push(e.to_loc());
    }
    ctx.clear_memos();
    cross_edges.extend(&wave1);
    ctx.run.check()?;
    cross_edges.extend(wildcard_wave(
        ctx,
        reqs,
        dirty_fis,
        edges.iter().chain(&wave1),
    ));
    ctx.run.check()?;
    cross_edges.extend(producer_wave(ctx));
    ctx.run.check()?;

    let calls = |cross: &[Edge]| {
        let all = edges.iter().chain(cross.iter());
        all.filter(|e| e.kind == EdgeKind::Calls)
            .copied()
            .collect::<Vec<_>>()
    };
    let mut seen: FxHashSet<_> = calls(cross_edges).iter().map(call_key).collect();
    let mut wave: Vec<Edge> = calls(cross_edges);
    wave.retain(|e| dirty_fis.contains(&e.from_tree));
    while !wave.is_empty() {
        ctx.run.check()?;
        for e in &wave {
            if let Some(site) = e.site
                && ctx.type_uses.contains_key(&(e.from_tree, site))
                && !ctx.call_at_site.contains_key(&(e.from_tree, site))
            {
                let site = Loc {
                    fi: e.from_tree,
                    node: site,
                };
                ctx.wave_callees.entry(site).or_insert(e.to_loc());
            }
        }
        ctx.clear_memos();
        let shared: &Ctx = ctx;
        wave = wave
            .par_iter()
            .flat_map_iter(|ce| R::new(shared).resolve_type_edges(ce))
            .collect::<Vec<_>>()
            .into_iter()
            .filter(|e| seen.insert(call_key(e)))
            .collect();
        cross_edges.extend(&wave);
    }
    Ok(())
}

fn import_wave(
    ctx: &Ctx,
    reqs: &[ImportReq],
    dirty_fis: &FxHashSet<u32>,
    per_file: Vec<Vec<Edge>>,
) -> Vec<Edge> {
    reqs.par_iter()
        .filter(|r| touches(ctx, dirty_fis, r))
        .flat_map(|req| resolve_one_import(ctx, req))
        .chain(per_file.into_par_iter().flatten())
        .filter(|e| {
            !(e.kind == EdgeKind::Calls
                && e.site
                    .is_some_and(|s| ctx.corpus.jump(e.from_tree, s).has(C::Property))
                && ctx.corpus.follow(e).is_class())
        })
        .collect()
}

fn wildcard_wave<'e>(
    ctx: &Ctx,
    reqs: &[ImportReq],
    dirty_fis: &FxHashSet<u32>,
    imports: impl Iterator<Item = &'e Edge>,
) -> Vec<Edge> {
    let wildcard = ctx.wildcard_sym;
    let mut referrers: FxHashMap<Loc, FxHashSet<u32>> = FxHashMap::default();
    for e in imports.filter(|e| e.kind == EdgeKind::Imports) {
        if e.from_tree == e.to_tree && wildcard_or_name(wildcard, ctx.corpus.follow(e)) == wildcard
        {
            referrers.entry(e.to_loc()).or_default().insert(e.from_node);
        }
    }
    reqs.chunk_by(|a, b| (a.fi, a.node) == (b.fi, b.node))
        .collect::<Vec<_>>()
        .par_iter()
        .flat_map_iter(|imports| {
            let mut r = R::new(ctx);
            let mut out = Vec::new();
            for req in imports.iter().filter(|r| touches(ctx, dirty_fis, r)) {
                let names = ctx.corpus.jump(req.fi, req.node).names();
                for name in names.filter(|n| wildcard_or_name(wildcard, *n) == wildcard) {
                    if let Some(referrers) = referrers.get(&name.into()) {
                        out.extend(wildcard_uses(&mut r, req, name, referrers));
                    }
                }
            }
            let mut emitted = FxHashSet::default();
            out.retain(|e| {
                emitted.insert((e.from(), e.to(), e.kind, e.site, e.call_resolution as u8))
            });
            out
        })
        .collect()
}

fn producer_wave(ctx: &Ctx) -> Vec<Edge> {
    ctx.type_uses
        .par_iter()
        .flat_map_iter(|(&(tree, node), uses)| {
            let producer = ctx.corpus.jump(tree, node);
            let mut r = R::new(ctx);
            let typed = producer.is(C::Binding)
                || (producer.is(C::Call)
                    && !uses
                        .iter()
                        .any(|e| e.call_resolution == CallResolution::NonCallable)
                    && !ctx.call_at_site.contains_key(&(tree, node)))
                || r.external_of(producer).is_some();
            if !typed || ctx.run.check().is_err() {
                return Vec::new();
            }
            let class = r.producer_class(producer);
            r.dispatch(producer, class, uses)
        })
        .collect()
}

type Memo = std::sync::Mutex<MemoMap<Fun>>;

pub(super) struct Ctx<'a> {
    references: FxHashMap<Loc, Loc>,
    implementations: FxHashMap<Loc, Vec<Loc>>,
    saved_imports: FxHashMap<(u32, u32), Vec<u32>>,
    trees: &'a [Tree],
    run: &'a Sentinel,
    file_resolve_ms: u64,
    env: &'a Env,
    extends_of: FxHashMap<Loc, Vec<Loc>>,
    imports_to: FxHashMap<(u32, u32), Vec<&'a Edge>>,
    call_at_site: FxHashMap<(u32, u32), &'a Edge>,
    wave_callees: FxHashMap<Loc, Loc>,
    corpus: Cursor<'a>,
    type_uses: FxHashMap<(u32, u32), Vec<&'a Edge>>,
    producers: FxHashMap<(u32, u32), Vec<&'a Edge>>,
    lang: &'a Lang,
    visible: &'a VisibleMap,
    ambiguous: &'a FxHashSet<(u32, u32)>,
    file_index: &'a FileIndex,
    support_lang: SupportLang,
    index_names: &'a [String],
    wildcard_sym: u32,
    tags: ReservedTags,
    packages: &'a [FxHashMap<u32, u32>],
    partials: FxHashMap<(u32, u32, u32), Vec<Loc>>,
    extensions: FxHashMap<u32, Vec<Loc>>,
    members: Members,
    memo: Vec<Memo>,
    exporters: Vec<FxHashSet<u32>>,
    imports: Vec<FxHashMap<u32, (u32, u32)>>,
}

impl<'a> Ctx<'a> {
    fn new(
        resolver: &'a Resolver,
        trees: &'a [Tree],
        edges: &'a [Edge],
        env: &'a Env,
        run: &'a Sentinel,
        ambiguous: &'a FxHashSet<(u32, u32)>,
        packages: &'a [FxHashMap<u32, u32>],
    ) -> Self {
        let resolved_imports: FxHashSet<_> =
            resolver.reqs.iter().map(|req| (req.fi, req.node)).collect();
        let mut ctx = Ctx {
            references: FxHashMap::default(),
            implementations: FxHashMap::default(),
            saved_imports: FxHashMap::default(),
            trees,
            run,
            file_resolve_ms: env.limits.file_resolve_ms,
            env,
            extends_of: FxHashMap::default(),
            imports_to: FxHashMap::default(),
            call_at_site: FxHashMap::default(),
            wave_callees: FxHashMap::default(),
            corpus: Cursor::new(trees, 0, 0),
            type_uses: FxHashMap::default(),
            producers: FxHashMap::default(),
            lang: &env.lang,
            visible: &resolver.visible,
            ambiguous,
            file_index: &resolver.file_index,
            support_lang: env.lang_id,
            index_names: env.lang_id.index_names(),
            wildcard_sym: resolver.wildcard_sym,
            tags: resolver.tags,
            packages,
            partials: FxHashMap::default(),
            extensions: FxHashMap::default(),
            members: Members::new(trees.len()),
            memo: trees.iter().map(|_| Default::default()).collect(),
            exporters: resolver
                .visible
                .iter()
                .map(|names| names.values().map(|l| l.fi).collect())
                .collect(),
            imports: trees.iter().map(import_identities).collect(),
        };
        for e in edges {
            let site = e.site.map(|site| (e.from_tree, site));
            match e.kind {
                EdgeKind::Extends => ctx
                    .extends_of
                    .entry(e.from_loc())
                    .or_default()
                    .push(e.to_loc()),
                EdgeKind::Calls => {
                    if let Some(site) = site {
                        ctx.call_at_site.entry(site).or_insert(e);
                    }
                }
                EdgeKind::TypeFlow => {
                    ctx.type_uses.entry(e.to()).or_default().push(e);
                    if let Some(site) = site {
                        ctx.producers.entry(site).or_default().push(e);
                    }
                }
                EdgeKind::Imports => {
                    ctx.imports_to.entry(e.to()).or_default().push(e);
                    if e.call_resolution != CallResolution::Reference {
                        continue;
                    }
                    let import = ctx.corpus.follow(e).parent();
                    match site {
                        Some(site)
                            if import.is_some_and(|i| {
                                !resolved_imports.contains(&(i.fi(), i.index()))
                            }) =>
                        {
                            ctx.saved_imports.entry(site).or_default().push(e.to_node)
                        }
                        Some(_) => {}
                        None => {
                            ctx.references.insert(e.from_loc(), e.to_loc());
                        }
                    }
                }
                EdgeKind::Defines | EdgeKind::Links => {}
            }
        }
        (ctx.partials, ctx.extensions) = gather_members(trees, packages);
        ctx.resolve_owners(&resolver.reqs);
        ctx
    }

    fn resolve_owners(&mut self, reqs: &[ImportReq]) {
        let referenced_imports: FxHashSet<Loc> = self.references.values().copied().collect();
        let imported_references: Vec<_> = reqs
            .iter()
            .flat_map(|req| {
                self.corpus
                    .jump(req.fi, req.node)
                    .names()
                    .map(move |name| (req, name))
            })
            .filter(|(_, name)| referenced_imports.contains(&(*name).into()))
            .filter_map(|(req, name)| Some((name.into(), name_target(self, req, name)?)))
            .collect();
        self.references.extend(imported_references);
        for &source in self.references.keys() {
            let reference = self.corpus.jump(source.fi, source.node);
            if reference.is(C::DefName)
                && let Some(implementation) =
                    reference.parent().filter(|node| node.has(C::ImplBlock))
                && let Some(owner) = self.resolve_reference(reference)
            {
                self.implementations
                    .entry(owner.into())
                    .or_default()
                    .push(implementation.into());
            }
        }
        for owners in self.implementations.values_mut() {
            owners.sort_unstable();
        }
    }

    pub(super) fn resolve_reference(&self, reference: Cursor<'a>) -> Option<Cursor<'a>> {
        reachable(Loc::from(reference), |id| self.references.get(&id).copied())
            .skip(1)
            .map(|loc| self.corpus.jump(loc.fi, loc.node))
            .find(|node| node.is(C::Def))
    }

    fn imports_to(&self, fi: usize, target: u32) -> impl Iterator<Item = &Edge> {
        let node = self.corpus.jump(fi as u32, target);
        let related = std::iter::once(node)
            .chain(node.children())
            .chain(node.parent());
        related
            .flat_map(move |n| self.imports_to.get(&(fi as u32, n.index())))
            .flatten()
            .copied()
    }

    fn clear_memos(&mut self) {
        for memo in &mut self.memo {
            memo.get_mut().unwrap_or_else(|e| e.into_inner()).clear();
        }
    }
}

fn import_identities(tree: &Tree) -> FxHashMap<u32, (u32, u32)> {
    let mut identities = FxHashMap::default();
    let imports = tree.root().descendants().filter(|c| c.is(C::Import));
    for name in imports.flat_map(|i| i.names()) {
        let local = name.child_sym(C::Alias).unwrap_or(name.sym());
        let source = name.parent().and_then(|i| i.child_sym(C::SourcePath));
        identities
            .entry(local)
            .or_insert((source.unwrap_or(0), name.sym()));
    }
    identities
}

fn package_owners(tree: &Tree) -> FxHashMap<u32, u32> {
    let mut owners = FxHashMap::default();
    for c in tree.root().descendants() {
        if let Some(package) = c.child_sym(C::Package)
            && let Some(owner) = c.parent()
        {
            owners.entry(owner.index()).or_insert(package);
        }
    }
    owners
}

pub(super) fn partial_key(d: Cursor, packages: &[FxHashMap<u32, u32>]) -> Option<(u32, u32, u32)> {
    let owners = packages.get(d.fi() as usize)?;
    if !d.is_class() {
        return None;
    }
    let pkg = d.ancestors().find_map(|a| owners.get(&a.index()).copied());
    let arity = d
        .children()
        .filter(|c| c.is(C::Binding) && c.sym_opt().is_some() && c.children().next().is_none())
        .count();
    Some((
        pkg.unwrap_or(0),
        d.child_sym(C::DefName)?,
        u32::try_from(arity).expect("type arity exceeds u32"),
    ))
}

type PartsIndex = (
    FxHashMap<(u32, u32, u32), Vec<Loc>>,
    FxHashMap<u32, Vec<Loc>>,
);

fn gather_members(trees: &[Tree], packages: &[FxHashMap<u32, u32>]) -> PartsIndex {
    let (mut parts, mut extensions) = PartsIndex::default();
    for (fi, tree) in trees.iter().enumerate() {
        let defs = tree
            .root()
            .descendants_pruned(|d| d.is(C::Def) && !d.has(C::ImplBlock))
            .filter(|d| d.is(C::Def));
        for d in defs {
            let Some(name) = d.child_sym(C::DefName) else {
                continue;
            };
            let loc = Loc::new(fi, d.index());
            if let Some(key) = partial_key(d, packages) {
                parts.entry(key).or_default().push(loc);
            }
            if d.parent().is_some_and(|p| p.is(C::Def)) {
                extensions.entry(name).or_default().push(loc);
            }
        }
    }
    (parts, extensions)
}

#[cfg(test)]
mod remap_tests {
    use super::*;

    #[test]
    fn files_that_lost_a_visible_name_are_reported() {
        let lang = Lang::new();
        let mut resolver = Resolver::new(&lang);
        let name = 7;
        resolver.visible = vec![
            FxHashMap::from_iter([(name, Loc::new(1, 0))]),
            FxHashMap::default(),
            FxHashMap::from_iter([(name, Loc::new(0, 0))]),
        ];
        let old_labels: Vec<String> = ["a", "b", "c"].map(String::from).to_vec();
        let label_to_fi = FxHashMap::from_iter([("a", 0u32), ("c", 1u32)]);

        let lost = resolver.remap(&old_labels, &label_to_fi);

        assert_eq!(lost, FxHashSet::from_iter([0]));
        assert_eq!(resolver.visible[1].get(&name), Some(&Loc::new(0, 0)));
    }
}

#[cfg(test)]
mod resolving_tests {
    use super::imports::{Via, package_imports};
    use super::*;

    #[test]
    fn files_of_one_namespace_import_it_as_one_shared_package() {
        let repo = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(repo.path().join("Big")).unwrap();
        for f in 0..4 {
            let source = format!("namespace Big;\npublic class C{f} {{}}\n");
            std::fs::write(repo.path().join(format!("Big/F{f}.cs")), source).unwrap();
        }
        let env = Env::with_limits(SupportLang::CSharp, crate::Limits::UNLIMITED).unwrap();
        let repository = std::sync::Arc::new(
            orbit_utils::vfs::Vfs::load(
                orbit_utils::vfs::sources::Directory(repo.path()),
                orbit_utils::vfs_policies::CodePolicy,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );
        let graph = crate::templates::index(crate::pipeline::Context::new(&env, repository));
        let (_, resolved) = graph.unwrap().finish();
        let state = &resolved.state;
        let resolver = &state.resolver;
        let (via, packages) = package_imports(&state.trees, &resolver.reqs, resolver.wildcard_sym);
        assert_eq!(packages, [vec![0, 1, 2, 3]]);
        let shared = via.iter().filter(|v| matches!(v, Via::Package(0))).count();
        assert_eq!(shared, 4);
    }
}
