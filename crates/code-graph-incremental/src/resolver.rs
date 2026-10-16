use either::Either;
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use smallvec::SmallVec;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::canonical::Canonical as C;
use crate::constants::{PATH_SEP, RED_ZONE, ROOT_PATH, STACK_SEGMENT, WILDCARD};
use crate::env::Env;
use crate::file_tree::{Distance, LookupConfig, Roots};
use crate::intern::Lang;
use crate::rules::{ExportVia, ResolveConfig};
use crate::sentinel::{Killed, Sentinel};
use crate::tags::ReservedTags;
use crate::tree::{
    CallResolution, Compact, Edge, EdgeKind, find_method_in, infer_return_type, members_by_level,
    reachable,
};
type Tree = crate::tree::Tree<Compact>;
type Cursor<'a> = crate::tree::Cursor<'a, Compact>;
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
    pub fi: u32,
    pub node: u32,
}

impl Loc {
    fn new(fi: usize, node: u32) -> Self {
        Self {
            fi: u32::try_from(fi).expect("file index exceeds u32"),
            node,
        }
    }
}

pub struct ResolvedSourcePath {
    pub fi: u32,
    pub node: u32,
    pub sym: u32,
}

pub struct ResolveResult {
    pub cross_edges: Vec<Edge>,
    /// Files, or the whole family, whose cross-file pass overran its budget;
    /// the edges it had not resolved are absent.
    pub killed: Vec<Killed>,
    pub resolved_source_paths: Vec<ResolvedSourcePath>,
    /// How long each file's cross-file pass took.
    pub file_timings: Vec<(u32, Duration)>,
}

#[derive(Default)]
pub struct FileIndex {
    /// Every file or top-level def with this key, in tree order: a file by
    /// its path, stem, package, or module path; a def by its package or
    /// module path and name.
    keys: FxHashMap<String, SmallVec<[Loc; 1]>>,
    dirs: FxHashMap<String, Vec<u32>>,
}

impl FileIndex {
    fn insert(&mut self, key: String, loc: Loc) {
        let dir = key.rsplit_once(PATH_SEP).map_or("", |(d, _)| d);
        self.dirs.entry(dir.to_string()).or_default().push(loc.fi);
        self.insert_key(key, loc);
    }

    fn insert_key(&mut self, key: String, loc: Loc) {
        let locs = self.keys.entry(key).or_default();
        if !locs.contains(&loc) {
            locs.push(loc);
        }
    }

    fn get(&self, key: &str) -> &[Loc] {
        self.keys.get(key).map_or(&[], SmallVec::as_slice)
    }

    fn insert_entrypoints(&mut self, entrypoints: &[(String, String)]) {
        for (directory, path) in entrypoints {
            if let Some(&target) = self.get(path).first() {
                self.keys
                    .insert(directory.clone(), SmallVec::from_elem(target, 1));
            }
        }
    }
}

#[derive(Clone, rkyv::Archive, rkyv::Serialize, rkyv::Deserialize)]
pub struct ImportReq {
    pub fi: u32,
    pub node: u32,
    pub target_fi: u32,
    /// Def in the target file the path names (`use crate::dep::X` with an
    /// inline `mod dep {}`), or 0 for the file itself.
    pub anchor: u32,
    pub target_path: String,
}

impl ImportReq {
    /// What the import can name: the target file's visible names, plus the
    /// defs directly inside the anchor def when the path named one.
    fn exports<'a>(&self, trees: &[Tree], visible: &'a VisibleMap) -> Cow<'a, FxHashMap<u32, Loc>> {
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
        let mut index = build_file_index(trees, env, env.lang_id.index_names());
        index.insert_entrypoints(entrypoints);
        std::mem::replace(&mut self.file_index, index)
    }

    /// The files whose cross-file pass must run again: the dirty ones, every
    /// importer whose import answers changed, importers of files that first
    /// resolve now, and their dependents.
    fn invalidate(
        &self,
        previous_index: &FileIndex,
        trees: &[Tree],
        dirty_fis: &FxHashSet<u32>,
        paths: &PathLookup,
    ) -> FxHashSet<u32> {
        if self.visible.is_empty() {
            return (0..trees.len() as u32).collect();
        }
        let mut invalidated: FxHashSet<u32> = dirty_fis.iter().copied().collect();
        let lookup_changed = self.lookup != *paths.lookup;
        if lookup_changed {
            invalidated.extend(self.reqs.iter().map(|req| req.fi));
        } else {
            let mut known: FxHashMap<(u32, u32), SmallVec<[Loc; 1]>> = FxHashMap::default();
            for req in &self.reqs {
                known
                    .entry((req.fi, req.node))
                    .or_default()
                    .push(Loc::new(req.target_fi as usize, req.anchor));
            }
            let mut names_of: FxHashMap<u32, FxHashSet<u32>> = FxHashMap::default();
            for (&(fi, node), targets) in &known {
                if invalidated.contains(&fi) {
                    continue;
                }
                let import = trees[fi as usize].cursor(node);
                let now: SmallVec<[Loc; 1]> = paths
                    .resolve(fi, import)
                    .iter()
                    .map(|hit| hit.loc)
                    .collect();
                if targets.iter().any(|target| !now.contains(target)) {
                    invalidated.insert(fi);
                    continue;
                }
                let gained = now.iter().filter(|loc| !targets.contains(loc));
                let claims = |name: u32| !matches!(self.visible[fi as usize].get(&name), Some(held) if held.fi == fi);
                let gained_names: FxHashSet<u32> = gained
                    .flat_map(|loc| {
                        names_of
                            .entry(loc.fi)
                            .or_insert_with(|| {
                                gather_visible_one(
                                    &trees[loc.fi as usize],
                                    loc.fi as usize,
                                    self.tags.exports,
                                )
                                .into_keys()
                                .collect()
                            })
                            .iter()
                            .copied()
                            .filter(|&name| claims(name))
                            .collect::<Vec<_>>()
                    })
                    .collect();
                if !gained_names.is_empty()
                    && trees[fi as usize]
                        .root()
                        .descendants()
                        .any(|node| gained_names.contains(&node.sym()))
                {
                    invalidated.insert(fi);
                }
            }
        }
        let added_keys = self
            .file_index
            .keys
            .keys()
            .any(|key| !previous_index.keys.contains_key(key));
        if added_keys || lookup_changed {
            let known_imports: FxHashSet<_> =
                self.reqs.iter().map(|req| (req.fi, req.node)).collect();
            let discovery_files = (0..trees.len() as u32)
                .filter(|fi| !dirty_fis.contains(fi))
                .collect();
            let (discovered, _) = gather_imports_for(trees, paths, &discovery_files);
            invalidated.extend(
                discovered
                    .iter()
                    .filter(|req| !known_imports.contains(&(req.fi, req.node)))
                    .map(|req| req.fi),
            );
        }
        if !invalidated.is_empty() {
            let mut dependents: FxHashMap<u32, Vec<u32>> = FxHashMap::default();
            for req in &self.reqs {
                dependents.entry(req.target_fi).or_default().push(req.fi);
            }
            let mut pending: VecDeque<_> = invalidated.iter().copied().collect();
            while let Some(target) = pending.pop_front() {
                for &dependent in dependents.get(&target).into_iter().flatten() {
                    if invalidated.insert(dependent) {
                        pending.push_back(dependent);
                    }
                }
            }
        }
        invalidated
    }

    /// Renumbers the resolver's memory after files left the graph. Returns
    /// the retained files that lost a visible name, whatever brought it in:
    /// an import or a `visible_from` directive. They must resolve again.
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

    #[allow(clippy::too_many_arguments)]
    pub fn resolve(
        &mut self,
        trees: &[Tree],
        edges: &mut Vec<Edge>,
        lang: &Lang,
        dirty_fis: &FxHashSet<u32>,
        support_lang: SupportLang,
        lookup: &LookupConfig,
        config: &ResolveConfig,
        entrypoints: &[(String, String)],
        env: &Env,
        run: &Sentinel,
    ) -> Result<ResolveResult, Killed> {
        let index_names = support_lang.index_names();
        let roots = Roots::build(trees, lang);
        let previous_index = self.rebuild_file_index(trees, env, entrypoints);
        let paths = PathLookup::new(trees, env, &self.file_index, lookup, &roots);
        let dirty_fis = &self.invalidate(&previous_index, trees, dirty_fis, &paths);
        self.lookup = lookup.clone();
        edges.retain(|edge| edge.from_tree == edge.to_tree || !dirty_fis.contains(&edge.from_tree));

        self.visible.resize_with(trees.len(), Default::default);
        for &fi in dirty_fis {
            let fi = fi as usize;
            if fi < trees.len() {
                self.visible[fi] = gather_visible_one(&trees[fi], fi, self.tags.exports);
            }
        }
        self.reqs.retain(|r| !dirty_fis.contains(&r.fi));
        let (new_reqs, mut cross_edges) = gather_imports_for(trees, &paths, dirty_fis);
        drop(paths);
        self.reqs.extend(new_reqs);
        let packages: Vec<_> = match config.merge_same_named_types {
            true => trees.iter().map(package_owners).collect(),
            false => Vec::new(),
        };
        let scope = Scope {
            env,
            tags: self.tags,
            wildcard_sym: self.wildcard_sym,
            owners: &packages,
        };
        let (ambiguous, too_many_names) = propagate_reexports(
            trees,
            &self.reqs,
            &mut self.visible,
            &scope,
            run,
            env.limits.max_cross_file_names,
        );
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

        let mut type_uses: FxHashMap<(u32, u32), Vec<&Edge>> = FxHashMap::default();
        let mut producers: FxHashMap<(u32, u32), Vec<&Edge>> = FxHashMap::default();
        let mut extends_of: FxHashMap<(u32, u32), Vec<(u32, u32)>> = FxHashMap::default();
        let mut imports_to: FxHashMap<(u32, u32), Vec<&Edge>> = FxHashMap::default();
        let mut call_at_site: FxHashMap<(u32, u32), &Edge> = FxHashMap::default();
        for e in edges.iter() {
            match e.kind {
                EdgeKind::Extends => extends_of.entry(e.from()).or_default().push(e.to()),
                EdgeKind::Imports => imports_to.entry(e.to()).or_default().push(e),
                EdgeKind::Calls => {
                    if let Some(site) = e.site {
                        call_at_site.entry((e.from_tree, site)).or_insert(e);
                    }
                }
                _ => {}
            }
            if e.kind == EdgeKind::TypeFlow {
                type_uses.entry((e.to_tree, e.to_node)).or_default().push(e);
                if let Some(site) = e.site {
                    producers.entry((e.from_tree, site)).or_default().push(e);
                }
            }
        }

        let (partials, extensions) = gather_members(trees, &packages);
        let declared_members: Vec<MemberCache> = trees.iter().map(|_| Default::default()).collect();
        let exporters: Vec<FxHashSet<u32>> = self
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
        let resolved_imports: FxHashSet<_> =
            self.reqs.iter().map(|req| (req.fi, req.node)).collect();
        let mut saved_imports: FxHashMap<_, Vec<u32>> = FxHashMap::default();
        for edge in edges.iter().filter(|edge| {
            edge.kind == EdgeKind::Imports && edge.call_resolution == CallResolution::Reference
        }) {
            if let Some(site) = edge.site
                && let Some(import) = trees[edge.to_tree as usize].cursor(edge.to_node).parent()
                && !resolved_imports.contains(&(edge.to_tree, import.index()))
            {
                saved_imports
                    .entry((edge.from_tree, site))
                    .or_default()
                    .push(edge.to_node);
            }
        }
        let mut ctx = ResolveCtx {
            references: FxHashMap::default(),
            implementations: FxHashMap::default(),
            saved_imports,
            trees,
            run,
            file_resolve_ms: env.limits.file_resolve_ms,
            env,
            extends_of,
            imports_to,
            call_at_site,
            wave_callees: FxHashMap::default(),
            corpus: Cursor::new(trees, 0, 0),
            type_uses,
            producers,
            lang,
            visible: &self.visible,
            ambiguous: &ambiguous,
            file_index: &self.file_index,
            support_lang,
            index_names,
            wildcard_sym: self.wildcard_sym,
            tags: self.tags,
            packages: &packages,
            partials: &partials,
            extensions: &extensions,
            declared_members: &declared_members,
            chains: trees.iter().map(|_| Default::default()).collect(),
            exporters: &exporters,
            imports: &imports,
        };
        ctx.resolve_owners(edges, &self.reqs);
        let (outcomes, file_timings): (Vec<_>, Vec<(u32, Duration)>) = dirty_fis
            .par_iter()
            .map(|&fi| {
                let started = Instant::now();
                (resolve_file(&ctx, fi as usize), (fi, started.elapsed()))
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
            overrun.family = Some(support_lang);
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

fn call_key(e: &Edge) -> (u32, u32, Option<u32>, u32, u32) {
    (e.from_tree, e.from_node, e.site, e.to_tree, e.to_node)
}

fn expand(
    ctx: &mut ResolveCtx,
    reqs: &[ImportReq],
    dirty_fis: &FxHashSet<u32>,
    edges: &[Edge],
    per_file: Vec<Vec<Edge>>,
    cross_edges: &mut Vec<Edge>,
) -> Result<(), Killed> {
    let wave1: Vec<Edge> = reqs
        .par_iter()
        .filter(|r| {
            (dirty_fis.contains(&r.fi) || dirty_fis.contains(&r.target_fi))
                && ctx.run.check().is_ok()
        })
        .flat_map(|req| resolve_one_import(ctx, req))
        .chain(per_file.into_par_iter().flatten())
        .filter(|e| {
            !(e.kind == EdgeKind::Calls
                && e.site
                    .is_some_and(|s| ctx.corpus.jump(e.from_tree, s).has(C::Property))
                && ctx.corpus.follow(e).is_class())
        })
        .collect();
    for e in wave1.iter().filter(|e| e.kind == EdgeKind::Extends) {
        ctx.extends_of.entry(e.from()).or_default().push(e.to());
    }
    ctx.chains
        .iter_mut()
        .for_each(|memo| memo.get_mut().unwrap_or_else(|e| e.into_inner()).clear());
    cross_edges.extend(&wave1);
    ctx.run.check()?;

    // Wildcard references come from the linker and from wave 1's unbound
    // names alike, so their uses resolve once both are known.
    let mut referrers: FxHashMap<(u32, u32), FxHashSet<u32>> = FxHashMap::default();
    for e in edges
        .iter()
        .chain(&wave1)
        .filter(|e| e.kind == EdgeKind::Imports)
    {
        let target = ctx.corpus.jump(e.to_tree, e.to_node);
        if e.from_tree == e.to_tree
            && wildcard_or_name(ctx.wildcard_sym, target) == ctx.wildcard_sym
        {
            referrers.entry(e.to()).or_default().insert(e.from_node);
        }
    }
    let wildcard_edges: Vec<Edge> = reqs
        .chunk_by(|a, b| (a.fi, a.node) == (b.fi, b.node))
        .collect::<Vec<_>>()
        .par_iter()
        .flat_map_iter(|imports| {
            let mut emitted = FxHashSet::default();
            imports
                .iter()
                .filter(|r| {
                    (dirty_fis.contains(&r.fi) || dirty_fis.contains(&r.target_fi))
                        && ctx.run.check().is_ok()
                })
                .flat_map(|req| {
                    let import = ctx.corpus.jump(req.fi, req.node);
                    import
                        .names()
                        .filter(|n| wildcard_or_name(ctx.wildcard_sym, *n) == ctx.wildcard_sym)
                        .filter_map(|n| referrers.get(&(n.fi(), n.index())).map(|r| (n, r)))
                        .flat_map(|(n, r)| wildcard_uses(ctx, req, n, r))
                        .collect::<Vec<_>>()
                })
                .filter(|e| {
                    emitted.insert((e.from(), e.to(), e.kind, e.site, e.call_resolution as u8))
                })
                .collect::<Vec<_>>()
        })
        .collect();
    cross_edges.extend(&wildcard_edges);
    ctx.run.check()?;

    let wave2: Vec<Edge> = ctx
        .type_uses
        .par_iter()
        .map(|(&(tree, node), uses)| (ctx.corpus.jump(tree, node), uses))
        .filter(|(producer, uses)| {
            (producer.is(C::Binding)
                || (producer.is(C::Call)
                    && !uses
                        .iter()
                        .any(|e| e.call_resolution == CallResolution::NonCallable)
                    && !ctx
                        .call_at_site
                        .contains_key(&(producer.fi(), producer.index())))
                || external_of(ctx, *producer).is_some())
                && ctx.run.check().is_ok()
        })
        .flat_map(|(producer, uses)| dispatch(ctx, producer, producer_class(ctx, producer), uses))
        .collect();
    cross_edges.extend(&wave2);
    ctx.run.check()?;

    let mut wave: Vec<Edge> = edges
        .iter()
        .chain(cross_edges.iter())
        .filter(|e| e.kind == EdgeKind::Calls && dirty_fis.contains(&e.from_tree))
        .copied()
        .collect();
    let mut seen: FxHashSet<_> = edges
        .iter()
        .chain(cross_edges.iter())
        .filter(|e| e.kind == EdgeKind::Calls)
        .map(call_key)
        .collect();
    while !wave.is_empty() {
        ctx.run.check()?;
        for e in wave.iter().filter(|e| e.kind == EdgeKind::Calls) {
            if let Some(site) = e.site
                && ctx.type_uses.contains_key(&(e.from_tree, site))
                && !ctx.call_at_site.contains_key(&(e.from_tree, site))
            {
                ctx.wave_callees
                    .entry((e.from_tree, site))
                    .or_insert(e.to());
            }
        }
        wave = wave
            .par_iter()
            .flat_map(|ce| resolve_type_edges(ctx, ce))
            .collect::<Vec<_>>()
            .into_iter()
            .filter(|e| seen.insert(call_key(e)))
            .collect();
        cross_edges.extend(&wave);
    }
    Ok(())
}

struct ResolveCtx<'a> {
    references: FxHashMap<(u32, u32), (u32, u32)>,
    implementations: FxHashMap<(u32, u32), Vec<(u32, u32)>>,
    saved_imports: FxHashMap<(u32, u32), Vec<u32>>,
    trees: &'a [Tree],
    run: &'a Sentinel,
    file_resolve_ms: u64,
    env: &'a Env,
    extends_of: FxHashMap<(u32, u32), Vec<(u32, u32)>>,
    imports_to: FxHashMap<(u32, u32), Vec<&'a Edge>>,
    call_at_site: FxHashMap<(u32, u32), &'a Edge>,
    wave_callees: FxHashMap<(u32, u32), (u32, u32)>,
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
    partials: &'a FxHashMap<(u32, u32, u32), Vec<Loc>>,
    extensions: &'a FxHashMap<u32, Vec<Loc>>,
    declared_members: &'a [MemberCache],
    chains: Vec<ChainMemo>,
    exporters: &'a [FxHashSet<u32>],
    imports: &'a [FxHashMap<u32, (u32, u32)>],
}

impl ResolveCtx<'_> {
    fn resolve_owners(&mut self, edges: &[Edge], reqs: &[ImportReq]) {
        self.references = edges
            .iter()
            .filter(|edge| {
                edge.kind == EdgeKind::Imports
                    && edge.call_resolution == CallResolution::Reference
                    && edge.site.is_none()
            })
            .map(|edge| (edge.from(), edge.to()))
            .collect();
        let referenced_imports: FxHashSet<_> = self.references.values().copied().collect();
        let imported_references: Vec<_> = reqs
            .iter()
            .flat_map(|req| {
                self.corpus
                    .jump(req.fi, req.node)
                    .names()
                    .map(move |name| (req, name))
            })
            .filter(|(_, name)| referenced_imports.contains(&(name.fi(), name.index())))
            .filter_map(|(req, name)| {
                let target = name_target(self, req, name)?;
                Some(((name.fi(), name.index()), (target.fi, target.node)))
            })
            .collect();
        self.references.extend(imported_references);
        for &source in self.references.keys() {
            let reference = self.corpus.jump(source.0, source.1);
            if reference.is(C::DefName)
                && let Some(implementation) =
                    reference.parent().filter(|node| node.has(C::ImplBlock))
                && let Some(owner) = resolve_reference(self, reference)
            {
                self.implementations
                    .entry((owner.fi(), owner.index()))
                    .or_default()
                    .push((implementation.fi(), implementation.index()));
            }
        }
        for owners in self.implementations.values_mut() {
            owners.sort_unstable();
        }
    }

    /// Import edges into `target`, its name children, or its parent import.
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
}

fn gather_visible_one(tree: &Tree, fi: usize, exports_key: u32) -> FxHashMap<u32, Loc> {
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

fn gather_imports_for(
    trees: &[Tree],
    paths: &PathLookup,
    dirty_fis: &FxHashSet<u32>,
) -> (Vec<ImportReq>, Vec<Edge>) {
    let dirty_vec: Vec<u32> = dirty_fis.iter().copied().collect();
    let per_tree: Vec<(Vec<ImportReq>, Vec<Edge>)> = dirty_vec
        .par_iter()
        .map(|&fi| {
            trees[fi as usize].root().fold_tree(
                (Vec::new(), Vec::new()),
                |(reqs, edges), cur, _w| {
                    if cur.kind() != C::Import && cur.kind() != C::ImportType {
                        return;
                    }
                    for hit in paths.resolve(fi, cur) {
                        if hit.route == Route::Submodule {
                            edges.push(Edge::new(
                                fi,
                                cur.index(),
                                hit.loc.fi,
                                0,
                                EdgeKind::Imports,
                            ));
                        }
                        reqs.push(ImportReq {
                            fi,
                            node: cur.index(),
                            target_fi: hit.loc.fi,
                            anchor: hit.loc.node,
                            target_path: hit.path,
                        });
                    }
                },
            )
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

/// The step of the lookup that found a target.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Route {
    /// The path names a file, a directory, or a top-level def.
    Direct,
    /// The path plus an imported name names a file.
    Submodule,
    /// A segment of the path is a name an import binds in the file before
    /// it; the lookup went on from that import's source.
    ImportHop,
    /// The path names defs inside a file, one per trailing segment.
    DefClimb,
}

struct Hit {
    loc: Loc,
    path: String,
    route: Route,
}

/// One lookup for every import path: the gather and the stale check ask it
/// the same question and get the same answer.
struct PathLookup<'a> {
    trees: &'a [Tree],
    env: &'a Env,
    lang: &'a Lang,
    config: &'a ResolveConfig,
    file_index: &'a FileIndex,
    lookup: &'a LookupConfig,
    roots: &'a Roots,
    tags: ReservedTags,
    wildcard_sym: u32,
    stdlib_prefixes: Vec<String>,
}

impl<'a> PathLookup<'a> {
    fn new(
        trees: &'a [Tree],
        env: &'a Env,
        file_index: &'a FileIndex,
        lookup: &'a LookupConfig,
        roots: &'a Roots,
    ) -> Self {
        let declared: FxHashSet<&str> = roots.paths().collect();
        let stdlib_prefixes = lookup
            .prefixes
            .iter()
            .filter(|prefix| declared.contains(prefix.as_str()))
            .cloned()
            .collect();
        Self {
            trees,
            env,
            lang: &env.lang,
            config: &env.resolve.config,
            file_index,
            lookup,
            roots,
            tags: ReservedTags::new(&env.lang),
            wildcard_sym: env.lang.syms.intern(WILDCARD),
            stdlib_prefixes,
        }
    }

    /// Every target an import of file `fi` names, found by the first step
    /// that answers: alias, direct, submodule, import hop, then def climb.
    fn resolve(&self, fi: u32, import: Cursor) -> Vec<Hit> {
        let tree = &self.trees[fi as usize];
        let Some(source_sym) = import
            .tag(self.tags.original_source_path)
            .or_else(|| import.child_sym(C::SourcePath))
        else {
            return Vec::new();
        };
        let source = self.lang.syms.resolve(source_sym);
        let Some(resolved) = tree.get_tag(import.index(), self.tags.resolved_source) else {
            return Vec::new();
        };
        let raw = self.lang.syms.resolve(resolved);
        let mapped = self.lookup.aliases.iter().find_map(|alias| {
            if alias.scope.is_some() && alias.scope != import.tag(self.tags.alias_scope) {
                return None;
            }
            let path = apply_alias(source, &alias.pattern, &alias.replacement)?;
            (!alias.if_exists || !self.files(fi, &path, &[]).is_empty()).then_some(path)
        });
        if mapped.is_none() && is_external(source, &self.config.external) {
            return Vec::new();
        }
        let stdlib = mapped.is_none() && is_external(source, &self.config.stdlib);
        let prefixes = match stdlib {
            true => &self.stdlib_prefixes,
            false => &self.lookup.prefixes,
        };
        let path = mapped.unwrap_or_else(|| self.rooted(fi, raw));
        let names: Vec<u32> = import.names().map(|name| name.sym()).collect();
        let hits = |locs: Vec<Loc>, path: &str, route| -> Vec<Hit> {
            locs.into_iter()
                .map(|loc| Hit {
                    loc,
                    path: path.to_owned(),
                    route,
                })
                .collect()
        };
        let direct = self.files(fi, &path, prefixes);
        if !direct.is_empty() {
            return hits(direct, &path, Route::Direct);
        }
        let submodules: Vec<Hit> = names
            .iter()
            .filter(|&&name| !stdlib || name != self.wildcard_sym)
            .flat_map(|&name| {
                let submodule = format!("{path}{PATH_SEP}{}", self.lang.syms.resolve(name));
                let found = self.files(fi, &submodule, prefixes);
                hits(found, &submodule, Route::Submodule)
            })
            .collect();
        if !submodules.is_empty() {
            return submodules;
        }
        if let Some((bound, found)) = self.follow_bindings(fi, &path, prefixes) {
            return hits(found, &bound, Route::ImportHop);
        }
        self.defs_on_path(fi, &path, &names, prefixes)
    }

    /// A source path with the own-root segment replaced by the root of `fi`:
    /// `.` is the root itself and `./a/b` is `a/b` below it.
    fn rooted(&self, fi: u32, raw: &str) -> String {
        let own = self.roots.path(fi).unwrap_or_default();
        match (raw, raw.split_once(PATH_SEP)) {
            (ROOT_PATH, _) => own.to_owned(),
            (_, Some((ROOT_PATH, rest))) if own.is_empty() => rest.to_owned(),
            (_, Some((ROOT_PATH, rest))) => format!("{own}{PATH_SEP}{rest}"),
            _ => raw.to_owned(),
        }
    }

    /// The files or defs a path names from file `fi`, nearest root first:
    /// every file of a directory for a glob, else the one best match. A
    /// glob drops a file from another root when the importer's root has a
    /// file at the same relative path and the language says that path
    /// names one type. A path whose best match is a def of the importing
    /// file names nothing here: the linker owns same-file paths. A path
    /// whose best match is the importing file itself falls through to a
    /// file in another root, as a test at the path of the class it tests
    /// needs, but not to a root nested inside the importer's own.
    fn files(&self, fi: u32, path: &str, prefixes: &[String]) -> Vec<Loc> {
        let (glob, mut found) = self.matches(fi, path, prefixes);
        if !glob && found.first().is_some_and(|loc| loc.fi == fi) {
            if found[0].node != 0 {
                return Vec::new();
            }
            found.retain(|loc| !self.roots.nested_below(fi, loc.fi));
        }
        found.retain(|loc| loc.fi != fi);
        if !glob {
            found.truncate(1);
        }
        found
    }

    /// Every match of `path` from `fi`, nearest root first and a file before
    /// a def, with the importer's own file still in the list. Says whether
    /// the path was a glob.
    fn matches(&self, fi: u32, path: &str, prefixes: &[String]) -> (bool, Vec<Loc>) {
        let keyed = |key: &str| -> Vec<Loc> {
            std::iter::once(key.to_owned())
                .chain(
                    prefixes
                        .iter()
                        .filter(|prefix| !prefix.is_empty())
                        .map(|prefix| format!("{prefix}{PATH_SEP}{key}")),
                )
                .flat_map(|key| self.file_index.get(&key).to_vec())
                .collect()
        };
        let glob = path
            .strip_suffix(WILDCARD)
            .map(|dir| dir.trim_end_matches(PATH_SEP));
        let mut found: Vec<Loc> = match glob {
            Some(dir) => std::iter::once(dir.to_owned())
                .chain(prefixes.iter().map(|p| format!("{p}{PATH_SEP}{dir}")))
                .filter_map(|dir| self.file_index.dirs.get(&dir))
                .flatten()
                .map(|&fi| Loc { fi, node: 0 })
                .collect(),
            None => keyed(path),
        };
        found = found.into_iter().unique().collect();
        found.sort_by_key(|loc| (self.roots.distance(fi, loc.fi), loc.node != 0));
        if glob.is_none() {
            return (false, found);
        }
        let rel = |fi: u32| self.roots.rel_path(fi, &self.trees[fi as usize].label);
        let own: FxHashSet<&str> = std::iter::once(fi)
            .chain(found.iter().map(|loc| loc.fi))
            .filter(|&hit| self.roots.distance(fi, hit) == Distance::Own)
            .map(rel)
            .collect();
        found.retain(|loc| {
            self.roots.distance(fi, loc.fi) == Distance::Own
                || !own.contains(rel(loc.fi))
                || !self
                    .env
                    .rules_for(&self.trees[loc.fi as usize].label)
                    .config
                    .resolve
                    .same_path_same_type
        });
        (true, found)
    }

    /// The file an empty path names from `fi`: the index file of its own
    /// root, else of the first lookup prefix that has one.
    fn root_file(&self, fi: u32, prefixes: &[String]) -> Option<Loc> {
        self.roots
            .path(fi)
            .into_iter()
            .chain(prefixes.iter().map(String::as_str))
            .find_map(|root| {
                self.file_index
                    .get(root)
                    .iter()
                    .find(|loc| loc.node == 0)
                    .copied()
            })
    }

    /// Follows the imports that bind a segment of `path` until the path
    /// names files. A binding seen twice is a cycle, and the walk stops.
    fn follow_bindings(
        &self,
        fi: u32,
        path: &str,
        prefixes: &[String],
    ) -> Option<(String, Vec<Loc>)> {
        let mut visited: FxHashSet<(u32, u32)> = FxHashSet::default();
        let mut path = path.to_owned();
        loop {
            let (bound, binding) = self.bind_once(fi, &path, prefixes)?;
            if !visited.insert(binding) {
                return None;
            }
            path = bound;
            let found = self.files(fi, &path, prefixes);
            if !found.is_empty() {
                return Some((path, found));
            }
        }
    }

    /// Rewrites `path` through the import that binds its last bound
    /// segment: the file at the head of the path imports a name that equals
    /// the next segment, so the path goes on from that import's source.
    fn bind_once(&self, fi: u32, path: &str, prefixes: &[String]) -> Option<(String, (u32, u32))> {
        let segments: Vec<&str> = path.split(PATH_SEP).collect();
        (0..segments.len()).rev().find_map(|i| {
            let segment = segments[i];
            if segment.is_empty() || segment == WILDCARD {
                return None;
            }
            let name = self.lang.syms.lookup(segment);
            if name == 0 {
                return None;
            }
            let file = match i {
                0 => self.root_file(fi, prefixes),
                _ => self
                    .files(fi, &segments[..i].join(PATH_SEP), prefixes)
                    .into_iter()
                    .next(),
            }
            .filter(|loc| loc.node == 0)?;
            let tree = &self.trees[file.fi as usize];
            let (import, original) = tree
                .root()
                .descendants_pruned(|c| c.is(C::Def))
                .filter(|c| c.is(C::Import))
                .find_map(|import| {
                    let bound = import
                        .names()
                        .find(|n| n.child_sym(C::Alias).unwrap_or(n.sym()) == name)?;
                    Some((import, bound.sym()))
                })?;
            let source = self
                .lang
                .syms
                .resolve(tree.get_tag(import.index(), self.tags.resolved_source)?);
            let mut next = self.rooted(file.fi, source);
            let rest = segments[i + 1..].iter().copied();
            for part in std::iter::once(self.lang.syms.resolve(original)).chain(rest) {
                if !next.is_empty() {
                    next.push_str(PATH_SEP);
                }
                next.push_str(part);
            }
            Some((next, (file.fi, name)))
        })
    }

    /// A path whose tail names defs inside a file, `use crate::errors::Error::Kind`
    /// with `enum Error { Kind }` in `errors.rs`: the longest head that names a
    /// file or a def, then one def per remaining segment. An import whose one
    /// name is the last segment anchors at the def that holds it. A head that
    /// names the importer's own file ends the climb: the linker owns that path.
    fn defs_on_path(&self, fi: u32, path: &str, names: &[u32], prefixes: &[String]) -> Vec<Hit> {
        let imported = (names.len() == 1).then(|| names[0]);
        let mut prefix = path;
        let mut tail = Vec::new();
        while let Some((head, last)) = prefix.rsplit_once(PATH_SEP) {
            tail.push(self.lang.syms.lookup(last));
            let (_, found) = self.matches(fi, head, prefixes);
            if found.first().is_some_and(|loc| loc.fi == fi) {
                return Vec::new();
            }
            let found: Vec<Loc> = found
                .into_iter()
                .filter(|loc| loc.fi != fi)
                .take(1)
                .collect();
            if !found.is_empty() {
                let rest = &path[head.len()..];
                let head_name = self
                    .lang
                    .syms
                    .lookup(head.rsplit_once(PATH_SEP).map_or(head, |(_, last)| last));
                return found
                    .into_iter()
                    .filter_map(|loc| {
                        let mut node = self.trees[loc.fi as usize].cursor(loc.node);
                        if loc.node == 0 {
                            node = node.child_def(head_name).unwrap_or(node);
                        }
                        for &name in tail.iter().rev() {
                            node = node.child_def(name)?;
                        }
                        if imported.is_some_and(|name| tail.first() == Some(&name))
                            && let Some(parent) = node.parent().filter(|parent| parent.is(C::Def))
                        {
                            node = parent;
                        }
                        Some(Hit {
                            loc: Loc::new(loc.fi as usize, node.index()),
                            path: format!("{head}{rest}"),
                            route: Route::DefClimb,
                        })
                    })
                    .collect();
            }
            prefix = head;
        }
        Vec::new()
    }
}

/// What `propagate_reexports` reads besides the requests: the per-file rule
/// config and the C# partial-class owners.
struct Scope<'a> {
    env: &'a Env,
    tags: ReservedTags,
    wildcard_sym: u32,
    owners: &'a [FxHashMap<u32, u32>],
}

/// How strongly an export claims a name: an inner package clause beats an
/// outer one; any other two claims on one name make it ambiguous.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Claim {
    level: u32,
}

impl Claim {
    /// Whether this claim beats `other`; `None` when neither does.
    fn beats(self, other: Claim) -> Option<bool> {
        match (self.level > 0, other.level > 0) {
            (true, true) if self.level != other.level => Some(self.level > other.level),
            _ => None,
        }
    }
}

const PROPAGATE_CHECK_INTERVAL: usize = 100_000;
type MemberCache = std::sync::Mutex<FxHashMap<u32, std::sync::Arc<FxHashMap<u32, u32>>>>;

fn members_of(ctx: &ResolveCtx, owner: Cursor) -> std::sync::Arc<FxHashMap<u32, u32>> {
    let cache = &ctx.declared_members[owner.fi() as usize];
    if let Some(members) = cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&owner.index())
    {
        return members.clone();
    }
    let mut members = FxHashMap::default();
    let mut walk = owner.walk();
    while let Some(node) = walk.next() {
        if node.is(C::Def) {
            if let Some(name) = node.child_sym(C::DefName) {
                members.entry(name).or_insert(node.index());
            }
            if node.is_class() && !node.has(C::ImplBlock) {
                walk.skip_subtree();
            }
        }
    }
    cache
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry(owner.index())
        .or_insert_with(|| std::sync::Arc::new(members))
        .clone()
}

fn propagate_reexports(
    trees: &[Tree],
    reqs: &[ImportReq],
    visible: &mut VisibleMap,
    scope: &Scope,
    run: &Sentinel,
    max_names: usize,
) -> (FxHashSet<(u32, u32)>, Option<Killed>) {
    let (tags, wildcard_sym) = (scope.tags, scope.wildcard_sym);
    let mut ambiguous: FxHashSet<(u32, u32)> = FxHashSet::default();
    let mut names: usize = visible.iter().map(FxHashMap::len).sum();
    let mut too_many_names = None;
    let key = |l: Loc| partial_key(trees[l.fi as usize].cursor(l.node), scope.owners);
    let config = |fi: usize| &scope.env.rules_for(&trees[fi].label).config.resolve;

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
    let import_of = |req: &ImportReq| trees[req.fi as usize].cursor(req.node);
    let file_level = |import: Cursor| import.enclosing(|p| p.is(C::Def)).is_none();
    let level = |req: &ImportReq| {
        let import = import_of(req);
        let chained = config(req.fi as usize).nested_packages
            && import.ancestors().any(|node| node.is(C::ModuleExport));
        match chained {
            true => req.node,
            false => 0,
        }
    };
    'propagate: loop {
        if run.check().is_err() {
            break;
        }
        let mut unions: Vec<Option<PackageExports>> = vec![None; packages.len()];
        let mut new_exports: indexmap::IndexMap<
            (usize, u32),
            (Loc, Claim),
            rustc_hash::FxBuildHasher,
        > = Default::default();
        let added = std::cell::Cell::new(0);
        let mut export = |fi: usize, name, loc: Loc, via: Option<ExportVia>, level: u32| {
            if let Some(&held) = visible[fi].get(&name) {
                let held_via = match held.fi as usize == fi {
                    true if trees[fi]
                        .cursor(held.node)
                        .enclosing(|p| p.is(C::Def))
                        .is_some() =>
                    {
                        Some(ExportVia::NestedDef)
                    }
                    true => Some(ExportVia::OwnDef),
                    false => None,
                };
                let replaces = held != loc
                    && via
                        .zip(held_via)
                        .is_some_and(|(via, held)| config(fi).outranks(via, held));
                if !replaces {
                    return;
                }
            }
            let claim = Claim { level };
            match new_exports.entry((fi, name)) {
                indexmap::map::Entry::Vacant(entry) => {
                    entry.insert((loc, claim));
                    added.set(added.get() + 1);
                }
                indexmap::map::Entry::Occupied(mut entry) => {
                    let (existing, held) = *entry.get();
                    if existing == loc || key(existing).is_some() && key(existing) == key(loc) {
                        return;
                    }
                    let id = (u32::try_from(fi).expect("file index exceeds u32"), name);
                    match claim.beats(held) {
                        Some(true) => {
                            entry.insert((loc, claim));
                            ambiguous.remove(&id);
                        }
                        Some(false) => {}
                        None => {
                            ambiguous.insert(id);
                        }
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
            let import = import_of(req);
            let claim = |kind| file_level(import).then_some(kind);
            match via[i] {
                Via::Own => {}
                Via::Package(package) => {
                    let union = unions[package]
                        .get_or_insert_with(|| package_exports(&packages[package], visible));
                    let level = level(req);
                    for (&name, locs) in union.iter() {
                        for &loc in locs {
                            export(req.fi as usize, name, loc, claim(ExportVia::Glob), level);
                        }
                    }
                    continue;
                }
                Via::Covered => continue,
            }
            let exports = req.exports(trees, visible);
            for c in import.names() {
                let ns = wildcard_or_name(wildcard_sym, c);
                if ns == wildcard_sym {
                    if let Some(alias) = namespace_alias(wildcard_sym, c)
                        && c.ancestors().any(|node| node.is(C::ModuleExport))
                    {
                        export(
                            req.fi as usize,
                            alias,
                            Loc::new(req.target_fi as usize, req.anchor),
                            None,
                            0,
                        );
                        continue;
                    }
                    let level = level(req);
                    for (&ds, &loc) in exports.iter() {
                        export(req.fi as usize, ds, loc, claim(ExportVia::Glob), level);
                    }
                } else if let Some(&loc) = exports.get(&ns) {
                    export(
                        req.fi as usize,
                        c.child_sym(C::Alias).unwrap_or(ns),
                        loc,
                        claim(ExportVia::NamedImport),
                        0,
                    );
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
                    export(fi, ds, dloc, None, 0);
                }
            }
        }

        for req in reqs {
            for &n in &declared[req.target_fi as usize] {
                let own = visible[req.fi as usize].get(&n).filter(|l| l.fi == req.fi);
                if let Some(&loc) = own {
                    export(req.target_fi as usize, n, loc, None, 0);
                }
            }
        }
        if new_exports.is_empty() {
            break;
        }
        names += new_exports.len();
        for ((fi, ns), (loc, _)) in new_exports {
            visible[fi].insert(ns, loc);
        }
    }
    (ambiguous, too_many_names)
}

type PackageExports = indexmap::IndexMap<u32, SmallVec<[Loc; 1]>, rustc_hash::FxBuildHasher>;

#[derive(Clone, Copy)]
enum Via {
    Own,
    Package(usize),
    Covered,
}

fn package_imports(
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

fn package_exports(files: &[u32], visible: &VisibleMap) -> PackageExports {
    let mut union = PackageExports::default();
    for &fi in files {
        for (&name, &loc) in &visible[fi as usize] {
            let locs = union.entry(name).or_default();
            if !locs.contains(&loc) {
                locs.push(loc);
            }
        }
    }
    union
}

fn name_target(ctx: &ResolveCtx, req: &ImportReq, c: Cursor) -> Option<Loc> {
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

fn resolve_one_import(ctx: &ResolveCtx, req: &ImportReq) -> Vec<Edge> {
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
        let named = name_target(ctx, req, name);
        let member = named
            .filter(|target| req.anchor != 0 && target.node != 0)
            .and_then(|target| {
                members_of(ctx, ctx.corpus.jump(target.fi, target.node))
                    .get(&m.sym())
                    .map(|&node| ctx.corpus.jump(target.fi, node))
            });
        let target_file = named
            .filter(|target| target.node == 0)
            .map_or(tfi, |target| target.fi as usize);
        let target = member
            .or_else(|| {
                if target_file == tfi {
                    exports.get(&m.sym())
                } else {
                    ctx.visible[target_file].get(&m.sym())
                }
                .map(|loc| ctx.corpus.jump(loc.fi, loc.node))
            })
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

/// A wildcard import names the file; the definitions it stands for are the
/// ones the importing code refers to: called, extended or used as a
/// decorator. Each reference gives the wildcard an import edge to the
/// definition, and a call to a callable one a call edge; nothing else.
fn wildcard_uses<'a>(
    ctx: &'a ResolveCtx,
    req: &ImportReq,
    name: Cursor<'a>,
    referrers: &FxHashSet<u32>,
) -> Vec<Edge> {
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
    let mut imported: FxHashSet<(u32, u32)> = FxHashSet::default();
    let mut edges = Vec::new();
    let mut import = |target: Cursor, edges: &mut Vec<Edge>| {
        if imported.insert((target.fi(), target.index())) {
            edges.push(name.edge_to(target, EdgeKind::Imports));
        }
    };
    let root = |r: Cursor<'a>| qualified(ctx, r.sym(), &provided);
    let declared = |callee: Cursor<'a>| {
        let head = chain_head(callee);
        resolve_reference(ctx, head).or_else(|| enclosing_name(ctx, head, head.sym()))
    };
    for from in referrers.iter().map(|&n| ctx.corpus.jump(fi as u32, n)) {
        for call in from.calls() {
            let Some(callee) = call.child(C::Callee) else {
                continue;
            };
            let Some(target) = chain(ctx, callee, &root) else {
                continue;
            };
            if declared(callee).is_some() {
                continue;
            }
            import(target, &mut edges);
            if target.has_tag(ctx.tags.callable) {
                edges.push(call_edge(from, target, Some(call.index())));
            }
        }
        let headers = from
            .children_of(C::SuperType)
            .chain(from.children_of(C::Decorator));
        for target in headers.filter_map(|h| chain(ctx, h, &root)) {
            import(target, &mut edges);
        }
    }
    edges
}

/// A name that is a path in the language's own spelling (`Security::Ctx`):
/// the first segment resolves as a root, each further one as a member.
fn qualified<'a>(
    ctx: &'a ResolveCtx,
    sym: u32,
    root: &dyn Fn(u32) -> Option<Cursor<'a>>,
) -> Option<Cursor<'a>> {
    if let Some(found) = root(sym) {
        return Some(found);
    }
    let separator = ctx.support_lang.fqn_separator();
    let mut segments = ctx.lang.syms.resolve(sym).split(separator);
    let mut current = root(ctx.lang.syms.lookup(segments.next()?))?;
    for segment in segments {
        let name = ctx.lang.syms.lookup(segment);
        current = method_up(ctx, current, name, current.fi() as usize)
            .into_iter()
            .exactly_one()
            .ok()?;
    }
    Some(current)
}

fn namespace_alias(wildcard_sym: u32, name: Cursor) -> Option<u32> {
    name.child_sym(C::Alias)
        .filter(|_| wildcard_or_name(wildcard_sym, name) == wildcard_sym)
}

fn wildcard_or_name(wildcard_sym: u32, name: Cursor) -> u32 {
    name.child_sym(C::SsaHint)
        .filter(|&h| h == wildcard_sym)
        .unwrap_or(name.sym())
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
                    reaching.iter().map(|e| {
                        let reached = ctx.corpus.follow(e);
                        match (reached.fi(), reached.index()) == (producer.fi(), producer.index()) {
                            true => class,
                            false => producer_class(ctx, reached),
                        }
                    }),
                ),
                _ => class,
            };
            let from = ctx.corpus.jump(usage.from_tree, usage.from_node);
            let Some(class) = class else {
                if call
                    .child(C::Callee)
                    .is_some_and(|callee| callee.sym_opt().is_some())
                    && let Some(imports) = ctx.saved_imports.get(&(producer.fi(), producer.index()))
                {
                    return Some(Either::Left(imports.iter().map(move |&node| Edge {
                        site: usage.site,
                        ..from.edge_to(from.jump(producer.fi(), node), EdgeKind::Imports)
                    })));
                }
                let owner = Owner::External(external_of(ctx, producer)?);
                let targets = extension_members(ctx, owner, call.member()?.sym(), usage.from_fi());
                return Some(Either::Right(call_edges(from, targets, usage.site)));
            };
            let class = match call.member().and_then(|m| m.child(C::Object)) {
                Some(object) if object.child(C::Member).is_some() => {
                    chain(ctx, object, &|_| Some(class))?
                }
                _ => class,
            };
            if call.is(C::Binding) && class.initializer().and_then(Cursor::rhs_callee).is_none() {
                return None;
            }
            let targets = match call.member() {
                Some(member) => member_targets(ctx, member, class),
                None => class.child_sym(C::Callable).map_or_else(
                    || vec![class],
                    |name| method_up(ctx, class, name, usage.from_fi()),
                ),
            };
            Some(Either::Right(call_edges(from, targets, usage.site)))
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
    if producer.typed().is_none()
        && let Some(rhs) = producer.bare_rhs()
    {
        return resolve_chain(ctx, rhs);
    }
    let callee = if producer.is(C::Binding) {
        resolve_chain(ctx, producer.typed()?)?
    } else {
        callee_of(ctx, producer)?
    };
    class_of(ctx, callee)
}

fn callee_of<'a>(ctx: &'a ResolveCtx, call: Cursor<'a>) -> Option<Cursor<'a>> {
    let local = ctx
        .call_at_site
        .get(&(call.fi(), call.index()))
        .map(|e| ctx.corpus.follow(e))
        .or_else(|| {
            let &(fi, node) = ctx.wave_callees.get(&(call.fi(), call.index()))?;
            Some(ctx.corpus.jump(fi, node))
        });
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
        infer_return_type(target).and_then(|sym| {
            enclosing_name(ctx, target, sym).or_else(|| visible_type(ctx, target.fi(), sym))
        })
    }
}

fn visible_type<'a>(ctx: &'a ResolveCtx, fi: u32, sym: u32) -> Option<Cursor<'a>> {
    let loc = ctx.visible[fi as usize]
        .get(&sym)
        .filter(|_| !ctx.ambiguous.contains(&(fi, sym)))?;
    Some(ctx.corpus.jump(loc.fi, loc.node))
}

thread_local! {
    static RESOLVING: std::cell::RefCell<FxHashSet<(u32, u32)>> = Default::default();
    static CUT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

type ChainMemo = std::sync::Mutex<FxHashMap<u32, Option<(u32, u32)>>>;

struct Resolving((u32, u32));

impl Resolving {
    fn enter(key: (u32, u32)) -> Option<Self> {
        let entered = RESOLVING.with_borrow_mut(|resolving| resolving.insert(key));
        CUT.set(CUT.get() || !entered);
        entered.then_some(Self(key))
    }
}

impl Drop for Resolving {
    fn drop(&mut self) {
        RESOLVING.with_borrow_mut(|resolving| resolving.remove(&self.0));
    }
}

fn resolve_chain<'a>(ctx: &'a ResolveCtx, c: Cursor<'a>) -> Option<Cursor<'a>> {
    let memo = || {
        ctx.chains[c.fi() as usize]
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    };
    if let Some(&found) = memo().get(&c.index()) {
        return found.map(|(fi, node)| ctx.corpus.jump(fi, node));
    }
    let _resolving = Resolving::enter((c.fi(), c.index()))?;
    if ctx.run.check().is_err() {
        CUT.set(true);
        return None;
    }
    let outer = CUT.replace(false);
    let found = stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || {
        chain(ctx, c, &|r| {
            resolve_reference(ctx, r)
                .or_else(|| enclosing_name(ctx, r, r.sym()))
                .or_else(|| visible_type(ctx, r.fi(), r.sym()).filter(|_| !imported_inline(r)))
        })
    });
    if !CUT.get() {
        memo().insert(c.index(), found.map(|f| (f.fi(), f.index())));
    }
    CUT.set(outer || CUT.get());
    found
}

fn resolve_reference<'a>(ctx: &'a ResolveCtx, reference: Cursor<'a>) -> Option<Cursor<'a>> {
    reachable((reference.fi(), reference.index()), |id| {
        ctx.references.get(&id).copied()
    })
    .skip(1)
    .map(|(fi, node)| ctx.corpus.jump(fi, node))
    .find(|node| node.is(C::Def))
}

/// A callee whose call carries an inline import of that very name, as a
/// `std::fs::FileTimes::new()` path call does: the import names the target,
/// so a same-named local def is not it.
fn imported_inline(callee: Cursor) -> bool {
    callee.parent().is_some_and(|call| {
        call.is(C::Call)
            && call
                .children()
                .filter(|child| child.is(C::Import))
                .any(|import| import.names().any(|name| name.sym() == callee.sym()))
    })
}

fn enclosing_defs<'a>(r: Cursor<'a>) -> impl Iterator<Item = (Cursor<'a>, bool)> {
    let mut child = r;
    r.ancestors().filter_map(move |up| {
        let via = child;
        child = up;
        let header =
            crate::canonical::is_canonical(via.kind()) && !via.is(C::Def) && !via.is(C::Scope);
        up.is(C::Def).then_some((up, header))
    })
}

fn enclosing_name<'a>(ctx: &'a ResolveCtx, at: Cursor<'a>, sym: u32) -> Option<Cursor<'a>> {
    enclosing_defs(at).find_map(|(d, header)| {
        enclosing_alias(ctx, d, sym).or_else(|| match header {
            true => None,
            false => enclosing_type(ctx, d, sym),
        })
    })
}

fn enclosing_alias<'a>(ctx: &'a ResolveCtx, d: Cursor<'a>, sym: u32) -> Option<Cursor<'a>> {
    if d.children_of(C::Alias).any(|a| a.sym() == sym) {
        return Some(d);
    }
    let bound = d
        .children()
        .filter(|c| c.is(C::Binding) && c.sym() == sym && c.children().count() == 1)
        .find_map(|c| c.child(C::SsaTyped))
        .filter(|bound| bound.sym() != sym)?;
    resolve_chain(ctx, bound)
}

fn enclosing_type<'a>(ctx: &'a ResolveCtx, d: Cursor<'a>, sym: u32) -> Option<Cursor<'a>> {
    let plain = |node: Cursor<'a>| node.is_class() && !node.has(C::ImplBlock);
    if plain(d) && d.child_sym(C::DefName) == Some(sym) {
        return Some(d);
    }
    let node = *members_of(ctx, d).get(&sym)?;
    Some(d.jump(d.fi(), node)).filter(|member| plain(*member))
}

fn chain_head(c: Cursor<'_>) -> Cursor<'_> {
    let c = c.reference();
    let member = c.has(C::Object).then_some(c).or_else(|| c.child(C::Member));
    match member.and_then(|m| m.child(C::Object)) {
        Some(object) => chain_head(object),
        None => c,
    }
}

fn chain<'a>(
    ctx: &'a ResolveCtx,
    c: Cursor<'a>,
    root: &dyn Fn(Cursor<'a>) -> Option<Cursor<'a>>,
) -> Option<Cursor<'a>> {
    let c = c.reference();
    let Some(m) = c.has(C::Object).then_some(c).or_else(|| c.child(C::Member)) else {
        return value_type(ctx, root(c)?);
    };
    let object = m.child(C::Object)?;
    let receiver = stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || chain(ctx, object, root))?;
    let targets = member_targets(ctx, m, receiver);
    let member = targets.into_iter().exactly_one().ok()?;
    value_type(ctx, member)
}

fn value_type<'a>(ctx: &'a ResolveCtx, d: Cursor<'a>) -> Option<Cursor<'a>> {
    if d.has(C::EnumVariant) {
        d.enclosing_def(&[C::Enum])
    } else if d.has(C::TypeAlias) {
        match d.child(C::SsaTyped) {
            Some(aliased) => resolve_chain(ctx, aliased),
            None => Some(d),
        }
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

fn partial_key(d: Cursor, packages: &[FxHashMap<u32, u32>]) -> Option<(u32, u32, u32)> {
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

type Members = (
    FxHashMap<(u32, u32, u32), Vec<Loc>>,
    FxHashMap<u32, Vec<Loc>>,
);

fn gather_members(trees: &[Tree], packages: &[FxHashMap<u32, u32>]) -> Members {
    let (mut parts, mut extensions) = Members::default();
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

fn supertypes<'a>(ctx: &'a ResolveCtx, cls: Cursor<'a>) -> Vec<(u32, u32)> {
    std::iter::once(cls)
        .chain(impl_blocks_of(ctx, cls))
        .flat_map(|part| {
            let linked = ctx.extends_of.get(&(part.fi(), part.index()));
            part.children()
                .filter(|s| s.is(C::SuperType))
                .filter_map(|s| resolve_chain(ctx, s))
                .map(|c| (c.fi(), c.index()))
                .chain(linked.into_iter().flatten().copied())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn impl_blocks_of<'a>(ctx: &'a ResolveCtx, cls: Cursor<'a>) -> impl Iterator<Item = Cursor<'a>> {
    let owner = cls
        .child(C::DefName)
        .filter(|_| cls.has(C::ImplBlock))
        .and_then(|name| resolve_reference(ctx, name))
        .unwrap_or(cls);
    std::iter::once(owner)
        .chain(
            ctx.implementations
                .get(&(owner.fi(), owner.index()))
                .into_iter()
                .flatten()
                .map(|&(fi, node)| ctx.corpus.jump(fi, node)),
        )
        .filter(move |node| (node.fi(), node.index()) != (cls.fi(), cls.index()))
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
    let above: FxHashMap<_, _> = common.iter().map(|&u| (u, ancestors(u))).collect();
    let mut least = common
        .iter()
        .filter(|t| !common.iter().any(|u| u != *t && above[u].contains(*t)));
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
                stacker::maybe_grow(RED_ZONE, STACK_SEGMENT, || branch_type(ctx, tail))
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

fn member_targets<'a>(
    ctx: &'a ResolveCtx,
    member: Cursor<'a>,
    receiver: Cursor<'a>,
) -> Vec<Cursor<'a>> {
    let Some(constraint) = member.child(C::Dispatch) else {
        return method_up(ctx, receiver, member.sym(), member.fi() as usize);
    };
    let Some(contract) = resolve_reference(ctx, constraint)
        .or_else(|| {
            qualified(ctx, constraint.sym(), &|name| {
                visible_type(ctx, constraint.fi(), name)
            })
        })
        .filter(|owner| owner.is_dispatch_contract())
    else {
        return Vec::new();
    };
    let mut implementations = std::iter::once(receiver)
        .chain(impl_blocks_of(ctx, receiver))
        .filter(|owner| {
            owner
                .children_of(C::SuperType)
                .filter_map(|constraint| resolve_chain(ctx, constraint))
                .any(|target| (target.fi(), target.index()) == (contract.fi(), contract.index()))
        });
    let Some(implementation) = implementations.next() else {
        return Vec::new();
    };
    if implementations.next().is_some() {
        return Vec::new();
    }
    find_method_in(implementation, member.sym())
        .or_else(|| {
            find_method_in(contract, member.sym()).filter(|method| !method.has(C::Declaration))
        })
        .into_iter()
        .collect()
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
        .filter(|l| l.fi as usize == fi || ctx.exporters[fi].contains(&l.fi))
        .filter_map(|l| extends_owner(ctx.corpus.jump(l.fi, l.node)))
        .collect()
}

fn declared_member<'a>(ctx: &'a ResolveCtx, cls: Cursor<'a>, name: u32) -> Option<Cursor<'a>> {
    let parts = partial_key(cls, ctx.packages).and_then(|k| ctx.partials.get(&k));
    let parts = parts.into_iter().flatten().map(|l| cls.jump(l.fi, l.node));
    std::iter::once(cls)
        .chain(impl_blocks_of(ctx, cls))
        .chain(parts.filter(|d| (d.fi(), d.index()) != (cls.fi(), cls.index())))
        .find_map(|owner| {
            if owner.is(C::Def) {
                members_of(ctx, owner)
                    .get(&name)
                    .map(|&node| owner.jump(owner.fi(), node))
            } else {
                find_method_in(owner, name)
            }
        })
}

/// Cross-file edges for one file that the import pass cannot produce:
/// inheritance, decorators, destructuring, and member calls on imported types.
fn resolve_file(ctx: &ResolveCtx, fi: usize) -> Result<Vec<Edge>, Killed> {
    let file = Sentinel::new("resolve", &ctx.trees[fi].label, ctx.file_resolve_ms).on_thread_cpu();
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
    let builtins = &ctx.env.rules_for(&ctx.trees[fi].label).config.link.builtins;
    let unbound = |from: Cursor, sym: u32| -> Vec<Edge> {
        let wild = wild
            .iter()
            .filter(|_| !named.contains(&sym) && !builtins.contains(&sym));
        wild.map(|&w| from.edge_to(from.jump(fi as u32, w), EdgeKind::Imports))
            .collect()
    };
    let cross = |c: Cursor| c.fi() != fi as u32;
    let mut bound_in: FxHashMap<u32, FxHashSet<u32>> = FxHashMap::default();

    let mut path: Vec<(u32, Option<Cursor>)> = Vec::new();
    for node in root.descendants() {
        ctx.run.check().and_then(|()| file.check())?;
        let parent = node.parent().expect("a descendant has a parent");
        while path.last().is_some_and(|&(id, _)| id != parent.index()) {
            path.pop();
        }
        let enclosing = match parent.is(C::Def) {
            true => Some(parent),
            false => path.last().and_then(|&(_, enclosing)| enclosing),
        };
        path.push((node.index(), enclosing));
        let Some(from) = Some(node).filter(|n| n.is(C::Def)).or(enclosing) else {
            continue;
        };
        if node.is(C::Def) {
            for dec in node.children_of(C::Decorator) {
                match resolve_chain(ctx, dec) {
                    Some(t) if cross(t) => out.push(node.edge_to(t, EdgeKind::Calls)),
                    Some(_) => {}
                    None => out.extend(unbound(node, dec.sym())),
                }
            }
            let parents: Vec<Cursor> = node
                .children_of(C::SuperType)
                .filter_map(|s| resolve_chain(ctx, s))
                .filter(|p| cross(*p))
                .collect();
            out.extend(parents.iter().map(|p| node.edge_to(*p, EdgeKind::Extends)));
            if !parents.is_empty() {
                out.extend(inherited_calls(ctx, node, &parents, fi));
            }
        } else if node.is(C::Destructure) {
            out.extend(destructure_calls(ctx, node, from, fi));
        } else if let Some(m) = node.member().filter(|m| m.sym_opt().is_some()) {
            let Some(object) = m.child(C::Object) else {
                continue;
            };
            let Some(obj) = object.reference().chain_root().sym_opt() else {
                continue;
            };
            match resolve_chain(ctx, object) {
                Some(target)
                    if m.child(C::Dispatch)
                        .is_some_and(|dispatch| !dispatch.has(C::Object)) =>
                {
                    out.extend(call_edges(
                        from,
                        member_targets(ctx, m, target),
                        Some(node.index()),
                    ));
                }
                Some(target) if m.has(C::Dispatch) && target.is_dispatch_contract() => {}
                Some(target) if cross(target) && target.is_class() => {
                    let members = method_up(ctx, target, m.sym(), fi);
                    out.extend(call_edges(from, members, Some(node.index())));
                }
                None => {
                    let bound = bound_in.entry(from.index()).or_insert_with(|| {
                        from.descendants()
                            .filter(|b| b.is(C::Binding))
                            .filter_map(|b| b.sym_opt())
                            .collect()
                    });
                    if !bound.contains(&obj) {
                        out.extend(unbound(from, obj));
                    }
                }
                _ => {}
            }
        }
    }
    Ok(out)
}

/// Unqualified and self calls inside `class` that reach a member `class` does
/// not declare itself, resolved on each cross-file parent.
fn inherited_calls<'a>(
    ctx: &'a ResolveCtx,
    class: Cursor<'a>,
    parents: &[Cursor<'a>],
    fi: usize,
) -> Vec<Edge> {
    let own: FxHashSet<u32> = class
        .descendants_pruned(|n| n.index() != class.index() && n.is_class() && !n.has(C::ImplBlock))
        .filter(|n| n.is(C::Def))
        .filter_map(|n| n.child_sym(C::DefName))
        .collect();
    let mut out = Vec::new();
    for call in class.calls() {
        let owned = call
            .enclosing(|e| e.is_class())
            .is_some_and(|o| o.index() == class.index());
        let callee = call.child(C::Callee).and_then(|k| {
            k.child_sym(C::Ivar)
                .or_else(|| call.has_tag(ctx.tags.implicit_self).then(|| k.sym()))
        });
        let from = call.enclosing(|e| e.is(C::Def)).filter(|_| owned);
        let Some((from, name)) = from.zip(callee) else {
            continue;
        };
        if own.contains(&name) {
            continue;
        }
        for parent in parents {
            let site = Some(call.index());
            out.extend(call_edges(from, method_up(ctx, *parent, name, fi), site));
        }
    }
    out
}

/// Each slot of a destructuring pattern reads the matching positional
/// component of the destructured type.
fn destructure_calls<'a>(
    ctx: &'a ResolveCtx,
    d: Cursor<'a>,
    from: Cursor<'a>,
    fi: usize,
) -> Vec<Edge> {
    let typed_local = |k: Cursor| {
        let s = k.sym_opt()?;
        let binding = from
            .descendants()
            .find(|b| b.is(C::Binding) && b.sym_opt() == Some(s))?;
        resolve_chain(ctx, binding.typed()?)
    };
    let class = match (d.child(C::Call), d.child(C::Rhs)) {
        (Some(pattern), _) => pattern.child(C::Callee).and_then(|k| resolve_chain(ctx, k)),
        (None, Some(value)) => match value.child(C::Call) {
            Some(call) => producer_class(ctx, call),
            None => typed_local(value),
        },
        (None, None) => None,
    };
    let Some(class) = class else {
        return vec![];
    };
    let slots: Vec<Cursor> = d
        .children()
        .filter(|s| s.is(C::Binding) || s.is(C::Destructure))
        .collect();
    let components: Vec<Cursor> = class
        .child(C::Positional)
        .map(|p| p.children().filter(|c| c.is(C::Binding)).collect())
        .unwrap_or_default();
    if components.len() != slots.len() {
        return vec![];
    }
    slots
        .iter()
        .zip(components)
        .flat_map(|(slot, component)| {
            call_edges(
                from,
                method_up(ctx, class, component.sym(), fi),
                Some(slot.index()),
            )
        })
        .collect()
}

fn build_file_index(trees: &[Tree], env: &Env, index_names: &[String]) -> FileIndex {
    let (lang, support_lang) = (&env.lang, env.lang_id);
    let mut idx = FileIndex::default();
    let mut module_defs = Vec::new();
    let sep = support_lang.fqn_separator();
    for (fi, (tree, path)) in trees.iter().map(|t| (t, t.label.as_str())).enumerate() {
        let file_lang = SupportLang::from_path(path).unwrap_or(support_lang);
        let stem = file_lang.strip_extension(path);
        let file = Loc::new(fi, 0);
        idx.insert(path.to_string(), file);
        idx.insert(stem.to_string(), file);
        let mut module = stem;
        for name in index_names {
            if let Some(pkg) = stem
                .strip_suffix(name.as_str())
                .and_then(|s| s.strip_suffix(PATH_SEP))
            {
                if !pkg.is_empty() {
                    idx.insert(pkg.to_string(), file);
                }
                module = pkg;
            } else if stem == name.as_str() {
                idx.insert(String::new(), file);
                module = "";
            }
        }
        let root = tree.root();
        let mut clauses = root
            .children()
            .filter(|c| c.is(C::Package))
            .filter_map(|c| c.sym_opt())
            .map(|s| lang.syms.resolve(s));
        let pkg = match env.rules_for(path).config.resolve.nested_packages {
            false => clauses.next().map(str::to_owned),
            true => Some(clauses.collect::<Vec<_>>())
                .filter(|c| !c.is_empty())
                .map(|c| c.join(sep)),
        }
        .map_or(String::new(), |pkg| pkg.replace(sep, PATH_SEP) + PATH_SEP);
        let defs = root.children().filter(|d| d.is(C::Def));
        for (name, d) in defs.filter_map(|d| Some((d.child_sym(C::DefName)?, d))) {
            let name = lang.syms.resolve(name).replace(sep, PATH_SEP);
            if pkg.is_empty() && !module.is_empty() {
                module_defs.push((format!("{module}{PATH_SEP}{name}"), Loc::new(fi, d.index())));
            }
            idx.insert(format!("{pkg}{name}"), Loc::new(fi, d.index()));
        }
    }
    for (key, loc) in module_defs {
        idx.insert_key(key, loc);
    }
    idx
}

fn is_external(source_str: &str, external: &[String]) -> bool {
    external.iter().any(|module| {
        source_str == module
            || source_str
                .strip_prefix(module.as_str())
                .is_some_and(|suffix| suffix.starts_with(PATH_SEP))
    })
}

fn resolve_submodule(
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
        .first()
        .map(|loc| loc.fi as usize)
}

/// `@/*` -> `src/*` rewrites a prefix; a key without `*` matches whole path
/// components only, so `app` never matches `application/x`.
fn apply_alias(path: &str, pattern: &str, replacement: &str) -> Option<String> {
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

#[cfg(test)]
mod remap_tests {
    use super::*;

    /// `a` sees a name from `b`, `c` sees a name from `a`. Dropping `b`
    /// must report `a` and leave `c` alone, however the name got there.
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

    #[test]
    fn a_panic_while_resolving_releases_the_name() {
        let key = (0, 7);
        let panicked = std::panic::catch_unwind(|| {
            let _resolving = Resolving::enter(key);
            panic!("resolution failed");
        });
        assert!(panicked.is_err());
        assert!(Resolving::enter(key).is_some());
    }
}
