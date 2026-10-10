use either::Either;
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

use crate::canonical::Canonical as C;
use crate::constants::{PATH_SEP, WILDCARD};
use crate::env::Env;
use crate::file_tree::LookupConfig;
use crate::intern::Lang;
use crate::rules::ResolveConfig;
use crate::sentinel::{Killed, Sentinel};
use crate::tags::ReservedTags;
use crate::tree::{
    CallResolution, Cursor, Edge, EdgeKind, Tree, find_method_in, infer_return_type,
    members_by_level, reachable,
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
    pub import_lookups: ImportLookupStats,
    pub cross_edges: Vec<Edge>,
    /// Files whose cross-file pass overran its budget; their edges are absent.
    pub killed: Vec<Killed>,
    pub resolved_source_paths: Vec<ResolvedSourcePath>,
    /// How long each file's cross-file pass took.
    pub file_timings: Vec<(u32, Duration)>,
}

#[derive(Default, Debug)]
pub struct ImportLookupStats {
    pub sites: usize,
    pub unique_contexts: usize,
    pub provider_contexts: usize,
    pub module_searches: usize,
    pub module_searches_avoided: usize,
}

enum ImportTarget {
    External,
    Search {
        path: String,
        targets: Vec<Loc>,
        allow_submodules: bool,
    },
}

#[derive(Default)]
pub struct FileIndex {
    /// A file, or a top-level def in one when the key is the def's name.
    keys: FxHashMap<String, Loc>,
    dirs: FxHashMap<String, Vec<u32>>,
}

impl FileIndex {
    fn insert(&mut self, key: String, loc: Loc) {
        let dir = key.rsplit_once(PATH_SEP).map_or("", |(d, _)| d);
        self.dirs.entry(dir.to_string()).or_default().push(loc.fi);
        self.keys.entry(key).or_insert(loc);
    }

    fn get(&self, key: &str) -> Option<Loc> {
        self.keys.get(key).copied()
    }

    fn insert_entrypoints(&mut self, entrypoints: &[(String, String)]) {
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
        let mut invalidated: FxHashSet<u32> = self
            .reqs
            .iter()
            .filter(|req| {
                self.lookup != *lookup
                    || !resolve_glob(&req.target_path, &self.file_index, &lookup.prefixes)
                        .contains(&Loc::new(req.target_fi as usize, req.anchor))
            })
            .map(|req| req.fi)
            .chain(dirty_fis.iter().copied())
            .collect();
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
            let (discovered, _, _) = gather_imports_for(
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
        let previous_index = self.rebuild_file_index(trees, env, entrypoints);
        let dirty_fis = &self.invalidate(&previous_index, trees, dirty_fis, lookup, env);
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
        let (new_reqs, mut cross_edges, import_lookups) =
            gather_imports_for(trees, lang, &self.file_index, lookup, config, dirty_fis);
        self.reqs.extend(new_reqs);
        let ambiguous = propagate_reexports(
            trees,
            &self.reqs,
            &mut self.visible,
            self.wildcard_sym,
            self.tags,
            config.merge_same_named_types,
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

        let (partials, extensions) = gather_members(trees, config.merge_same_named_types);
        let declared_members: Vec<FxHashMap<(u32, u32), u32>> = trees
            .par_iter()
            .map(|tree| {
                let mut members = FxHashMap::default();
                for member in tree.root().descendants().filter(|node| node.is(C::Def)) {
                    let Some(name) = member.child_sym(C::DefName) else {
                        continue;
                    };
                    for owner in member.ancestors() {
                        if owner.is(C::Def) {
                            members
                                .entry((owner.index(), name))
                                .or_insert(member.index());
                        }
                        if owner.is_class() && !owner.has(C::ImplBlock) {
                            break;
                        }
                    }
                }
                members
            })
            .collect();
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
            merge_types: config.merge_same_named_types,
            partials: &partials,
            extensions: &extensions,
            declared_members: &declared_members,
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
        run.check()?;
        let (per_file, killed): (Vec<Vec<Edge>>, Vec<Killed>) =
            outcomes.into_iter().partition_map(|r| match r {
                Ok(v) => Either::Left(v),
                Err(k) => Either::Right(k),
            });
        let wave1: Vec<Edge> = self
            .reqs
            .par_iter()
            .filter(|r| dirty_fis.contains(&r.fi) || dirty_fis.contains(&r.target_fi))
            .flat_map(|req| resolve_one_import(&ctx, req))
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
        cross_edges.extend(&wave1);
        run.check()?;

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
        let wildcard_edges: Vec<Edge> = self
            .reqs
            .par_iter()
            .filter(|r| dirty_fis.contains(&r.fi) || dirty_fis.contains(&r.target_fi))
            .flat_map_iter(|req| {
                let import = ctx.corpus.jump(req.fi, req.node);
                import
                    .names()
                    .filter(|n| wildcard_or_name(ctx.wildcard_sym, *n) == ctx.wildcard_sym)
                    .filter_map(|n| referrers.get(&(n.fi(), n.index())).map(|r| (n, r)))
                    .flat_map(|(n, r)| wildcard_uses(&ctx, req, n, r))
                    .collect::<Vec<_>>()
            })
            .collect();
        cross_edges.extend(&wildcard_edges);
        run.check()?;

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
            .filter(|e| e.kind == EdgeKind::Calls && dirty_fis.contains(&e.from_tree))
            .copied()
            .collect();
        let key = |e: &Edge| (e.from_tree, e.from_node, e.site, e.to_tree, e.to_node);
        seen.extend(edges.iter().filter(|e| e.kind == EdgeKind::Calls).map(key));
        cross_edges.retain(|e| e.kind != EdgeKind::Calls || seen.insert(key(e)));
        let mut type_edges: Vec<Edge> = Vec::new();
        while !wave.is_empty() {
            run.check()?;
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

        Ok(ResolveResult {
            import_lookups,
            cross_edges,
            killed,
            resolved_source_paths,
            file_timings,
        })
    }
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
    merge_types: bool,
    partials: &'a FxHashMap<(u32, u32, u32), Vec<Loc>>,
    extensions: &'a FxHashMap<u32, Vec<Loc>>,
    declared_members: &'a [FxHashMap<(u32, u32), u32>],
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
    lang: &Lang,
    file_index: &FileIndex,
    lookup: &LookupConfig,
    config: &ResolveConfig,
    dirty_fis: &FxHashSet<u32>,
) -> (Vec<ImportReq>, Vec<Edge>, ImportLookupStats) {
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
    let context_key = |cur: Cursor| {
        Some((
            cur.tag(tags.original_source_path)
                .or_else(|| cur.child_sym(C::SourcePath))?,
            cur.tag(resolved_tag_key)?,
            cur.tag(tags.alias_scope),
        ))
    };
    let mut stats = ImportLookupStats::default();
    let mut contexts = FxHashMap::default();
    let mut import_sites = Vec::new();
    for &fi in dirty_fis {
        let mut sites = Vec::new();
        for import in trees[fi as usize]
            .root()
            .descendants()
            .filter(|cur| cur.is(C::Import) || cur.is(C::ImportType))
        {
            if let Some(key) = context_key(import) {
                stats.sites += 1;
                *contexts.entry(key).or_insert(0usize) += 1;
                sites.push((import.index(), key));
            }
        }
        import_sites.push((fi, sites));
    }
    stats.unique_contexts = contexts.len();
    let mut classified = FxHashMap::default();
    for (key @ (source, resolved, scope), sites) in contexts {
        let mut searches = 0;
        let source = lang.syms.resolve(source);
        let mapped = lookup.aliases.iter().find_map(|alias| {
            if alias.scope.is_some() && alias.scope != scope {
                return None;
            }
            let path = apply_alias(source, &alias.pattern, &alias.replacement)?;
            if alias.if_exists {
                searches += 1;
                if resolve_glob(&path, file_index, &[]).is_empty() {
                    return None;
                }
            }
            Some(path)
        });
        let runtime = mapped.is_none() && is_external(source, &config.external);
        let standard = mapped.is_none() && is_external(source, &config.stdlib);
        stats.provider_contexts += usize::from(runtime || standard);
        let target = if runtime {
            ImportTarget::External
        } else {
            let path = mapped.unwrap_or_else(|| lang.syms.resolve(resolved).to_owned());
            let prefixes = if standard {
                &stdlib_prefixes
            } else {
                &lookup.prefixes
            };
            searches += 1;
            let targets = resolve_glob(&path, file_index, prefixes);
            ImportTarget::Search {
                path,
                targets,
                allow_submodules: !standard,
            }
        };
        stats.module_searches += searches;
        stats.module_searches_avoided += searches * (sites - 1);
        classified.insert(key, target);
    }
    let per_tree: Vec<(Vec<ImportReq>, Vec<Edge>)> = import_sites
        .par_iter()
        .map(|(fi, sites)| {
            let fi = *fi;
            let tree = &trees[fi as usize];
            let mut reqs = Vec::new();
            let mut edges = Vec::new();
            for &(node_idx, key) in sites {
                let cur = tree.cursor(node_idx);
                let Some(ImportTarget::Search {
                    path: target_path,
                    targets,
                    allow_submodules,
                }) = classified.get(&key)
                else {
                    continue;
                };
                let direct: Vec<_> = targets.iter().copied().filter(|loc| loc.fi != fi).collect();
                if direct.is_empty() && !allow_submodules {
                    continue;
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
    (all_reqs, all_edges, stats)
}

fn propagate_reexports(
    trees: &[Tree],
    reqs: &[ImportReq],
    visible: &mut VisibleMap,
    wildcard_sym: u32,
    tags: ReservedTags,
    merge_types: bool,
) -> FxHashSet<(u32, u32)> {
    let mut ambiguous: FxHashSet<(u32, u32)> = FxHashSet::default();

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
    loop {
        let mut new_exports = Vec::new();
        let mut export = |fi: usize, name, loc| {
            if !visible[fi].contains_key(&name) {
                new_exports.push((fi, name, loc));
            }
        };
        for req in reqs {
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
        for (fi, ns, loc) in new_exports {
            if let Some(&existing) = visible[fi].get(&ns) {
                let key = |l: Loc| partial_key(trees[l.fi as usize].cursor(l.node), merge_types);
                if existing != loc && (key(existing).is_none() || key(existing) != key(loc)) {
                    ambiguous.insert((u32::try_from(fi).expect("file index exceeds u32"), ns));
                }
                continue;
            }
            visible[fi].insert(ns, loc);
        }
    }
    ambiguous
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
    let root = |r: Cursor<'_>| qualified(ctx, r.sym(), &provided);
    for from in referrers.iter().map(|&n| ctx.corpus.jump(fi as u32, n)) {
        for call in from.calls() {
            let Some(target) = call.child(C::Callee).and_then(|c| chain(ctx, c, &root)) else {
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
                    reaching
                        .iter()
                        .map(|e| producer_class(ctx, ctx.corpus.follow(e))),
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
        .filter(|_| !ctx.ambiguous.contains(&(fi, sym)))?;
    Some(ctx.corpus.jump(loc.fi, loc.node))
}

fn resolve_chain<'a>(ctx: &'a ResolveCtx, c: Cursor<'a>) -> Option<Cursor<'a>> {
    chain(ctx, c, &|r| {
        resolve_reference(ctx, r)
            .or_else(|| enclosing_alias(ctx, r))
            .or_else(|| visible_type(ctx, r.fi(), r.sym()))
    })
}

fn resolve_reference<'a>(ctx: &'a ResolveCtx, reference: Cursor<'a>) -> Option<Cursor<'a>> {
    reachable((reference.fi(), reference.index()), |id| {
        ctx.references.get(&id).copied()
    })
    .skip(1)
    .map(|(fi, node)| ctx.corpus.jump(fi, node))
    .find(|node| node.is(C::Def))
}

/// `Self` in `impl Service { fn new() -> Self }` names the enclosing def;
/// `T` in `fn f<T: Pinger>(t: T)` is the type parameter bound in it.
fn enclosing_alias<'a>(ctx: &'a ResolveCtx, r: Cursor<'a>) -> Option<Cursor<'a>> {
    let sym = r.sym();
    r.ancestors().filter(|a| a.is(C::Def)).find_map(|d| {
        if d.children_of(C::Alias).any(|a| a.sym() == sym) {
            return Some(d);
        }
        let bound = d
            .children()
            .filter(|c| c.is(C::Binding) && c.sym() == sym && c.children().count() == 1)
            .find_map(|c| c.child(C::SsaTyped))
            .filter(|bound| bound.sym() != sym)?;
        resolve_chain(ctx, bound)
    })
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
    let receiver = chain(ctx, m.child(C::Object)?, root)?;
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

fn partial_key(d: Cursor, merge_types: bool) -> Option<(u32, u32, u32)> {
    if !merge_types || !d.is_class() {
        return None;
    }
    let pkg = d
        .ancestors()
        .find_map(|a| a.children().find_map(|c| c.child_sym(C::Package)));
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

fn gather_members(trees: &[Tree], merge_types: bool) -> Members {
    let (mut parts, mut extensions) = Members::default();
    for (fi, tree) in trees.iter().enumerate() {
        for d in tree.root().descendants().filter(|d| d.is(C::Def)) {
            let Some(name) = d.child_sym(C::DefName) else {
                continue;
            };
            let nested = d.ancestors().any(|a| a.is(C::Def) && !a.has(C::ImplBlock));
            if nested {
                continue;
            }
            let loc = Loc::new(fi, d.index());
            if let Some(key) = partial_key(d, merge_types) {
                parts.entry(key).or_default().push(loc);
            }
            let wrapped = d
                .parent()
                .is_some_and(|p| p.is(C::Def) && p.has(C::ImplBlock));
            if wrapped {
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
    let parts = partial_key(cls, ctx.merge_types).and_then(|k| ctx.partials.get(&k));
    let parts = parts.into_iter().flatten().map(|l| cls.jump(l.fi, l.node));
    std::iter::once(cls)
        .chain(impl_blocks_of(ctx, cls))
        .chain(parts.filter(|d| (d.fi(), d.index()) != (cls.fi(), cls.index())))
        .find_map(|owner| {
            if owner.is(C::Def) {
                ctx.declared_members[owner.fi() as usize]
                    .get(&(owner.index(), name))
                    .map(|&node| owner.jump(owner.fi(), node))
            } else {
                find_method_in(owner, name)
            }
        })
}

/// Cross-file edges for one file that the import pass cannot produce:
/// inheritance, decorators, destructuring, and member calls on imported types.
fn resolve_file(ctx: &ResolveCtx, fi: usize) -> Result<Vec<Edge>, Killed> {
    let file = Sentinel::new("resolve", &ctx.trees[fi].label, ctx.file_resolve_ms);
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

    for node in root.descendants() {
        ctx.run.check().and_then(|()| file.check())?;
        let Some(from) = Some(node)
            .filter(|n| n.is(C::Def))
            .or_else(|| node.enclosing(|e| e.is(C::Def)))
        else {
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

fn resolve_glob(target: &str, idx: &FileIndex, prefixes: &[String]) -> Vec<Loc> {
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

fn is_external(source_str: &str, external: &[String]) -> bool {
    external.iter().any(|module| {
        source_str == module
            || source_str
                .strip_prefix(module.as_str())
                .is_some_and(|suffix| suffix.starts_with(PATH_SEP))
    })
}

fn resolve_path(target: &str, file_index: &FileIndex, prefixes: &[String]) -> Option<Loc> {
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
