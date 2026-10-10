use either::Either;
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::sync::Arc;
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
    CallResolution, Compact, Edge, EdgeKind, find_method_in, infer_return_type, reachable,
};
type Tree = crate::tree::Tree<Compact>;
type Cursor<'a> = crate::tree::Cursor<'a, Compact>;
use crate::Error;
use crate::tree::{TreeRepository, TreeScan, TreeSession};
use crate::treesitter::SupportLang;

pub const CLASS_LIKE: &[C] = &[
    C::Class,
    C::Struct,
    C::ImplBlock,
    C::Interface,
    C::Trait,
    C::Enum,
];

type VisibleMap = Vec<Arc<FxHashMap<u32, Loc>>>;

fn share_visible(visible: &mut VisibleMap) {
    use std::hash::{Hash, Hasher};
    let mut shared = FxHashMap::<(usize, u64), Vec<Arc<FxHashMap<u32, Loc>>>>::default();
    for names in visible {
        let hash = names.iter().fold(0u64, |sum, entry| {
            let mut hash = rustc_hash::FxHasher::default();
            entry.hash(&mut hash);
            sum.wrapping_add(hash.finish())
        });
        let candidates = shared.entry((names.len(), hash)).or_default();
        if let Some(existing) = candidates.iter().find(|existing| ***existing == **names) {
            *names = existing.clone();
        } else {
            candidates.push(names.clone());
        }
    }
}

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
    /// Files whose cross-file pass overran its budget; their edges are absent.
    pub killed: Vec<Killed>,
    pub resolved_source_paths: Vec<ResolvedSourcePath>,
    /// How long each file's cross-file pass took.
    pub file_timings: Vec<(u32, Duration)>,
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
    fn exports<'a>(
        &self,
        trees: TreeRepository<'_>,
        visible: &'a VisibleMap,
    ) -> Result<Cow<'a, FxHashMap<u32, Loc>>, Error> {
        if self.anchor == 0 {
            return Ok(Cow::Borrowed(&visible[self.target_fi as usize]));
        }
        let mut names = (*visible[self.target_fi as usize]).clone();
        let tree = trees.acquire(self.target_fi as usize)?;
        let module = tree.cursor(self.anchor);
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
        Ok(Cow::Owned(names))
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

    pub fn from_parts(
        visible: Vec<FxHashMap<u32, Loc>>,
        reqs: Vec<ImportReq>,
        lang: &Lang,
    ) -> Self {
        Self {
            visible: visible.into_iter().map(Arc::new).collect(),
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
        trees: TreeRepository<'_>,
        env: &Env,
        entrypoints: &[(String, String)],
    ) -> Result<FileIndex, Error> {
        let mut index = build_file_index(trees, &env.lang, env.lang_id, env.lang_id.index_names())?;
        index.insert_entrypoints(entrypoints);
        Ok(std::mem::replace(&mut self.file_index, index))
    }

    fn invalidate(
        &self,
        previous_index: &FileIndex,
        trees: TreeRepository<'_>,
        dirty_fis: &FxHashSet<u32>,
        lookup: &LookupConfig,
        env: &Env,
    ) -> Result<FxHashSet<u32>, Error> {
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
            let (discovered, _) = gather_imports_for(
                trees,
                &env.lang,
                &self.file_index,
                lookup,
                &env.resolve.config,
                &discovery_files,
            )?;
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
        Ok(invalidated)
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
        let mut remapped: VisibleMap = (0..n).map(|_| Arc::default()).collect();
        let mut lost_a_name = FxHashSet::default();
        for (old_fi, names) in self.visible.iter().enumerate() {
            let Some(&new_fi) = old_labels
                .get(old_fi)
                .and_then(|l| label_to_fi.get(l.as_str()))
            else {
                continue;
            };
            for (&sym, &loc) in names.iter() {
                match old_labels
                    .get(loc.fi as usize)
                    .and_then(|l| label_to_fi.get(l.as_str()))
                {
                    Some(&target_new_fi) => {
                        Arc::make_mut(&mut remapped[new_fi as usize])
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
    pub(crate) fn resolve(
        &mut self,
        trees: TreeRepository<'_>,
        edges: &mut Vec<Edge>,
        lang: &Lang,
        dirty_fis: &FxHashSet<u32>,
        support_lang: SupportLang,
        lookup: &LookupConfig,
        config: &ResolveConfig,
        entrypoints: &[(String, String)],
        env: &Env,
        run: &Sentinel,
    ) -> Result<ResolveResult, Error> {
        let index_names = support_lang.index_names();
        let trees = trees.with_run(run);
        let previous_index = self.rebuild_file_index(trees, env, entrypoints)?;
        let dirty_fis = &self.invalidate(&previous_index, trees, dirty_fis, lookup, env)?;
        drop(previous_index);
        self.lookup = lookup.clone();
        edges.retain(|edge| edge.from_tree == edge.to_tree || !dirty_fis.contains(&edge.from_tree));

        self.visible.resize_with(trees.len(), Default::default);
        for &fi in dirty_fis {
            let fi = fi as usize;
            if fi < trees.len() {
                self.visible[fi] = Arc::new(gather_visible_one(
                    &*trees.acquire(fi)?,
                    fi,
                    self.tags.exports,
                ));
            }
        }
        self.reqs.retain(|r| !dirty_fis.contains(&r.fi));
        let (new_reqs, mut cross_edges) =
            gather_imports_for(trees, lang, &self.file_index, lookup, config, dirty_fis)?;
        self.reqs.extend(new_reqs);
        let ambiguous = propagate_reexports(
            trees,
            &self.reqs,
            &mut self.visible,
            self.wildcard_sym,
            self.tags,
            config.merge_same_named_types,
        )?;
        share_visible(&mut self.visible);
        let mut scan = TreeScan::new(trees);
        let resolved_source_paths: Vec<ResolvedSourcePath> = self
            .reqs
            .iter()
            .filter(|req| dirty_fis.contains(&req.fi))
            .map(|req| -> Result<_, Error> {
                let sym = lang.syms.intern(&req.target_path);
                let tree = scan.acquire(req.fi as usize)?;
                Ok(tree
                    .cursor(req.node)
                    .child(C::SourcePath)
                    .map(|node| ResolvedSourcePath {
                        fi: req.fi,
                        node: node.index(),
                        sym,
                    }))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
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

        let (partials, extensions) = gather_members(trees, config.merge_same_named_types)?;
        let (declared_members, definitions): (Vec<_>, Vec<_>) = (0..trees.len())
            .into_par_iter()
            .map(|fi| -> Result<_, Error> {
                let tree = trees.acquire(fi)?;
                let mut members = FxHashMap::default();
                let mut definitions = FxHashMap::default();
                for member in tree.root().descendants().filter(|node| node.is(C::Def)) {
                    definitions.insert(
                        member.index(),
                        (member.is_class(), member.has_tag(self.tags.non_callable)),
                    );
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
                Ok((members, definitions))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .unzip();
        let exporters: Vec<FxHashSet<u32>> = self
            .visible
            .iter()
            .map(|names| {
                if extensions.is_empty() {
                    FxHashSet::default()
                } else {
                    names.values().map(|l| l.fi).collect()
                }
            })
            .collect();
        let imports: Vec<FxHashMap<u32, (u32, u32)>> = (0..trees.len())
            .map(|fi| -> Result<_, Error> {
                let t = trees.acquire(fi)?;
                let imports = t.root().descendants().filter(|c| c.is(C::Import));
                Ok(imports
                    .flat_map(|i| i.names())
                    .fold(FxHashMap::default(), |mut m, n| {
                        let local = n.child_sym(C::Alias).unwrap_or(n.sym());
                        m.entry(local).or_insert_with(|| import_identity(n));
                        m
                    }))
            })
            .collect::<Result<_, _>>()?;
        let resolved_imports: FxHashSet<_> =
            self.reqs.iter().map(|req| (req.fi, req.node)).collect();
        let mut saved_imports: FxHashMap<_, Vec<u32>> = FxHashMap::default();
        for edge in edges.iter().filter(|edge| {
            edge.kind == EdgeKind::Imports && edge.call_resolution == CallResolution::Reference
        }) {
            let tree = scan.acquire(edge.to_fi())?;
            if let Some(site) = edge.site
                && let Some(import) = tree.cursor(edge.to_node).parent()
                && !resolved_imports.contains(&(edge.to_tree, import.index()))
            {
                saved_imports
                    .entry((edge.from_tree, site))
                    .or_default()
                    .push(edge.to_node);
            }
        }
        let sentinels = [run];
        let session = TreeSession::from_repository(trees, &sentinels);
        let mut ctx = ResolveCtx {
            references: Arc::default(),
            implementations: Arc::default(),
            saved_imports: Arc::new(saved_imports),
            trees,
            run,
            file_resolve_ms: env.limits.file_resolve_ms,
            env,
            extends_of: Arc::new(extends_of),
            imports_to: Arc::new(imports_to),
            call_at_site: Arc::new(call_at_site),
            corpus: &session,
            type_uses: Arc::new(type_uses),
            producers: Arc::new(producers),
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
        ctx.resolve_owners(edges, &self.reqs)?;
        let (outcomes, file_timings): (Vec<_>, Vec<(u32, Duration)>) = dirty_fis
            .par_iter()
            .map(|&fi| {
                let started = Instant::now();
                let file = Sentinel::new(
                    "resolve",
                    trees.label(fi as usize),
                    env.limits.file_resolve_ms,
                );
                (
                    ctx.with_session(&[run, &file], |ctx| resolve_file(ctx, fi as usize)),
                    (fi, started.elapsed()),
                )
            })
            .unzip();
        run.check()?;
        let mut per_file = Vec::new();
        let mut killed = Vec::new();
        for outcome in outcomes {
            match outcome {
                Ok(edges) => per_file.push(edges),
                Err(Error::Killed(error)) => killed.push(error),
                Err(error) => return Err(error),
            }
        }
        let mut requests = vec![Vec::new(); trees.len()];
        for req in &self.reqs {
            if dirty_fis.contains(&req.fi) || dirty_fis.contains(&req.target_fi) {
                requests[req.fi as usize].push(req);
            }
        }
        let wave1: Vec<Edge> = requests
            .par_iter()
            .map(|requests| {
                ctx.with_session(&[run], |ctx| {
                    let mut edges = Vec::new();
                    for req in requests {
                        edges.extend(resolve_one_import(ctx, req)?);
                    }
                    Ok(edges)
                })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .chain(per_file)
            .flatten()
            .map(|e| -> Result<_, Error> {
                if e.kind == EdgeKind::Calls
                    && let Some(site) = e.site
                    && scan.acquire(e.from_fi())?.cursor(site).has(C::Property)
                    && definitions[e.to_fi()]
                        .get(&e.to_node)
                        .is_some_and(|&(class, _)| class)
                {
                    return Ok(None);
                }
                Ok(Some(e))
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();
        for e in wave1.iter().filter(|e| e.kind == EdgeKind::Extends) {
            Arc::make_mut(&mut ctx.extends_of)
                .entry(e.from())
                .or_default()
                .push(e.to());
        }
        cross_edges.extend(&wave1);
        run.check()?;

        // Wildcard references come from the linker and from wave 1's unbound
        // names alike, so their uses resolve once both are known.
        let mut referrers: FxHashMap<(u32, u32), FxHashSet<u32>> = FxHashMap::default();
        for e in edges
            .iter()
            .chain(&wave1)
            .filter(|e| e.kind == EdgeKind::Imports && e.from_tree == e.to_tree)
        {
            let tree = scan.acquire(e.to_fi())?;
            let target = tree.cursor(e.to_node);
            if e.from_tree == e.to_tree
                && wildcard_or_name(ctx.wildcard_sym, target) == ctx.wildcard_sym
            {
                referrers.entry(e.to()).or_default().insert(e.from_node);
            }
        }
        let wildcard_edges: Vec<Edge> = requests
            .par_iter()
            .map(|requests| {
                ctx.with_session(&[run], |ctx| -> Result<_, Error> {
                    let mut edges = Vec::new();
                    for req in requests {
                        let import = ctx.corpus.acquire(req.fi, req.node)?;
                        for name in import
                            .names()
                            .filter(|n| wildcard_or_name(ctx.wildcard_sym, *n) == ctx.wildcard_sym)
                        {
                            if let Some(refs) = referrers.get(&(name.fi(), name.index())) {
                                edges.extend(wildcard_uses(ctx, req, name, refs)?);
                            }
                        }
                    }
                    Ok(edges)
                })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect();
        cross_edges.extend(&wildcard_edges);
        run.check()?;

        let mut uses_by_file = vec![Vec::new(); trees.len()];
        for (&(file, node), uses) in ctx.type_uses.iter() {
            uses_by_file[file as usize].push((node, uses));
        }
        let wave2: Vec<Edge> = uses_by_file
            .par_iter()
            .enumerate()
            .map(|(tree, entries)| {
                ctx.with_session(&[run], |ctx| -> Result<_, Error> {
                    let mut edges = Vec::new();
                    for &(node, uses) in entries {
                        let producer = ctx.corpus.acquire(tree as u32, node)?;
                        if producer.is(C::Binding) || external_of(ctx, producer)?.is_some() {
                            edges.extend(dispatch(
                                ctx,
                                producer,
                                producer_class(ctx, producer)?,
                                uses,
                            )?);
                        }
                    }
                    Ok(edges)
                })
            })
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
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
            let mut by_file = vec![Vec::new(); trees.len()];
            for edge in &wave {
                by_file[edge.from_fi()].push(edge);
            }
            wave = by_file
                .par_iter()
                .map(|entries| {
                    ctx.with_session(&[run], |ctx| {
                        let mut edges = Vec::new();
                        for edge in entries {
                            edges.extend(resolve_type_edges(ctx, edge)?);
                        }
                        Ok(edges)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
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
            if let Some(&(_, target_non_callable)) = definitions[edge.to_fi()].get(&edge.to_node) {
                non_callable
                    .entry(edge.from())
                    .and_modify(|known| *known &= target_non_callable)
                    .or_insert(target_non_callable);
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
                && scan
                    .acquire(edge.from_tree as usize)?
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
            cross_edges,
            killed,
            resolved_source_paths,
            file_timings,
        })
    }
}

type RelatedNodes = FxHashMap<(u32, u32), Vec<(u32, u32)>>;

#[derive(Clone)]
struct ResolveCtx<'a> {
    references: Arc<FxHashMap<(u32, u32), (u32, u32)>>,
    implementations: Arc<RelatedNodes>,
    saved_imports: Arc<FxHashMap<(u32, u32), Vec<u32>>>,
    trees: TreeRepository<'a>,
    run: &'a Sentinel,
    file_resolve_ms: u64,
    env: &'a Env,
    extends_of: Arc<RelatedNodes>,
    imports_to: Arc<FxHashMap<(u32, u32), Vec<&'a Edge>>>,
    call_at_site: Arc<FxHashMap<(u32, u32), &'a Edge>>,
    corpus: &'a TreeSession<'a>,
    type_uses: Arc<FxHashMap<(u32, u32), Vec<&'a Edge>>>,
    producers: Arc<FxHashMap<(u32, u32), Vec<&'a Edge>>>,
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
    fn with_session<T>(
        &self,
        sentinels: &[&Sentinel],
        operation: impl FnOnce(&ResolveCtx<'_>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let session = TreeSession::from_repository(self.trees, sentinels);
        let mut ctx = self.clone();
        ctx.corpus = &session;
        operation(&ctx)
    }
    fn resolve_owners(&mut self, edges: &[Edge], reqs: &[ImportReq]) -> Result<(), Error> {
        self.references = Arc::new(
            edges
                .iter()
                .filter(|edge| {
                    edge.kind == EdgeKind::Imports
                        && edge.call_resolution == CallResolution::Reference
                        && edge.site.is_none()
                })
                .map(|edge| (edge.from(), edge.to()))
                .collect(),
        );
        let referenced_imports: FxHashSet<_> = self.references.values().copied().collect();
        let mut imported_references = Vec::new();
        let mut requests = vec![Vec::new(); self.trees.len()];
        for req in reqs {
            requests[req.fi as usize].push(req);
        }
        for requests in requests {
            let found = self.with_session(&[self.run], |ctx| {
                let mut found = Vec::new();
                for req in requests {
                    for name in ctx.corpus.acquire(req.fi, req.node)?.names() {
                        if referenced_imports.contains(&(name.fi(), name.index()))
                            && let Some(target) = name_target(ctx, req, name)?
                        {
                            found.push(((name.fi(), name.index()), (target.fi, target.node)));
                        }
                    }
                }
                Ok(found)
            })?;
            imported_references.extend(found);
        }
        Arc::make_mut(&mut self.references).extend(imported_references);
        let mut sources = vec![Vec::new(); self.trees.len()];
        for &source in self.references.keys() {
            sources[source.0 as usize].push(source);
        }
        for sources in sources {
            let found = self.with_session(&[self.run], |ctx| {
                let mut found = Vec::new();
                for source in sources {
                    let reference = ctx.corpus.acquire(source.0, source.1)?;
                    if reference.is(C::DefName)
                        && let Some(implementation) =
                            reference.parent().filter(|node| node.has(C::ImplBlock))
                        && let Some(owner) = resolve_reference(ctx, reference)?
                    {
                        found.push((
                            (owner.fi(), owner.index()),
                            (implementation.fi(), implementation.index()),
                        ));
                    }
                }
                Ok(found)
            })?;
            for (owner, implementation) in found {
                Arc::make_mut(&mut self.implementations)
                    .entry(owner)
                    .or_default()
                    .push(implementation);
            }
        }
        for owners in Arc::make_mut(&mut self.implementations).values_mut() {
            owners.sort_unstable();
        }
        Ok(())
    }

    /// Import edges into `target`, its name children, or its parent import.
    fn imports_to(&self, fi: usize, target: u32) -> Result<Vec<&Edge>, Error> {
        let node = self.corpus.acquire(fi as u32, target)?;
        let related = std::iter::once(node)
            .chain(node.children())
            .chain(node.parent());
        Ok(related
            .flat_map(move |n| self.imports_to.get(&(fi as u32, n.index())))
            .flatten()
            .copied()
            .collect())
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
    trees: TreeRepository<'_>,
    lang: &Lang,
    file_index: &FileIndex,
    lookup: &LookupConfig,
    config: &ResolveConfig,
    dirty_fis: &FxHashSet<u32>,
) -> Result<(Vec<ImportReq>, Vec<Edge>), Error> {
    let tags = ReservedTags::new(lang);
    let resolved_tag_key = tags.resolved_source;
    let root_relative = lang.syms.lookup("source_root_rel");
    let mut declared_roots = FxHashSet::default();
    for fi in 0..trees.len() {
        let tree = trees.acquire(fi)?;
        if let Some(relative) = tree.root().tag(root_relative)
            && let Some(root) = trees.label(fi).strip_suffix(lang.syms.resolve(relative))
        {
            declared_roots.insert(root.trim_end_matches(PATH_SEP));
        }
    }
    let stdlib_prefixes: Vec<_> = lookup
        .prefixes
        .iter()
        .filter(|prefix| declared_roots.contains(prefix.as_str()))
        .cloned()
        .collect();
    let dirty_vec: Vec<u32> = dirty_fis.iter().copied().collect();
    let per_tree: Vec<(Vec<ImportReq>, Vec<Edge>)> = dirty_vec
        .par_iter()
        .map(|&fi| -> Result<_, Error> {
            let tree = trees.acquire(fi as usize)?;
            Ok(tree
                .root()
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
                }))
        })
        .collect::<Result<_, _>>()?;
    let mut all_reqs = Vec::new();
    let mut all_edges = Vec::new();
    for (reqs, edges) in per_tree {
        all_reqs.extend(reqs);
        all_edges.extend(edges);
    }
    Ok((all_reqs, all_edges))
}

fn propagate_reexports(
    trees: TreeRepository<'_>,
    reqs: &[ImportReq],
    visible: &mut VisibleMap,
    wildcard_sym: u32,
    tags: ReservedTags,
    merge_types: bool,
) -> Result<FxHashSet<(u32, u32)>, Error> {
    let mut ambiguous: FxHashSet<(u32, u32)> = FxHashSet::default();

    let mut visible_from_directives = Vec::new();
    let mut declared = vec![Vec::new(); trees.len()];
    for (fi, names) in declared.iter_mut().enumerate() {
        let tree = trees.acquire(fi)?;
        for c in tree.root().descendants() {
            if let Some(source_sym) = c.tag(tags.visible_from) {
                visible_from_directives.push((fi, source_sym));
            }
            if let Some(name) = c.tag(tags.exports) {
                names.push(name);
            }
        }
    }
    let mut scan = TreeScan::new(trees);
    let bindings: Vec<Vec<(u32, u32, bool)>> = reqs
        .iter()
        .map(|req| -> Result<_, Error> {
            let tree = scan.acquire(req.fi as usize)?;
            Ok(tree
                .cursor(req.node)
                .names()
                .map(|name| {
                    let symbol = wildcard_or_name(wildcard_sym, name);
                    let alias = name.child_sym(C::Alias).unwrap_or(symbol);
                    let namespace = namespace_alias(wildcard_sym, name).is_some()
                        && name.ancestors().any(|node| node.is(C::ModuleExport));
                    (symbol, alias, namespace)
                })
                .collect())
        })
        .collect::<Result<_, _>>()?;
    drop(scan);
    loop {
        let mut new_exports = Vec::new();
        let mut export = |fi: usize, name, loc| {
            if !visible[fi].contains_key(&name) {
                new_exports.push((fi, name, loc));
            }
        };
        for (req, bindings) in reqs.iter().zip(&bindings) {
            let exports = req.exports(trees, visible)?;
            for &(ns, alias, namespace) in bindings {
                if ns == wildcard_sym {
                    if namespace {
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
                    export(req.fi as usize, alias, loc);
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
                for (&ds, &dloc) in visible[source_fi].iter() {
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
                let key = |l: Loc| -> Result<_, Error> {
                    if !merge_types {
                        return Ok(None);
                    }
                    Ok(partial_key(
                        trees.acquire(l.fi as usize)?.cursor(l.node),
                        merge_types,
                    ))
                };
                if existing != loc && (key(existing)?.is_none() || key(existing)? != key(loc)?) {
                    ambiguous.insert((u32::try_from(fi).expect("file index exceeds u32"), ns));
                }
                continue;
            }
            Arc::make_mut(&mut visible[fi]).insert(ns, loc);
        }
    }
    Ok(ambiguous)
}

fn name_target(ctx: &ResolveCtx, req: &ImportReq, c: Cursor) -> Result<Option<Loc>, Error> {
    let tfi = req.target_fi as usize;
    let exports = req.exports(ctx.trees, ctx.visible)?;
    let ns = wildcard_or_name(ctx.wildcard_sym, c);
    if ns == ctx.wildcard_sym {
        return Ok(Some(Loc::new(tfi, req.anchor)));
    }
    if ctx.ambiguous.contains(&(req.target_fi, ns)) {
        return Ok(None);
    }
    if let Some(&loc) = exports.get(&ns) {
        return Ok(Some(loc));
    }
    if req.anchor != 0 {
        return Ok(None);
    }
    let target_path = ctx
        .lang
        .syms
        .resolve(ctx.corpus.acquire(tfi as u32, 0)?.sym());
    Ok(resolve_submodule(
        target_path,
        ctx.lang.syms.resolve(ns),
        ctx.support_lang,
        ctx.index_names,
        ctx.file_index,
    )
    .map(|fi| Loc::new(fi, 0)))
}

fn resolve_one_import(ctx: &ResolveCtx, req: &ImportReq) -> Result<Vec<Edge>, Error> {
    let (fi, tfi) = (req.fi as usize, req.target_fi as usize);
    let import = ctx.corpus.acquire(req.fi, req.node)?;

    let mut import_edges = Vec::new();
    for name in import.names() {
        if let Some(loc) = name_target(ctx, req, name)?.filter(|loc| loc.fi as usize != fi) {
            import_edges.push(Edge::new(
                name.fi(),
                name.index(),
                loc.fi,
                loc.node,
                EdgeKind::Imports,
            ));
        }
    }

    let mut edges: Vec<Edge> = import_edges.clone();

    for edge in ctx.imports_to(fi, req.node)? {
        let caller = ctx.corpus.acquire(fi as u32, edge.from_node)?;
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
        let exports = req.exports(ctx.trees, ctx.visible)?;
        let target_file = name_target(ctx, req, name)?
            .filter(|target| target.node == 0)
            .map_or(tfi, |target| target.fi as usize);
        let target = if target_file == tfi {
            exports.get(&m.sym())
        } else {
            ctx.visible[target_file].get(&m.sym())
        };
        let target = target
            .map(|loc| ctx.corpus.acquire(loc.fi, loc.node))
            .transpose()?
            .filter(|tgt| tgt.has_tag(ctx.tags.callable));
        if let Some(target) = target {
            if namespace_alias(ctx.wildcard_sym, name).is_some() {
                edges.push(name.edge_to(target, EdgeKind::Imports));
            }
            edges.push(call_edge(caller, target, edge.site));
        }
    }

    for ie in &import_edges {
        let target = ctx.corpus.acquire(ie.to_tree, ie.to_node)?;
        let name = ctx.corpus.acquire(ie.from_tree, ie.from_node)?;
        if wildcard_or_name(ctx.wildcard_sym, name) == ctx.wildcard_sym
            || !target.has_tag(ctx.tags.callable)
        {
            continue;
        }
        for intra in ctx.imports_to(ie.from_fi(), ie.from_node)? {
            let site = intra
                .site
                .map(|site| ctx.corpus.acquire(intra.from_tree, site))
                .transpose()?;
            if intra.call_resolution == CallResolution::Reference
                && site
                    .is_some_and(|binding| binding.is(C::Binding) && binding.bare_rhs().is_some())
            {
                continue;
            }
            let from = ctx.corpus.acquire(ie.from_tree, intra.from_node)?;
            if site.is_some_and(|site| {
                site.member().is_some_and(|member| {
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

    Ok(edges)
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
) -> Result<Vec<Edge>, Error> {
    if namespace_alias(ctx.wildcard_sym, name).is_some() {
        return Ok(Vec::new());
    }
    let fi = req.fi as usize;
    let exports = req.exports(ctx.trees, ctx.visible)?;
    let provided = |sym: u32| {
        exports
            .get(&sym)
            .filter(|_| !ctx.ambiguous.contains(&(req.target_fi, sym)))
            .filter(|loc| loc.fi as usize != fi)
            .map(|loc| ctx.corpus.acquire(loc.fi, loc.node))
            .transpose()
    };
    let mut imported: FxHashSet<(u32, u32)> = FxHashSet::default();
    let mut edges = Vec::new();
    let mut import = |target: Cursor, edges: &mut Vec<Edge>| {
        if imported.insert((target.fi(), target.index())) {
            edges.push(name.edge_to(target, EdgeKind::Imports));
        }
    };
    let root = |r: Cursor<'_>| qualified(ctx, r.sym(), &provided);
    for &node in referrers {
        let from = ctx.corpus.acquire(fi as u32, node)?;
        for call in from.calls() {
            let Some(callee) = call.child(C::Callee) else {
                continue;
            };
            let Some(target) = chain(ctx, callee, &root)? else {
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
        for header in headers {
            if let Some(target) = chain(ctx, header, &root)? {
                import(target, &mut edges);
            }
        }
    }
    Ok(edges)
}

/// A name that is a path in the language's own spelling (`Security::Ctx`):
/// the first segment resolves as a root, each further one as a member.
fn qualified<'a>(
    ctx: &'a ResolveCtx,
    sym: u32,
    root: &dyn Fn(u32) -> Result<Option<Cursor<'a>>, Error>,
) -> Result<Option<Cursor<'a>>, Error> {
    if let Some(found) = root(sym)? {
        return Ok(Some(found));
    }
    let separator = ctx.support_lang.fqn_separator();
    let mut segments = ctx.lang.syms.resolve(sym).split(separator);
    let Some(first) = segments.next() else {
        return Ok(None);
    };
    let Some(mut current) = root(ctx.lang.syms.lookup(first))? else {
        return Ok(None);
    };
    for segment in segments {
        let name = ctx.lang.syms.lookup(segment);
        let Some(target) = method_up(ctx, current, name, current.fi() as usize)?
            .into_iter()
            .exactly_one()
            .ok()
        else {
            return Ok(None);
        };
        current = target;
    }
    Ok(Some(current))
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

fn resolve_type_edges(ctx: &ResolveCtx, ce: &Edge) -> Result<Vec<Edge>, Error> {
    let uses = |site| Some((site, ctx.type_uses.get(&(ce.from_tree, site))?));
    let Some((site, uses)) = ce.site.and_then(uses) else {
        return Ok(vec![]);
    };
    let producer = ctx.corpus.acquire(ce.from_tree, site)?;
    dispatch(
        ctx,
        producer,
        class_of(ctx, ctx.corpus.acquire(ce.to_tree, ce.to_node)?)?,
        uses,
    )
}

fn dispatch(
    ctx: &ResolveCtx,
    producer: Cursor,
    class: Option<Cursor>,
    uses: &[&Edge],
) -> Result<Vec<Edge>, Error> {
    let mut output = Vec::new();
    for usage in uses {
        let Some(site) = usage.site else {
            continue;
        };
        let call = ctx.corpus.acquire(usage.from_tree, site)?;
        let reaching = ctx.producers.get(&(usage.from_tree, site));
        let class = match reaching {
            Some(reaching) if reaching.len() > 1 => {
                let classes = reaching
                    .iter()
                    .map(|edge| {
                        producer_class(ctx, ctx.corpus.acquire(edge.to_tree, edge.to_node)?)
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                lub(ctx, classes.into_iter())?
            }
            _ => class,
        };
        let from = ctx.corpus.acquire(usage.from_tree, usage.from_node)?;
        let Some(class) = class else {
            if call
                .child(C::Callee)
                .is_some_and(|callee| callee.sym_opt().is_some())
                && let Some(imports) = ctx.saved_imports.get(&(producer.fi(), producer.index()))
            {
                output.extend(imports.iter().map(|&node| Edge {
                    site: usage.site,
                    ..Edge::new(
                        from.fi(),
                        from.index(),
                        producer.fi(),
                        node,
                        EdgeKind::Imports,
                    )
                }));
                continue;
            }
            let Some(owner) = external_of(ctx, producer)? else {
                continue;
            };
            let Some(member) = call.member() else {
                continue;
            };
            let targets =
                extension_members(ctx, Owner::External(owner), member.sym(), usage.from_fi())?;
            output.extend(call_edges(from, targets, usage.site));
            continue;
        };
        let class = match call.member().and_then(|m| m.child(C::Object)) {
            Some(object) if object.child(C::Member).is_some() => {
                let Some(target) = chain(ctx, object, &|_| Ok(Some(class)))? else {
                    continue;
                };
                target
            }
            _ => class,
        };
        if call.is(C::Binding) && class.initializer().and_then(Cursor::rhs_callee).is_none() {
            continue;
        }
        let targets = match call.member() {
            Some(member) => member_targets(ctx, member, class)?,
            None => match class.child_sym(C::Callable) {
                Some(name) => method_up(ctx, class, name, usage.from_fi())?,
                None => vec![class],
            },
        };
        output.extend(call_edges(from, targets, usage.site));
    }
    Ok(output)
}

fn import_identity(name: Cursor) -> (u32, u32) {
    let source = name.parent().and_then(|i| i.child_sym(C::SourcePath));
    (source.unwrap_or(0), name.sym())
}

fn external_of(ctx: &ResolveCtx, producer: Cursor) -> Result<Option<(u32, u32)>, Error> {
    let named = match producer.is(C::Binding) {
        true => producer.typed(),
        false => producer.child(C::Callee),
    };
    let Some(named) = named.filter(|t| !t.has(C::Member)) else {
        return Ok(None);
    };
    if visible_type(ctx, named.fi(), named.sym())?.is_some() {
        return Ok(None);
    }
    Ok(ctx.imports[named.fi() as usize].get(&named.sym()).copied())
}

enum Owner<'a> {
    Class(Cursor<'a>),
    External((u32, u32)),
}

fn producer_class<'a>(
    ctx: &'a ResolveCtx,
    producer: Cursor<'a>,
) -> Result<Option<Cursor<'a>>, Error> {
    if producer.typed().is_none()
        && let Some(rhs) = producer.bare_rhs()
    {
        return resolve_chain(ctx, rhs);
    }
    let callee = if producer.is(C::Binding) {
        let Some(typed) = producer.typed() else {
            return Ok(None);
        };
        resolve_chain(ctx, typed)?
    } else {
        callee_of(ctx, producer)?
    };
    match callee {
        Some(callee) => class_of(ctx, callee),
        None => Ok(None),
    }
}

fn callee_of<'a>(ctx: &'a ResolveCtx, call: Cursor<'a>) -> Result<Option<Cursor<'a>>, Error> {
    if let Some(edge) = ctx.call_at_site.get(&(call.fi(), call.index())) {
        return Ok(Some(ctx.corpus.acquire(edge.to_tree, edge.to_node)?));
    }
    match call.child(C::Callee) {
        Some(callee) => resolve_chain(ctx, callee),
        None => Ok(None),
    }
}

fn class_of<'a>(ctx: &'a ResolveCtx, callee: Cursor<'a>) -> Result<Option<Cursor<'a>>, Error> {
    let Some(target) = value_type(ctx, callee)? else {
        return Ok(None);
    };
    if target.is_class() {
        Ok(Some(target))
    } else if let Some(ty) = target.child(C::SsaReturnType) {
        resolve_chain(ctx, ty)
    } else if let Some(branch) = target
        .descendants_pruned(|n| n.is(C::Def))
        .find(|n| n.is(C::SsaReturn))
        .and_then(|r| r.child(C::SsaBranch))
    {
        branch_type(ctx, branch)
    } else {
        match infer_return_type(target) {
            Some(sym) => visible_type(ctx, target.fi(), sym),
            None => Ok(None),
        }
    }
}

fn visible_type<'a>(ctx: &'a ResolveCtx, fi: u32, sym: u32) -> Result<Option<Cursor<'a>>, Error> {
    let Some(loc) = ctx.visible[fi as usize]
        .get(&sym)
        .filter(|_| !ctx.ambiguous.contains(&(fi, sym)))
    else {
        return Ok(None);
    };
    Ok(Some(ctx.corpus.acquire(loc.fi, loc.node)?))
}

fn resolve_chain<'a>(ctx: &'a ResolveCtx, c: Cursor<'a>) -> Result<Option<Cursor<'a>>, Error> {
    chain(ctx, c, &|r| {
        if let Some(target) = resolve_reference(ctx, r)? {
            return Ok(Some(target));
        }
        if let Some(target) = enclosing_alias(ctx, r)? {
            return Ok(Some(target));
        }
        visible_type(ctx, r.fi(), r.sym())
    })
}

fn resolve_reference<'a>(
    ctx: &'a ResolveCtx,
    reference: Cursor<'a>,
) -> Result<Option<Cursor<'a>>, Error> {
    for (fi, node) in reachable((reference.fi(), reference.index()), |id| {
        ctx.references.get(&id).copied()
    })
    .skip(1)
    {
        let node = ctx.corpus.acquire(fi, node)?;
        if node.is(C::Def) {
            return Ok(Some(node));
        }
    }
    Ok(None)
}

/// `Self` in `impl Service { fn new() -> Self }` names the enclosing def;
/// `T` in `fn f<T: Pinger>(t: T)` is the type parameter bound in it.
fn enclosing_alias<'a>(ctx: &'a ResolveCtx, r: Cursor<'a>) -> Result<Option<Cursor<'a>>, Error> {
    let sym = r.sym();
    for d in r.ancestors().filter(|a| a.is(C::Def)) {
        if d.children_of(C::Alias).any(|a| a.sym() == sym) {
            return Ok(Some(d));
        }
        let Some(bound) = d
            .children()
            .filter(|c| c.is(C::Binding) && c.sym() == sym && c.children().count() == 1)
            .find_map(|c| c.child(C::SsaTyped))
            .filter(|bound| bound.sym() != sym)
        else {
            continue;
        };
        if let Some(target) = resolve_chain(ctx, bound)? {
            return Ok(Some(target));
        }
    }
    Ok(None)
}

fn chain<'a>(
    ctx: &'a ResolveCtx,
    c: Cursor<'a>,
    root: &dyn Fn(Cursor<'a>) -> Result<Option<Cursor<'a>>, Error>,
) -> Result<Option<Cursor<'a>>, Error> {
    let c = c.reference();
    let Some(m) = c.has(C::Object).then_some(c).or_else(|| c.child(C::Member)) else {
        return match root(c)? {
            Some(target) => value_type(ctx, target),
            None => Ok(None),
        };
    };
    let Some(object) = m.child(C::Object) else {
        return Ok(None);
    };
    let Some(receiver) = chain(ctx, object, root)? else {
        return Ok(None);
    };
    let targets = member_targets(ctx, m, receiver)?;
    match targets.into_iter().exactly_one().ok() {
        Some(member) => value_type(ctx, member),
        None => Ok(None),
    }
}

fn value_type<'a>(ctx: &'a ResolveCtx, d: Cursor<'a>) -> Result<Option<Cursor<'a>>, Error> {
    if d.has(C::EnumVariant) {
        Ok(d.enclosing_def(&[C::Enum]))
    } else if d.has(C::TypeAlias) {
        match d.child(C::SsaTyped) {
            Some(aliased) => resolve_chain(ctx, aliased),
            None => Ok(Some(d)),
        }
    } else if d.has(C::FieldDef) || d.has(C::Property) {
        let declared = d.child(C::Binding).and_then(Cursor::typed);
        match declared.or_else(|| d.child(C::SsaReturnType)) {
            Some(ty) => resolve_chain(ctx, ty),
            None => Ok(Some(d)),
        }
    } else {
        Ok(Some(d))
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

fn gather_members(trees: TreeRepository<'_>, merge_types: bool) -> Result<Members, Error> {
    let (mut parts, mut extensions) = Members::default();
    for fi in 0..trees.len() {
        let tree = trees.acquire(fi)?;
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
    Ok((parts, extensions))
}

fn supertypes<'a>(ctx: &'a ResolveCtx, cls: Cursor<'a>) -> Result<Vec<(u32, u32)>, Error> {
    let mut targets = Vec::new();
    for part in std::iter::once(cls).chain(impl_blocks_of(ctx, cls)?) {
        for parent in part.children().filter(|node| node.is(C::SuperType)) {
            if let Some(target) = resolve_chain(ctx, parent)? {
                targets.push((target.fi(), target.index()));
            }
        }
        targets.extend(
            ctx.extends_of
                .get(&(part.fi(), part.index()))
                .into_iter()
                .flatten()
                .copied(),
        );
    }
    Ok(targets)
}

fn impl_blocks_of<'a>(ctx: &'a ResolveCtx, cls: Cursor<'a>) -> Result<Vec<Cursor<'a>>, Error> {
    let owner = match cls.child(C::DefName).filter(|_| cls.has(C::ImplBlock)) {
        Some(name) => resolve_reference(ctx, name)?.unwrap_or(cls),
        None => cls,
    };
    let mut owners = vec![owner];
    for &(fi, node) in ctx
        .implementations
        .get(&(owner.fi(), owner.index()))
        .into_iter()
        .flatten()
    {
        owners.push(ctx.corpus.acquire(fi, node)?);
    }
    owners.retain(|node| (node.fi(), node.index()) != (cls.fi(), cls.index()));
    Ok(owners)
}

fn lub<'a>(
    ctx: &'a ResolveCtx,
    classes: impl Iterator<Item = Option<Cursor<'a>>>,
) -> Result<Option<Cursor<'a>>, Error> {
    let ancestors = |id: (u32, u32)| -> Result<FxHashSet<_>, Error> {
        let mut seen = FxHashSet::from_iter([id]);
        let mut pending = vec![id];
        while let Some((fi, node)) = pending.pop() {
            for parent in supertypes(ctx, ctx.corpus.acquire(fi, node)?)? {
                if seen.insert(parent) {
                    pending.push(parent);
                }
            }
        }
        Ok(seen)
    };
    let mut common = None;
    for class in classes {
        let Some(class) = class.filter(|node| node.is_class()) else {
            return Ok(None);
        };
        let parents = ancestors((class.fi(), class.index()))?;
        match &mut common {
            None => common = Some(parents),
            Some(common) => common.retain(|id| parents.contains(id)),
        }
    }
    let Some(common) = common else {
        return Ok(None);
    };
    let mut least = common.clone();
    for &id in &common {
        for parent in ancestors(id)? {
            if parent != id {
                least.remove(&parent);
            }
        }
    }
    match least.into_iter().exactly_one().ok() {
        Some((fi, node)) => Ok(Some(ctx.corpus.acquire(fi, node)?)),
        None => Ok(None),
    }
}

fn branch_type<'a>(ctx: &'a ResolveCtx, branch: Cursor<'a>) -> Result<Option<Cursor<'a>>, Error> {
    let mut arms = Vec::new();
    for a in branch.children().filter(|a| a.is(C::SsaArm)) {
        let tail = a.tail_expr();
        if tail.is(C::SsaReturn)
            || tail.is(C::SsaBranch) && !tail.children().any(|c| c.is(C::SsaArm))
        {
            continue;
        }
        let value = if tail.is(C::SsaBranch) {
            branch_type(ctx, tail)?
        } else {
            let Some(callee) = tail.child(C::Callee) else {
                continue;
            };
            resolve_chain(ctx, callee)?
        };
        arms.push(value);
    }
    lub(ctx, arms.into_iter())
}

fn method_up<'a>(
    ctx: &'a ResolveCtx,
    cls: Cursor<'a>,
    name: u32,
    fi: usize,
) -> Result<Vec<Cursor<'a>>, Error> {
    let mut level = vec![(cls.fi(), cls.index())];
    let mut seen = FxHashSet::from_iter(level.iter().copied());
    while !level.is_empty() {
        let mut found = Vec::new();
        for &(file, node) in &level {
            if let Some(member) = declared_member(ctx, ctx.corpus.acquire(file, node)?, name)? {
                let id = (member.fi(), member.index());
                if !found.contains(&id) {
                    found.push(id);
                }
            }
        }
        if !found.is_empty() {
            return found
                .into_iter()
                .map(|(fi, node)| ctx.corpus.acquire(fi, node))
                .collect();
        }
        let mut next = Vec::new();
        for (file, node) in level {
            for parent in supertypes(ctx, ctx.corpus.acquire(file, node)?)? {
                if seen.insert(parent) {
                    next.push(parent);
                }
            }
        }
        level = next;
    }
    extension_members(ctx, Owner::Class(cls), name, fi)
}

fn member_targets<'a>(
    ctx: &'a ResolveCtx,
    member: Cursor<'a>,
    receiver: Cursor<'a>,
) -> Result<Vec<Cursor<'a>>, Error> {
    let Some(constraint) = member.child(C::Dispatch) else {
        return method_up(ctx, receiver, member.sym(), member.fi() as usize);
    };
    let contract = match resolve_reference(ctx, constraint)? {
        Some(contract) => Some(contract),
        None => qualified(ctx, constraint.sym(), &|name| {
            visible_type(ctx, constraint.fi(), name)
        })?,
    };
    let Some(contract) = contract.filter(|owner| owner.is_dispatch_contract()) else {
        return Ok(Vec::new());
    };
    let mut implementation = None;
    for owner in std::iter::once(receiver).chain(impl_blocks_of(ctx, receiver)?) {
        for constraint in owner.children_of(C::SuperType) {
            if resolve_chain(ctx, constraint)?.is_some_and(|target| {
                (target.fi(), target.index()) == (contract.fi(), contract.index())
            }) {
                if implementation.is_some() {
                    return Ok(Vec::new());
                }
                implementation = Some(owner);
                break;
            }
        }
    }
    let Some(implementation) = implementation else {
        return Ok(Vec::new());
    };
    Ok(find_method_in(implementation, member.sym())
        .or_else(|| {
            find_method_in(contract, member.sym()).filter(|method| !method.has(C::Declaration))
        })
        .into_iter()
        .collect())
}

fn extension_members<'a>(
    ctx: &'a ResolveCtx,
    owner: Owner<'a>,
    name: u32,
    fi: usize,
) -> Result<Vec<Cursor<'a>>, Error> {
    let mut targets = Vec::new();
    for loc in ctx.extensions.get(&name).into_iter().flatten() {
        if loc.fi as usize != fi && !ctx.exporters[fi].contains(&loc.fi) {
            continue;
        }
        let method = ctx.corpus.acquire(loc.fi, loc.node)?;
        let Some(receiver) = method
            .enclosing_def(&[C::ImplBlock])
            .and_then(|owner| owner.child_sym(C::DefName))
        else {
            continue;
        };
        let same = match owner {
            Owner::Class(cls) => {
                let Some(target) = visible_type(ctx, method.fi(), receiver)? else {
                    continue;
                };
                (target.fi(), target.index()) == (cls.fi(), cls.index())
            }
            Owner::External(id) => {
                visible_type(ctx, method.fi(), receiver)?.is_none()
                    && ctx.imports[method.fi() as usize].get(&receiver) == Some(&id)
            }
        };
        if same {
            targets.push(method);
        }
    }
    Ok(targets)
}

fn declared_member<'a>(
    ctx: &'a ResolveCtx,
    cls: Cursor<'a>,
    name: u32,
) -> Result<Option<Cursor<'a>>, Error> {
    let parts = partial_key(cls, ctx.merge_types).and_then(|k| ctx.partials.get(&k));
    let mut owners = vec![cls];
    owners.extend(impl_blocks_of(ctx, cls)?);
    for loc in parts
        .into_iter()
        .flatten()
        .filter(|loc| (loc.fi, loc.node) != (cls.fi(), cls.index()))
    {
        owners.push(ctx.corpus.acquire(loc.fi, loc.node)?);
    }
    for owner in owners {
        let found = {
            if owner.is(C::Def) {
                ctx.declared_members[owner.fi() as usize]
                    .get(&(owner.index(), name))
                    .map(|&node| owner.jump(owner.fi(), node))
            } else {
                find_method_in(owner, name)
            }
        };
        if found.is_some() {
            return Ok(found);
        }
    }
    Ok(None)
}

/// Cross-file edges for one file that the import pass cannot produce:
/// inheritance, decorators, destructuring, and member calls on imported types.
fn resolve_file(ctx: &ResolveCtx, fi: usize) -> Result<Vec<Edge>, Error> {
    let file = Sentinel::new("resolve", ctx.trees.label(fi), ctx.file_resolve_ms);
    let mut out = Vec::new();
    let root = ctx.corpus.acquire(fi as u32, 0)?;
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
    let builtins = &ctx.env.rules_for(ctx.trees.label(fi)).config.link.builtins;
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
                match resolve_chain(ctx, dec)? {
                    Some(t) if cross(t) => out.push(node.edge_to(t, EdgeKind::Calls)),
                    Some(_) => {}
                    None => out.extend(unbound(node, dec.sym())),
                }
            }
            let parents: Vec<Cursor> = node
                .children_of(C::SuperType)
                .map(|s| resolve_chain(ctx, s))
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .flatten()
                .filter(|p| cross(*p))
                .collect();
            out.extend(parents.iter().map(|p| node.edge_to(*p, EdgeKind::Extends)));
            if !parents.is_empty() {
                out.extend(inherited_calls(ctx, node, &parents, fi)?);
            }
        } else if node.is(C::Destructure) {
            out.extend(destructure_calls(ctx, node, from, fi)?);
        } else if let Some(m) = node.member().filter(|m| m.sym_opt().is_some()) {
            let Some(object) = m.child(C::Object) else {
                continue;
            };
            let Some(obj) = object.reference().chain_root().sym_opt() else {
                continue;
            };
            match resolve_chain(ctx, object)? {
                Some(target)
                    if m.child(C::Dispatch)
                        .is_some_and(|dispatch| !dispatch.has(C::Object)) =>
                {
                    out.extend(call_edges(
                        from,
                        member_targets(ctx, m, target)?,
                        Some(node.index()),
                    ));
                }
                Some(target) if m.has(C::Dispatch) && target.is_dispatch_contract() => {}
                Some(target) if cross(target) && target.is_class() => {
                    let members = method_up(ctx, target, m.sym(), fi)?;
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
) -> Result<Vec<Edge>, Error> {
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
            out.extend(call_edges(from, method_up(ctx, *parent, name, fi)?, site));
        }
    }
    Ok(out)
}

/// Each slot of a destructuring pattern reads the matching positional
/// component of the destructured type.
fn destructure_calls<'a>(
    ctx: &'a ResolveCtx,
    d: Cursor<'a>,
    from: Cursor<'a>,
    fi: usize,
) -> Result<Vec<Edge>, Error> {
    let typed_local = |k: Cursor| -> Result<_, Error> {
        let Some(s) = k.sym_opt() else {
            return Ok(None);
        };
        let Some(binding) = from
            .descendants()
            .find(|b| b.is(C::Binding) && b.sym_opt() == Some(s))
        else {
            return Ok(None);
        };
        match binding.typed() {
            Some(ty) => resolve_chain(ctx, ty),
            None => Ok(None),
        }
    };
    let class = match (d.child(C::Call), d.child(C::Rhs)) {
        (Some(pattern), _) => match pattern.child(C::Callee) {
            Some(k) => resolve_chain(ctx, k)?,
            None => None,
        },
        (None, Some(value)) => match value.child(C::Call) {
            Some(call) => producer_class(ctx, call)?,
            None => typed_local(value)?,
        },
        (None, None) => None,
    };
    let Some(class) = class else {
        return Ok(vec![]);
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
        return Ok(vec![]);
    }
    let mut edges = Vec::new();
    for (slot, component) in slots.iter().zip(components) {
        edges.extend(call_edges(
            from,
            method_up(ctx, class, component.sym(), fi)?,
            Some(slot.index()),
        ));
    }
    Ok(edges)
}

fn build_file_index(
    trees: TreeRepository<'_>,
    lang: &Lang,
    support_lang: SupportLang,
    index_names: &[String],
) -> Result<FileIndex, Error> {
    let mut idx = FileIndex::default();
    let sep = support_lang.fqn_separator();
    for fi in 0..trees.len() {
        let tree = trees.acquire(fi)?;
        let path = trees.label(fi);
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
    Ok(idx)
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
            Arc::new(FxHashMap::from_iter([(name, Loc::new(1, 0))])),
            Arc::default(),
            Arc::new(FxHashMap::from_iter([(name, Loc::new(0, 0))])),
        ];
        let old_labels: Vec<String> = ["a", "b", "c"].map(String::from).to_vec();
        let label_to_fi = FxHashMap::from_iter([("a", 0u32), ("c", 1u32)]);

        let lost = resolver.remap(&old_labels, &label_to_fi);

        assert_eq!(lost, FxHashSet::from_iter([0]));
        assert_eq!(resolver.visible[1].get(&name), Some(&Loc::new(0, 0)));
    }
}
