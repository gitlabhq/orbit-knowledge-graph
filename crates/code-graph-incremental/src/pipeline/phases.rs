//! The phases, in the order they run.

use std::borrow::Cow;
use std::time::{Duration, Instant};

use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

use arrow::record_batch::RecordBatch;
use ontology::Ontology;

use super::{
    Admission, Canonical, Context, DirtyGraph, Displayed, Error, Exported, FileStats, ItemPhase,
    Labelled, LinkedFile, Parsed, Phase, ReindexInput, Resolved, Rewritten, SourceFile,
    SourcePaths, State, Workset,
};
use crate::env::Env;
use crate::export::{self, Envelope};
use crate::linker;
use crate::pattern::{self, EdgeCtx, EdgeIndex};
use crate::sentinel::{Killed, Sentinel};
use crate::tree::{Edge, Storage, Tag, Tree};
use crate::treesitter::{self, SupportLang};

pub struct Prepare;

impl Phase<()> for Prepare {
    type Output = Workset<SourcePaths>;

    fn name(&self) -> Cow<'static, str> {
        "prepare".into()
    }

    fn run(self, context: &mut Context, _: ()) -> Result<Self::Output, Error> {
        workset(context, State::new(context.env), FxHashSet::default())
    }
}

fn read(repository: &orbit_utils::vfs::Vfs<()>, path: &str) -> std::io::Result<String> {
    String::from_utf8(repository.read(path)?.to_vec())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

fn workset(
    context: &mut Context,
    mut state: State,
    dirty: FxHashSet<u32>,
) -> Result<Workset<SourcePaths>, Error> {
    let repository = &context.repository;
    let paths: Vec<_> = super::candidates(repository, context.env.limits.max_files)
        .filter(|file| {
            SupportLang::from_path(&file.path).is_some_and(|lang| context.env.in_family(lang))
        })
        .map(|file| file.path.to_string())
        .collect();
    let cap = context.env.limits.max_manifest_bytes;
    let mut manifest_bytes = 0u64;
    for file in repository.files().filter(|file| file.keeps()) {
        context.run.check()?;
        let path = file.path.as_ref();
        if context
            .env
            .resolve
            .config
            .parse_files
            .iter()
            .any(|entry| path.rsplit('/').next() == Some(entry.name.as_str()))
        {
            manifest_bytes = manifest_bytes.saturating_add(file.size);
            if cap > 0 && manifest_bytes > cap {
                let (observed, cap) = (manifest_bytes as usize, cap as usize);
                let limited = Killed::limit("manifests", path, observed, cap);
                context.report.limited.push(limited);
                continue;
            }
            match read(repository, path) {
                Ok(content) => state.configs.push(SourceFile {
                    path: path.into(),
                    content,
                }),
                Err(_) => context.report.unread_files.push(path.into()),
            }
        }
    }
    Ok(Workset {
        state,
        items: SourcePaths { paths },
        dirty,
    })
}

/// Incremental index: removed and modified files leave the graph, edges and
/// resolver locations follow the compacted file indices, and every retained
/// file that pointed at a removed one is marked dirty.
pub struct Remap;

impl Phase<ReindexInput> for Remap {
    type Output = Workset<SourcePaths>;

    fn name(&self) -> Cow<'static, str> {
        "remap".into()
    }

    fn run(self, context: &mut Context, input: ReindexInput) -> Result<Self::Output, Error> {
        let ReindexInput { mut state, removed } = input;
        let old_labels: Vec<String> = state.trees.iter().map(|t| t.label.clone()).collect();
        let dirty_labels: FxHashSet<&str> = removed
            .iter()
            .map(String::as_str)
            .chain(context.repository.files().map(|f| f.path.as_ref()))
            .collect();
        let dirty = remap(&mut state, &old_labels, &dirty_labels);
        state
            .configs
            .retain(|c| !dirty_labels.contains(c.path.as_str()));
        workset(context, state, dirty)
    }
}

/// `Parse` entries this crate has a grammar for become the lazy workset,
/// Drops the dirty trees, renumbers what remains, and returns the retained
/// files whose resolution depended on a dropped one.
fn remap(state: &mut State, old_labels: &[String], dirty: &FxHashSet<&str>) -> FxHashSet<u32> {
    state.trees.retain(|t| !dirty.contains(t.label.as_str()));

    let label_to_fi: FxHashMap<&str, u32> = state
        .trees
        .iter()
        .enumerate()
        .map(|(i, t)| {
            (
                t.label.as_str(),
                u32::try_from(i).expect("file index exceeds u32"),
            )
        })
        .collect();

    state.edges = state
        .edges
        .iter()
        .filter(|e| {
            old_labels
                .get(e.from_fi())
                .is_some_and(|l| !dirty.contains(l.as_str()))
                && old_labels
                    .get(e.to_fi())
                    .is_some_and(|l| !dirty.contains(l.as_str()))
        })
        .map(|e| Edge {
            from_tree: label_to_fi[old_labels[e.from_fi()].as_str()],
            to_tree: label_to_fi[old_labels[e.to_fi()].as_str()],
            ..*e
        })
        .collect();

    let old_dirty_fis: FxHashSet<u32> = old_labels
        .iter()
        .enumerate()
        .filter(|(_, l)| dirty.contains(l.as_str()))
        .map(|(i, _)| u32::try_from(i).expect("file index exceeds u32"))
        .collect();
    let reverse_dirty: FxHashSet<u32> = state
        .resolver
        .reqs()
        .iter()
        .filter(|r| old_dirty_fis.contains(&r.target_fi))
        .filter_map(|r| label_to_fi.get(old_labels[r.fi as usize].as_str()))
        .copied()
        .collect();

    let mut dependents = state.resolver.remap(old_labels, &label_to_fi);
    dependents.extend(reverse_dirty);
    dependents
}

/// Runs an `ItemPhase` over every item of a workset in parallel. An item that
/// overruns its budget is skipped and reported; the rest continue.
pub struct Each<P>(pub P);

impl<C, P> Phase<Workset<C>> for Each<P>
where
    C: IntoParallel,
    C::Item: Labelled,
    P: ItemPhase<C::Item> + Sync,
    P::Output: Send,
{
    type Output = Workset<Vec<P::Output>>;

    fn name(&self) -> Cow<'static, str> {
        self.0.name()
    }

    fn run(self, context: &mut Context, input: Workset<C>) -> Result<Self::Output, Error> {
        let Workset {
            state,
            items,
            dirty,
        } = input;
        let (env, run, phase) = (context.env, &context.run, &self.0);
        let admission = &*context.admission;
        let outcomes: Vec<_> = items
            .into_parallel(&context.repository)
            .map(|item| {
                let item = item?;
                let path = item.label().to_string();
                let bytes = item.bytes();
                let started = Instant::now();
                let outcome = match Slot::admit(admission, bytes, run) {
                    Ok(_slot) => phase.run(env, run, item),
                    Err(killed) => Err(killed),
                };
                Ok((outcome, path, bytes, started.elapsed()))
            })
            .collect();
        context.run.check()?;
        let mut items = Vec::with_capacity(outcomes.len());
        for outcome in outcomes {
            let (outcome, path, bytes, elapsed) = match outcome {
                Ok(result) => result,
                Err(path) => {
                    context.report.unread_files.push(path);
                    continue;
                }
            };
            match outcome {
                Ok(v) => {
                    let stats = context.report.files.entry(path).or_insert(FileStats {
                        language: env.lang_id,
                        bytes: 0,
                        parse: Duration::ZERO,
                        resolve: Duration::ZERO,
                    });
                    stats.bytes += bytes;
                    stats.parse += elapsed;
                    items.push(v);
                }
                Err(k) => {
                    // Guard: a killed file must not keep an entry from an earlier phase.
                    context.report.files.remove(&path);
                    context.skip(k);
                }
            }
        }
        context.report.unread_files.sort_unstable();
        context.report.unread_files.dedup();
        Ok(Workset {
            state,
            items,
            dirty,
        })
    }
}

struct Slot<'a> {
    admission: &'a dyn Admission,
    bytes: u64,
}

impl<'a> Slot<'a> {
    fn admit(admission: &'a dyn Admission, bytes: u64, run: &Sentinel) -> Result<Self, Killed> {
        admission.admit(bytes, run)?;
        Ok(Self { admission, bytes })
    }
}

impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.admission.release(self.bytes);
    }
}

/// What `Each` fans out over: items already in memory, or a lazy source
/// pulled one item at a time as workers free up.
pub trait IntoParallel {
    type Item: Send;
    fn into_parallel(
        self,
        repository: &orbit_utils::vfs::Vfs<()>,
    ) -> impl ParallelIterator<Item = Result<Self::Item, String>>;
}

impl<T: Send> IntoParallel for Vec<T> {
    type Item = T;
    fn into_parallel(
        self,
        _: &orbit_utils::vfs::Vfs<()>,
    ) -> impl ParallelIterator<Item = Result<T, String>> {
        self.into_par_iter().with_max_len(1).map(Ok)
    }
}

impl IntoParallel for SourcePaths {
    type Item = SourceFile;
    fn into_parallel(
        self,
        repository: &orbit_utils::vfs::Vfs<()>,
    ) -> impl ParallelIterator<Item = Result<SourceFile, String>> {
        self.paths
            .into_par_iter()
            .with_max_len(1)
            .map(move |path| match read(repository, &path) {
                Ok(content) => Ok(SourceFile { path, content }),
                Err(_) => Err(path),
            })
    }
}

/// tree-sitter, with the grammar of the file's own language.
pub struct Parse;

impl ItemPhase<SourceFile> for Parse {
    type Output = Parsed;

    fn name(&self) -> Cow<'static, str> {
        "parse".into()
    }

    fn run(&self, env: &Env, run: &Sentinel, file: SourceFile) -> Result<Parsed, Killed> {
        let grammar = SupportLang::from_path(&file.path).unwrap_or(env.lang_id);
        let budget = run
            .child("tree-sitter", &file.path, env.limits.file_parse_ms)
            .on_thread_cpu();
        treesitter::parse_with_sentinel(
            &file.content,
            grammar,
            &env.lang,
            &file.path,
            &budget,
            env.limits.max_tree_depth,
        )
        .map(|(tree, cut)| Parsed(tree, cut))
    }
}

/// The language's rewrite stages, under the per-file rewrite budget.
pub struct Rewrite;

impl ItemPhase<Parsed> for Rewrite {
    type Output = Rewritten;

    fn name(&self) -> Cow<'static, str> {
        "rewrite".into()
    }

    fn run(
        &self,
        env: &Env,
        run: &Sentinel,
        Parsed(mut tree, cut): Parsed,
    ) -> Result<Rewritten, Killed> {
        let budget =
            Sentinel::new("rewrite", &tree.label, env.limits.file_rewrite_ms).on_thread_cpu();
        let root = tree.root;
        if tree.root().kind() == 0 {
            tree.node_mut(root).kind = crate::canonical::Canonical::SourceFile.into();
        }
        for stage in &env.rules_for(&tree.label).rewrite_stages {
            pattern::apply_rewrites(&mut tree, &env.lang, stage, &[run, &budget])?;
        }
        if !tree.root().is(crate::canonical::Canonical::SourceFile) {
            tree.node_mut(root).kind = crate::canonical::Canonical::SourceFile.into();
        }
        Ok(Rewritten(tree, cut))
    }
}

/// Drops every non-canonical node and the source text; what is left is what
/// the linker reads and the snapshot stores.
pub struct Canonicalize;

impl ItemPhase<Rewritten> for Canonicalize {
    type Output = Canonical;

    fn name(&self) -> Cow<'static, str> {
        "canonicalize".into()
    }

    fn run(
        &self,
        env: &Env,
        _run: &Sentinel,
        Rewritten(mut tree, cut): Rewritten,
    ) -> Result<Canonical, Killed> {
        tree.prune();
        for id in tree.preorder() {
            let sym = tree.sym_of(id, &env.lang);
            tree.node_mut(id).sym = sym;
        }
        let key = crate::tags::ReservedTags::new(&env.lang).original_source_path;
        let imports: Vec<_> = tree
            .root()
            .descendants()
            .filter_map(|node| {
                Some((
                    node.index(),
                    node.child_sym(crate::canonical::Canonical::SourcePath)?,
                ))
            })
            .collect();
        for (node, path) in imports {
            tree.set_tag(node, key, path);
        }
        tree.source = std::sync::Arc::from("");
        Ok(Canonical(tree.compact_and_remap(), cut))
    }
}

pub struct Link;

impl ItemPhase<Canonical> for Link {
    type Output = LinkedFile;

    fn name(&self) -> Cow<'static, str> {
        "link".into()
    }

    fn run(
        &self,
        env: &Env,
        run: &Sentinel,
        Canonical(tree, cut): Canonical,
    ) -> Result<LinkedFile, Killed> {
        let (edges, limited) = linker::link(&tree, env, run)?;
        Ok(LinkedFile {
            tree,
            edges,
            limited: limited.or(cut),
        })
    }
}

/// Puts the linked files on the graph, plus everything the inventory listed:
/// manifests for the resolver, and a `File` row for every unparsed file with
/// the reason, including candidates that were killed or could not be read.
pub struct Insert;

impl Phase<Workset<Vec<LinkedFile>>> for Insert {
    type Output = DirtyGraph;

    fn name(&self) -> Cow<'static, str> {
        "insert".into()
    }

    fn run(
        self,
        context: &mut Context,
        input: Workset<Vec<LinkedFile>>,
    ) -> Result<DirtyGraph, Error> {
        let Workset {
            mut state,
            items,
            mut dirty,
        } = input;
        for file in items {
            let fi = u32::try_from(state.trees.len()).expect("file index exceeds u32");
            dirty.insert(fi);
            context.report.limited.extend(file.limited);
            state.trees.push(file.tree);
            state.edges.extend(file.edges.into_iter().map(|mut e| {
                e.from_tree = fi;
                e.to_tree = fi;
                e
            }));
        }
        let lang = &context.env.lang;
        let parsed: FxHashSet<_> = state.trees.iter().map(|tree| tree.label.as_str()).collect();
        let skipped: FxHashMap<_, _> = context
            .report
            .skipped
            .iter()
            .map(|file| (file.path.as_str(), file.label.as_ref()))
            .collect();
        let unread: FxHashSet<_> = context
            .report
            .unread_files
            .iter()
            .map(String::as_str)
            .collect();
        let mut unparsed = Vec::new();
        for file in context
            .repository
            .files()
            .filter(|file| !parsed.contains(file.path.as_ref()))
        {
            let path = file.path.as_ref();
            let reason = match file.decision() {
                orbit_utils::vfs::Decision::List(reason) => format!("skip_{reason}"),
                _ if skipped.contains_key(path) => match skipped[path] {
                    "tree-sitter" => "skip_timeout_parse",
                    "stack" => "skip_stack_limit",
                    "rewrite" => "skip_timeout_walk",
                    "link" => "skip_timeout_ssa",
                    "edges" => "skip_too_many_edges",
                    _ => "skip_timeout_sentinel",
                }
                .into(),
                _ if unread.contains(path) => "fault_file_read".into(),
                _ => String::new(),
            };
            unparsed.push(Tree::unparsed(lang, path, file.size, &reason).into());
        }
        state.trees.extend(unparsed);
        Ok(DirtyGraph { state, dirty })
    }
}

/// Cross-file resolution over the dirty files. A file that overruns its
/// resolve budget keeps its intra-file edges and is reported. A family that
/// overruns keeps the edges resolved so far, is reported, and resolves in
/// full on the next run. Manifest files join the project tree so the
/// resolver can read module roots from them.
pub struct Resolve;

impl Phase<DirtyGraph> for Resolve {
    type Output = Resolved;

    fn name(&self) -> Cow<'static, str> {
        "resolve".into()
    }

    fn run(self, context: &mut Context, input: DirtyGraph) -> Result<Resolved, Error> {
        let DirtyGraph { mut state, dirty } = input;
        let env = context.env;
        let resolve_budget =
            context
                .run
                .child("resolve", "", context.env.limits.cross_file_resolve_ms);
        let walk = state.project_tree(env);
        let tree_by_path: FxHashMap<&str, usize> = state
            .trees
            .iter()
            .enumerate()
            .map(|(i, t)| (t.label.as_str(), i))
            .collect();
        let file_tags: Vec<(usize, &[Tag])> = walk
            .file_tags
            .iter()
            .filter_map(|(path, tags)| Some((*tree_by_path.get(path.as_str())?, tags.as_slice())))
            .collect();
        for tree in &mut state.trees {
            tree.clear_tags(0, &walk.tag_keys);
        }
        for (i, tags) in file_tags {
            for tag in tags {
                state.trees[i].set_tag(0, tag.key, tag.val);
            }
        }
        let result = state.resolver.resolve(
            &state.trees,
            &mut state.edges,
            &env.lang,
            &dirty,
            env.lang_id,
            &walk.lookup,
            &env.resolve.config,
            &walk.entrypoints,
            env,
            &resolve_budget,
        )?;
        context.run.check()?;
        for rsp in &result.resolved_source_paths {
            state.trees[rsp.fi as usize].storage.node_mut(rsp.node).sym = rsp.sym;
        }
        state.edges.extend(result.cross_edges);
        context.run.check()?;
        // Only files parsed in this run have an entry.
        let mut resolved = Vec::with_capacity(result.file_timings.len());
        for (fi, elapsed) in result.file_timings {
            resolved.push(fi);
            if let Some(stats) = context
                .report
                .files
                .get_mut(state.trees[fi as usize].label.as_str())
            {
                stats.resolve += elapsed;
            }
        }
        for k in result.killed {
            context.skip(k);
        }
        Ok(Resolved { state, resolved })
    }
}

/// The language's display rules: the tags export reads (`fqn`, `def_type`, ...).
pub struct Display;

impl Phase<Resolved> for Display {
    type Output = Displayed;

    fn name(&self) -> Cow<'static, str> {
        "display".into()
    }

    fn run(
        self,
        context: &mut Context,
        Resolved { mut state, .. }: Resolved,
    ) -> Result<Displayed, Error> {
        let env = context.env;
        let edges = EdgeIndex::new(&state.edges);
        let mut cut = Vec::new();
        state.trees = std::mem::take(&mut state.trees)
            .into_iter()
            .enumerate()
            .map(|(fi, tree)| {
                let rules = &env.rules_for(&tree.label).display_rules;
                if rules.is_empty() {
                    return Ok(tree);
                }
                let ctx = EdgeCtx {
                    tree_index: fi as u32,
                    edges: &edges,
                };
                let (tree, kept) =
                    pattern::apply_display(tree, &env.lang, rules, &ctx, &context.run)?;
                if let Some((key, segments)) = kept {
                    let label = env.lang.syms.resolve(key).to_string();
                    cut.push(Killed::limit(label, &tree.label, segments + 1, segments));
                }
                Ok(tree)
            })
            .collect::<Result<_, Killed>>()?;
        context.report.limited.extend(cut);
        Ok(Displayed { state })
    }
}

/// The graph as the ontology's local tables, following `config/export.yaml`.
pub struct Export<'a> {
    pub ontology: &'a Ontology,
    pub envelope: Envelope<'a>,
}

impl Phase<Displayed> for Export<'_> {
    type Output = Exported;

    fn name(&self) -> Cow<'static, str> {
        "export".into()
    }

    fn run(self, context: &mut Context, Displayed { state }: Displayed) -> Result<Exported, Error> {
        let tables = export::export(
            &state.trees,
            &state.edges,
            &context.env.lang,
            &export::ExportPlan::from_config(
                &context.env.export_config,
                self.ontology,
                &context.env.lang,
            )?,
            &self.envelope,
            true,
            &|_| true,
            &context.run,
        )?;
        Ok(Exported { state, tables })
    }
}

/// Hands each exported table to `sink` as the graph leaves the pipeline:
/// DuckDB, ClickHouse, a channel. The graph stays for the next step.
pub struct Emit<F>(pub F);

impl<F, E> Phase<Exported> for Emit<F>
where
    F: FnMut(&str, RecordBatch) -> Result<(), E>,
    E: std::fmt::Display,
{
    type Output = Displayed;

    fn name(&self) -> Cow<'static, str> {
        "emit".into()
    }

    fn run(mut self, _context: &mut Context, input: Exported) -> Result<Displayed, Error> {
        let Exported { state, tables } = input;
        for (table, batch) in tables {
            (self.0)(&table, batch).map_err(|e| {
                Error::Export(arrow::error::ArrowError::ExternalError(
                    e.to_string().into(),
                ))
            })?;
        }
        Ok(Displayed { state })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::pipeline::Observer;
    use crate::{Limits, templates};
    use orbit_utils::vfs::{Decision, File, Pass, Vfs, sources::Directory};

    struct KeepAll;

    impl Pass for KeepAll {
        type Tag = ();
        fn metadata(&self, _: &File<'_, ()>) -> Decision<()> {
            Decision::Pending
        }
        fn content(&self, _: &File<'_, ()>) -> Decision<()> {
            Decision::Keep(())
        }
    }

    struct ArmOn(&'static str, Arc<AtomicUsize>);

    impl Observer for ArmOn {
        fn started(&mut self, phase: &str) {
            if phase == self.0 {
                self.1.store(1, Ordering::Relaxed);
            }
        }

        fn finished(&mut self, phase: &str, _: Duration) {
            if phase == self.0 {
                self.1.store(0, Ordering::Relaxed);
            }
        }
    }

    fn cancelled_inside(
        phase: &'static str,
        lang: crate::treesitter::SupportLang,
        files: &[(String, String)],
        after: usize,
    ) -> Result<(), Error> {
        let repo = tempfile::tempdir().unwrap();
        for (path, content) in files {
            let path = repo.path().join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, content).unwrap();
        }
        let env = Env::with_limits(lang, Limits::UNLIMITED).unwrap();
        let repository = Arc::new(
            Vfs::load(
                Directory(repo.path()),
                KeepAll,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );
        let checks = Arc::new(AtomicUsize::new(0));
        let counted = checks.clone();
        let mut context = Context::new(&env, repository).observe(ArmOn(phase, checks));
        context.run = context.run.with_cancellation(Arc::new(move || {
            counted.load(Ordering::Relaxed) > 0 && counted.fetch_add(1, Ordering::Relaxed) >= after
        }));
        let ontology = Ontology::load_embedded().unwrap();
        let envelope = Envelope::new([
            ("project_id", export::Scalar::Int(1)),
            ("branch", export::Scalar::Str("main")),
            ("commit_sha", export::Scalar::Str("abc")),
        ]);
        templates::index(context)
            .and_then(|graph| graph.then(Display))
            .and_then(|graph| {
                graph.then(Export {
                    ontology: &ontology,
                    envelope,
                })
            })
            .map(|_| ())
    }

    #[test]
    fn export_checks_the_run_budget_inside_the_phase() {
        let files = [("a.js".to_owned(), "function a() {}\n".to_owned())];
        let lang = crate::treesitter::SupportLang::JavaScript;
        let exported = cancelled_inside("export", lang, &files, 3);
        assert!(matches!(exported, Err(Error::Killed(killed)) if killed.cancelled));
    }

    #[test]
    fn a_namespace_imported_through_many_files_links_each_use_once() {
        let (namespaces, per_namespace) = (10, 10);
        let usings: String = (0..namespaces).map(|n| format!("using Ns{n};\n")).collect();
        let calls: String = (0..namespaces).map(|n| format!(" new C{n}_0();")).collect();
        let files: Vec<_> = (0..namespaces)
            .flat_map(|n| (0..per_namespace).map(move |f| (n, f)))
            .map(|(n, f)| {
                (
                    format!("Ns{n}/C{n}_{f}.cs"),
                    format!(
                        "{usings}namespace Ns{n} {{ class C{n}_{f} {{ void M() {{{calls} }} }} }}\n"
                    ),
                )
            })
            .collect();
        let repo = tempfile::tempdir().unwrap();
        for (path, content) in &files {
            let path = repo.path().join(path);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, content).unwrap();
        }
        let env =
            Env::with_limits(crate::treesitter::SupportLang::CSharp, Limits::UNLIMITED).unwrap();
        let repository = Arc::new(
            Vfs::load(
                Directory(repo.path()),
                KeepAll,
                Default::default(),
                Default::default(),
            )
            .unwrap(),
        );
        let graph = templates::index(Context::new(&env, repository)).unwrap();
        let (_, resolved) = graph.finish();
        let mut keys: Vec<_> = resolved
            .state
            .edges
            .iter()
            .filter(|e| e.kind == crate::edge::EdgeKind::Imports && e.site.is_none())
            .map(|e| (e.from(), e.to()))
            .collect();
        let edges = keys.len();
        keys.sort_unstable();
        keys.dedup();
        assert_eq!(edges, keys.len());
    }

    #[test]
    fn display_stops_when_the_run_is_cancelled() {
        let files = [("main.py".to_string(), "def f():\n    pass\n".to_string())];
        let displayed =
            cancelled_inside("display", crate::treesitter::SupportLang::Python, &files, 2);
        assert!(matches!(displayed, Err(Error::Killed(killed)) if killed.cancelled));
    }

    #[test]
    fn cross_file_resolution_stops_when_the_run_is_cancelled_between_imports() {
        let files: Vec<_> = (0..200)
            .map(|file| {
                (
                    format!("Big/F{file}.cs"),
                    format!("namespace Big;\npublic class C{file} {{}}\n"),
                )
            })
            .collect();
        let resolved = cancelled_inside(
            "resolve",
            crate::treesitter::SupportLang::CSharp,
            &files,
            20_000,
        );
        assert!(
            matches!(resolved, Err(Error::Killed(killed)) if killed.cancelled && killed.label == "resolve")
        );
    }
}
