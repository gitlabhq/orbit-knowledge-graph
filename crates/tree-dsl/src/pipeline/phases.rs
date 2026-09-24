//! What flows through the pipeline and the phases that move it, in the
//! order they run. Each artifact type is a checkpoint: holding one says
//! which phases may follow.

use std::borrow::Cow;
use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use ontology::Ontology;
use orbit_utils::fs_walk::{Decision, FileInventoryEntry};
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::canonical::Canonical as CanonicalKind;
use crate::env::Env;
use crate::error::Error;
use crate::export::{self, Envelope};
use crate::file_tree::ProjectTree;
use crate::intern::Lang;
use crate::inventory::{FileFault, FileReason};
use crate::linker;
use crate::pattern::{self, EdgeCtx};
use crate::sentinel::{Killed, Sentinel};
use crate::tree::{Edge, Node, Tree};
use crate::treesitter::{self, SupportLang};

use super::{Context, ItemPhase, Phase, State};

pub struct SourceFile {
    pub path: String,
    pub content: String,
}

impl From<(String, String)> for SourceFile {
    fn from((path, content): (String, String)) -> Self {
        Self { path, content }
    }
}

/// A repository's classified files; `Parse` entries are read from `root`
/// as workers take them.
pub struct Sources {
    pub root: PathBuf,
    pub entries: Vec<FileInventoryEntry>,
}

/// Files changed since the graph was built, already classified.
pub struct Changes {
    pub changed: Vec<FileInventoryEntry>,
    pub removed: Vec<String>,
}

pub struct ReindexInput {
    pub state: State,
    pub root: PathBuf,
    pub changes: Changes,
}

/// The graph so far plus the items still moving through the per-item
/// phases. `items` is a `Vec` once a phase has run; before that it is a
/// lazy source so files are read only as workers take them. `dirty` is
/// every file the resolver must revisit.
pub struct Workset<C> {
    pub state: State,
    pub items: C,
    pub dirty: FxHashSet<usize>,
    pub listed: Listed,
}

/// Sources not yet read into memory.
pub type Lazy<T> = Box<dyn Iterator<Item = T> + Send>;

/// What the inventory held besides parseable code, for `Insert` to put on
/// the graph: manifests (`parse_files`) for the resolver, every other file
/// as a `File` row with the reason it was not parsed, and each parse
/// candidate's size so one that was killed or could not be read still
/// gets its row.
#[derive(Default)]
pub struct Listed {
    manifests: Vec<SourceFile>,
    files: Vec<(String, u64, FileReason)>,
    candidates: FxHashMap<String, u64>,
}

/// The tree-sitter tree, source attached.
pub struct Parsed(pub Tree);

/// Rewrite rules applied; language nodes still present.
pub struct Rewritten(pub Tree);

/// Only canonical nodes remain; the source text is gone.
pub struct Canonical(pub Tree);

pub struct LinkedFile {
    pub tree: Tree,
    pub edges: Vec<Edge>,
}

pub struct DirtyGraph {
    pub state: State,
    pub dirty: FxHashSet<usize>,
}

pub struct Resolved {
    pub state: State,
}

pub struct Displayed {
    pub state: State,
}

pub struct Exported {
    pub state: State,
    pub tables: Vec<(String, RecordBatch)>,
}

pub struct Prepare;

impl Phase<Sources> for Prepare {
    type Output = Workset<Lazy<SourceFile>>;

    fn name(&self) -> Cow<'static, str> {
        "prepare".into()
    }

    fn run(self, context: &mut Context, sources: Sources) -> Result<Self::Output, Error> {
        let Sources { root, entries } = sources;
        Ok(workset(
            context.env,
            State::new(context.env),
            root,
            entries,
            FxHashSet::default(),
        ))
    }
}

/// Incremental index: removed and modified files leave the graph, edges and
/// resolver locations follow the compacted file indices, and every retained
/// file that pointed at a removed one is marked dirty.
pub struct Remap;

impl Phase<ReindexInput> for Remap {
    type Output = Workset<Lazy<SourceFile>>;

    fn name(&self) -> Cow<'static, str> {
        "remap".into()
    }

    fn run(self, context: &mut Context, input: ReindexInput) -> Result<Self::Output, Error> {
        let ReindexInput {
            mut state,
            root,
            changes,
        } = input;
        let old_labels: Vec<String> = state.trees.iter().map(|t| t.label.clone()).collect();
        let dirty_labels: FxHashSet<&str> = changes
            .removed
            .iter()
            .map(String::as_str)
            .chain(changes.changed.iter().map(|f| f.path.as_str()))
            .collect();
        let dirty = remap(&mut state, &old_labels, &dirty_labels);
        state
            .configs
            .retain(|c| !dirty_labels.contains(c.path.as_str()));
        Ok(workset(context.env, state, root, changes.changed, dirty))
    }
}

/// `Parse` entries this crate has a grammar for become the lazy workset,
/// read from `root` when a worker takes them. Everything else is listed
/// now: manifests for the resolver, and every file as a `File` row.
fn workset(
    env: &Env,
    state: State,
    root: PathBuf,
    entries: Vec<FileInventoryEntry>,
    dirty: FxHashSet<usize>,
) -> Workset<Lazy<SourceFile>> {
    let manifest_names = &env.rules.config.resolve.parse_files;
    let is_manifest = |path: &str| {
        let name = path.rsplit('/').next().unwrap_or(path);
        manifest_names.iter().any(|pf| pf.name == name)
    };
    let mut listed = Listed::default();
    let mut candidates = Vec::new();
    for entry in entries {
        let FileInventoryEntry {
            path,
            size,
            decision,
            label,
        } = entry;
        if decision == Decision::Parse && SupportLang::from_path(&path).is_some() {
            listed.candidates.insert(path.clone(), size);
            candidates.push(path);
            continue;
        }
        let manifest = decision == Decision::Load && is_manifest(&path);
        let content = manifest
            .then(|| std::fs::read_to_string(root.join(&path)).ok())
            .flatten();
        let reason = match (decision, label.skip) {
            (Decision::ListOnly, Some(skip)) => FileReason::Filter(skip),
            _ if manifest && content.is_none() => FileReason::Fault(FileFault::FileRead),
            _ => FileReason::None,
        };
        if let Some(content) = content {
            listed.manifests.push(SourceFile {
                path: path.clone(),
                content,
            });
        }
        listed.files.push((path, size, reason));
    }
    let items = candidates.into_iter().filter_map(move |path| {
        let content = std::fs::read_to_string(root.join(&path)).ok()?;
        Some(SourceFile { path, content })
    });
    Workset {
        state,
        items: Box::new(items),
        dirty,
        listed,
    }
}

/// Drops the dirty trees, renumbers what remains, and returns the retained
/// files whose resolution depended on a dropped one.
fn remap(state: &mut State, old_labels: &[String], dirty: &FxHashSet<&str>) -> FxHashSet<usize> {
    state.trees.retain(|t| !dirty.contains(t.label.as_str()));

    let label_to_fi: FxHashMap<&str, u32> = state
        .trees
        .iter()
        .enumerate()
        .map(|(i, t)| (t.label.as_str(), i as u32))
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

    let old_dirty_fis: FxHashSet<usize> = old_labels
        .iter()
        .enumerate()
        .filter(|(_, l)| dirty.contains(l.as_str()))
        .map(|(i, _)| i)
        .collect();
    let reverse_dirty: FxHashSet<usize> = state
        .resolver
        .reqs()
        .iter()
        .filter(|r| old_dirty_fis.contains(&r.target_fi))
        .filter_map(|r| label_to_fi.get(old_labels[r.fi].as_str()))
        .map(|&fi| fi as usize)
        .collect();

    state.resolver.remap(old_labels, &label_to_fi);
    reverse_dirty
}

/// Runs an `ItemPhase` over every item of a workset in parallel. An item that
/// overruns its budget is skipped and reported; the rest continue.
pub struct Each<P>(pub P);

impl<C, P> Phase<Workset<C>> for Each<P>
where
    C: IntoParallel,
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
            listed,
        } = input;
        let (env, run, phase) = (context.env, &context.run, &self.0);
        let (items, killed): (Vec<_>, Vec<_>) = items
            .into_parallel()
            .map(|item| phase.run(env, run, item))
            .partition_map(|r| match r {
                Ok(v) => rayon::iter::Either::Left(v),
                Err(k) => rayon::iter::Either::Right(k),
            });
        context.run.check()?;
        for k in killed {
            context.skip(k);
        }
        Ok(Workset {
            state,
            items,
            dirty,
            listed,
        })
    }
}

/// What `Each` can fan out over: a `Vec` of items already in memory, or a
/// lazy source pulled one item at a time as workers free up.
pub trait IntoParallel {
    type Item: Send;
    fn into_parallel(self) -> impl ParallelIterator<Item = Self::Item>;
}

impl<T: Send> IntoParallel for Vec<T> {
    type Item = T;
    fn into_parallel(self) -> impl ParallelIterator<Item = T> {
        self.into_par_iter()
    }
}

impl<T: Send> IntoParallel for Lazy<T> {
    type Item = T;
    fn into_parallel(self) -> impl ParallelIterator<Item = T> {
        self.par_bridge()
    }
}

/// tree-sitter, with the grammar the file's own extension names when it
/// belongs to this pipeline (TSX inside the TypeScript pipeline).
pub struct Parse;

impl ItemPhase<SourceFile> for Parse {
    type Output = Parsed;

    fn name(&self) -> Cow<'static, str> {
        "parse".into()
    }

    fn run(&self, env: &Env, _run: &Sentinel, file: SourceFile) -> Result<Parsed, Killed> {
        let grammar = SupportLang::from_path(&file.path)
            .filter(|l| l.pipeline() == env.lang_id.pipeline())
            .unwrap_or(env.lang_id);
        treesitter::parse(&file.content, grammar, &env.lang, &file.path).map(Parsed)
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
        Parsed(mut tree): Parsed,
    ) -> Result<Rewritten, Killed> {
        let budget = Sentinel::new("rewrite", &tree.label, env.limits.file_rewrite_ms);
        for stage in &env.rules.rewrite_stages {
            pattern::apply_rewrites(&mut tree, &env.lang, stage, &[run, &budget])?;
        }
        Ok(Rewritten(tree))
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
        _env: &Env,
        _run: &Sentinel,
        Rewritten(mut tree): Rewritten,
    ) -> Result<Canonical, Killed> {
        tree.prune();
        tree.compact();
        tree.source = std::sync::Arc::from("");
        Ok(Canonical(tree))
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
        Canonical(tree): Canonical,
    ) -> Result<LinkedFile, Killed> {
        let edges = linker::link(&tree, env, run)?;
        Ok(LinkedFile { tree, edges })
    }
}

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
            listed,
        } = input;
        for file in items {
            let fi = state.trees.len();
            dirty.insert(fi);
            state.trees.push(file.tree);
            state.edges.extend(file.edges.into_iter().map(|mut e| {
                e.from_tree = fi as u32;
                e.to_tree = fi as u32;
                e
            }));
        }
        let Listed {
            manifests,
            files,
            mut candidates,
        } = listed;
        for manifest in manifests {
            state.configs.retain(|c| c.path != manifest.path);
            state.configs.push(manifest);
        }
        let lang = &context.env.lang;
        for (path, size, reason) in files {
            state.trees.push(listed_file(lang, &path, size, reason));
        }
        for tree in &state.trees {
            candidates.remove(&tree.label);
        }
        for killed in &context.report.skipped {
            if let Some(size) = candidates.remove(&killed.path) {
                let reason = crate::inventory::timeout(killed.label);
                state
                    .trees
                    .push(listed_file(lang, &killed.path, size, reason));
            }
        }
        for (path, size) in candidates {
            let reason = FileReason::Fault(FileFault::FileRead);
            state.trees.push(listed_file(lang, &path, size, reason));
        }
        Ok(DirtyGraph { state, dirty })
    }
}

/// A `File` row for a file that was not parsed: a bare `__source_file`
/// spanning its size, tagged with why.
fn listed_file(lang: &Lang, path: &str, size: u64, reason: FileReason) -> Tree {
    let mut tree = Tree::new(Node {
        kind: CanonicalKind::SourceFile.into(),
        named: true,
        sym: lang.syms.intern(path),
        end: size as u32,
        ..Default::default()
    });
    tree.label = path.to_string();
    let reason = reason.to_string();
    if !reason.is_empty() {
        tree.set_tag(0, lang.syms.intern("reason"), lang.syms.intern(&reason));
    }
    tree
}

/// Cross-file resolution over the dirty files. A file that overruns its
/// resolve budget keeps its intra-file edges and is reported. Manifest files
/// join the project tree so the resolver can read module roots from them.
pub struct Resolve;

impl Phase<DirtyGraph> for Resolve {
    type Output = Resolved;

    fn name(&self) -> Cow<'static, str> {
        "resolve".into()
    }

    fn run(self, context: &mut Context, input: DirtyGraph) -> Result<Resolved, Error> {
        let DirtyGraph { mut state, dirty } = input;
        let env = context.env;
        let paths: Vec<&str> = state
            .trees
            .iter()
            .map(|t| t.label.as_str())
            .chain(state.configs.iter().map(|f| f.path.as_str()))
            .collect();
        let walk = ProjectTree::build(
            &env.lang,
            &env.rules.config.resolve,
            &env.rules.resolve_stages,
            &paths,
            Some(&state.configs),
        );
        let result = state.resolver.resolve(
            &state.trees,
            &state.edges,
            &env.lang,
            &dirty,
            env.lang_id,
            &walk.prefixes,
            &env.rules.config.resolve,
            &walk.aliases,
            env,
            &context.run,
        )?;
        for rsp in &result.resolved_source_paths {
            let nid = state.trees[rsp.fi].to_id(rsp.node);
            state.trees[rsp.fi].node_mut(nid).sym = rsp.sym;
        }
        state.edges.extend(result.cross_edges);
        context.run.check()?;
        for k in result.killed {
            context.skip(k);
        }
        Ok(Resolved { state })
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
        Resolved { mut state }: Resolved,
    ) -> Result<Displayed, Error> {
        let env = context.env;
        for (fi, tree) in state.trees.iter_mut().enumerate() {
            let ctx = EdgeCtx {
                tree_index: fi as u32,
                edges: &state.edges,
            };
            let _ = pattern::apply_rewrites_with_edges(
                tree,
                &env.lang,
                &env.rules.display_rules,
                true,
                &ctx,
                &[],
            );
        }
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
        let tables = export::export(&state, &context.env.lang, self.ontology, &self.envelope)?;
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
