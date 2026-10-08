//! The phases, in the order they run.

use std::borrow::Cow;
use std::path::PathBuf;
use std::time::Instant;

use orbit_utils::fs_walk::{Decision, FileInventoryEntry};
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

use arrow::record_batch::RecordBatch;
use ontology::Ontology;

use super::{
    Canonical, Context, DirtyGraph, Displayed, Error, Exported, FileTiming, ItemPhase, Labelled,
    LinkedFile, Listed, Parsed, Phase, ReindexInput, Resolved, Rewritten, SourceFile, SourcePaths,
    Sources, State, Workset,
};
use crate::env::Env;
use crate::export::{self, Envelope};
use crate::file_tree::ProjectTree;
use crate::inventory::{FileFault, FileReason};
use crate::linker;
use crate::pattern::{self, EdgeCtx, EdgeIndex};
use crate::sentinel::{Killed, Sentinel};
use crate::tree::{Edge, Tag, Tree};
use crate::treesitter::{self, SupportLang};

pub struct Prepare;

impl Phase<Sources> for Prepare {
    type Output = Workset<SourcePaths>;

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

/// Parse entries of this pipeline's languages become the lazy workset, read
/// from `root` when a worker takes them. Everything else is listed now:
/// manifests for the resolver, and every file as a `File` row.
fn workset(
    env: &Env,
    state: State,
    root: PathBuf,
    entries: Vec<FileInventoryEntry>,
    dirty: FxHashSet<u32>,
) -> Workset<SourcePaths> {
    let manifest_names = &env.resolve.config.parse_files;
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
        let manifest = decision != Decision::ListOnly && is_manifest(&path);
        let in_family = SupportLang::from_path(&path).is_some_and(|l| env.in_family(l));
        if decision == Decision::Parse && in_family && !manifest {
            listed.candidates.insert(path.clone(), size);
            candidates.push(path);
            continue;
        }
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
    Workset {
        state,
        items: SourcePaths {
            root,
            paths: candidates,
        },
        dirty,
        listed,
    }
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
            listed,
        } = input;
        let (env, run, phase) = (context.env, &context.run, &self.0);
        let (outcomes, timings): (Vec<_>, Vec<_>) = items
            .into_parallel()
            .map(|item| {
                let path = item.label().to_string();
                let started = Instant::now();
                let outcome = phase.run(env, run, item);
                (outcome, (path, started.elapsed()))
            })
            .unzip();
        context.run.check()?;
        let phase_name = self.0.name();
        context
            .report
            .files
            .extend(timings.into_iter().map(|(path, elapsed)| FileTiming {
                path,
                phase: phase_name.to_string(),
                elapsed,
            }));
        let mut items = Vec::with_capacity(outcomes.len());
        for outcome in outcomes {
            match outcome {
                Ok(v) => items.push(v),
                Err(k) => context.skip(k),
            }
        }
        Ok(Workset {
            state,
            items,
            dirty,
            listed,
        })
    }
}

/// What `Each` fans out over: items already in memory, or a lazy source
/// pulled one item at a time as workers free up.
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

impl IntoParallel for SourcePaths {
    type Item = SourceFile;
    fn into_parallel(self) -> impl ParallelIterator<Item = SourceFile> {
        self.paths.into_par_iter().filter_map(move |path| {
            let content = std::fs::read_to_string(self.root.join(&path)).ok()?;
            Some(SourceFile { path, content })
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

    fn run(&self, env: &Env, _run: &Sentinel, file: SourceFile) -> Result<Parsed, Killed> {
        let grammar = SupportLang::from_path(&file.path).unwrap_or(env.lang_id);
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
        for stage in &env.rules_for(&tree.label).rewrite_stages {
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
            listed,
        } = input;
        for file in items {
            let fi = u32::try_from(state.trees.len()).expect("file index exceeds u32");
            dirty.insert(fi);
            state.trees.push(file.tree);
            state.edges.extend(file.edges.into_iter().map(|mut e| {
                e.from_tree = fi;
                e.to_tree = fi;
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
            state
                .trees
                .push(Tree::unparsed(lang, &path, size, &reason.to_string()));
        }
        for tree in &state.trees {
            candidates.remove(&tree.label);
        }
        for killed in &context.report.skipped {
            if let Some(size) = candidates.remove(&killed.path) {
                let reason = crate::inventory::timeout(killed.label);
                state.trees.push(Tree::unparsed(
                    lang,
                    &killed.path,
                    size,
                    &reason.to_string(),
                ));
            }
        }
        for (path, size) in candidates {
            let reason = FileReason::Fault(FileFault::FileRead);
            state
                .trees
                .push(Tree::unparsed(lang, &path, size, &reason.to_string()));
        }
        Ok(DirtyGraph { state, dirty })
    }
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
            &env.resolve.config,
            &env.resolve.stages,
            &paths,
            Some(&state.configs),
        );
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
            &state.edges,
            &env.lang,
            &dirty,
            env.lang_id,
            &walk.prefixes,
            &env.resolve.config,
            &walk.aliases,
            env,
            &context.run,
        )?;
        for rsp in &result.resolved_source_paths {
            let nid = state.trees[rsp.fi as usize].to_id(rsp.node);
            state.trees[rsp.fi as usize].node_mut(nid).sym = rsp.sym;
        }
        state.edges.extend(result.cross_edges);
        context.run.check()?;
        context
            .report
            .files
            .extend(
                result
                    .file_timings
                    .into_iter()
                    .map(|(fi, elapsed)| FileTiming {
                        path: state.trees[fi as usize].label.clone(),
                        phase: "resolve".to_string(),
                        elapsed,
                    }),
            );
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
        let edges = EdgeIndex::new(&state.edges);
        for (fi, tree) in state.trees.iter_mut().enumerate() {
            let ctx = EdgeCtx {
                tree_index: fi as u32,
                edges: &edges,
            };
            let _ = pattern::apply_rewrites_with_edges(
                tree,
                &env.lang,
                &env.rules_for(&tree.label).display_rules,
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
