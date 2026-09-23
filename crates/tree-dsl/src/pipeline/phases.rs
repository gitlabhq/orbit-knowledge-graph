//! What flows through the pipeline and the phases that move it, in the
//! order they run. Each artifact type is a checkpoint: holding one says
//! which phases may follow.

use std::borrow::Cow;

use arrow::record_batch::RecordBatch;
use ontology::Ontology;
use rayon::prelude::*;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::env::Env;
use crate::error::Error;
use crate::export::{self, Envelope};
use crate::file_tree::ProjectTree;
use crate::pattern::{self, EdgeCtx};
use crate::sentinel::{Killed, Sentinel};
use crate::tree::{Edge, Tree};
use crate::treesitter::{self, SupportLang};
use crate::{linker, rules};

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

pub struct Changes {
    pub added: Vec<SourceFile>,
    pub modified: Vec<SourceFile>,
    pub removed: Vec<String>,
}

pub struct ReindexInput {
    pub state: State,
    pub changes: Changes,
}

/// The graph so far plus the items still moving through the per-item
/// phases. `dirty` is every file the resolver must revisit; `configs` are
/// the manifest files (`parse_files`) the resolver reads for module roots.
pub struct Workset<F> {
    pub state: State,
    pub items: Vec<F>,
    pub dirty: FxHashSet<usize>,
    pub configs: Vec<SourceFile>,
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
    pub configs: Vec<SourceFile>,
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

impl Phase<Vec<SourceFile>> for Prepare {
    type Output = Workset<SourceFile>;

    fn name(&self) -> Cow<'static, str> {
        "prepare".into()
    }

    fn run(self, context: &mut Context, sources: Vec<SourceFile>) -> Result<Self::Output, Error> {
        let env = context.env;
        let (files, configs) = split_sources(env, sources);
        Ok(Workset {
            state: State::new(env),
            items: files,
            dirty: FxHashSet::default(),
            configs,
        })
    }
}

/// Incremental index: removed and modified files leave the graph, edges and
/// resolver locations follow the compacted file indices, and every retained
/// file that pointed at a removed one is marked dirty.
pub struct Remap;

impl Phase<ReindexInput> for Remap {
    type Output = Workset<SourceFile>;

    fn name(&self) -> Cow<'static, str> {
        "remap".into()
    }

    fn run(self, context: &mut Context, input: ReindexInput) -> Result<Self::Output, Error> {
        let ReindexInput { mut state, changes } = input;
        let old_labels: Vec<String> = state.trees.iter().map(|t| t.label.clone()).collect();
        let dirty_labels: FxHashSet<&str> = changes
            .removed
            .iter()
            .map(String::as_str)
            .chain(changes.modified.iter().map(|f| f.path.as_str()))
            .collect();
        let dirty = remap(&mut state, &old_labels, &dirty_labels);
        let sources: Vec<SourceFile> = changes.modified.into_iter().chain(changes.added).collect();
        let (files, configs) = split_sources(context.env, sources);
        Ok(Workset {
            state,
            items: files,
            dirty,
            configs,
        })
    }
}

/// Parseable sources go through the pipeline; manifest files named in
/// `parse_files` are kept for the resolver. A file can be both.
fn split_sources(env: &Env, sources: Vec<SourceFile>) -> (Vec<SourceFile>, Vec<SourceFile>) {
    let is_config = |path: &str| {
        let name = path.rsplit('/').next().unwrap_or(path);
        env.config
            .resolve
            .parse_files
            .iter()
            .any(|pf| pf.name == name)
    };
    let mut files = Vec::new();
    let mut configs = Vec::new();
    for source in sources {
        match (
            is_config(&source.path),
            SupportLang::from_path(&source.path).is_some(),
        ) {
            (true, true) => {
                configs.push(SourceFile {
                    path: source.path.clone(),
                    content: source.content.clone(),
                });
                files.push(source);
            }
            (true, false) => configs.push(source),
            (false, true) => files.push(source),
            (false, false) => {}
        }
    }
    (files, configs)
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

impl<I: Send, P: ItemPhase<I> + Sync> Phase<Workset<I>> for Each<P>
where
    P::Output: Send,
{
    type Output = Workset<P::Output>;

    fn name(&self) -> Cow<'static, str> {
        self.0.name()
    }

    fn run(self, context: &mut Context, input: Workset<I>) -> Result<Self::Output, Error> {
        let Workset {
            state,
            items,
            dirty,
            configs,
        } = input;
        let (env, run, phase) = (context.env, &context.run, &self.0);
        let (items, killed): (Vec<_>, Vec<_>) = items
            .into_par_iter()
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
            configs,
        })
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
        for stage in &env.rewrite_stages {
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

impl Phase<Workset<LinkedFile>> for Insert {
    type Output = DirtyGraph;

    fn name(&self) -> Cow<'static, str> {
        "insert".into()
    }

    fn run(self, _context: &mut Context, input: Workset<LinkedFile>) -> Result<DirtyGraph, Error> {
        let Workset {
            mut state,
            items,
            mut dirty,
            configs,
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
        Ok(DirtyGraph {
            state,
            dirty,
            configs,
        })
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
        let DirtyGraph {
            mut state,
            dirty,
            configs,
        } = input;
        let env = context.env;
        let paths: Vec<&str> = state
            .trees
            .iter()
            .map(|t| t.label.as_str())
            .chain(configs.iter().map(|f| f.path.as_str()))
            .collect();
        let walk = ProjectTree::build(
            &env.lang,
            &env.config.resolve,
            &env.resolve_stages,
            &paths,
            Some(&configs),
        );
        let result = state.resolver.resolve(
            &state.trees,
            &state.edges,
            &env.lang,
            &dirty,
            env.lang_id,
            &walk.prefixes,
            &env.config.resolve,
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
        let Some(yaml) = treesitter::lang_yaml(env.lang_id) else {
            return Ok(Displayed { state });
        };
        let config = rules::load_lang_full(yaml, &env.lang)?;
        for (fi, tree) in state.trees.iter_mut().enumerate() {
            let ctx = EdgeCtx {
                tree_index: fi as u32,
                edges: &state.edges,
            };
            let _ = pattern::apply_rewrites_with_edges(
                tree,
                &env.lang,
                &config.display_rules,
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
