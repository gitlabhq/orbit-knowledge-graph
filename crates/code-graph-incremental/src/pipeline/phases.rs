//! The phases, in the order they run.

use std::borrow::Cow;
use std::path::PathBuf;

use orbit_utils::fs_walk::{Decision, FileInventoryEntry};
use rayon::prelude::*;
use rustc_hash::FxHashSet;

use super::{
    Canonical, Context, Error, ItemPhase, Lazy, Listed, Parsed, Phase, Rewritten, SourceFile,
    Sources, State, Workset,
};
use crate::env::Env;
use crate::inventory::{FileFault, FileReason};
use crate::pattern;
use crate::sentinel::{Killed, Sentinel};
use crate::treesitter::{self, SupportLang};

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

/// Parse entries of this pipeline's languages become the lazy workset, read
/// from `root` when a worker takes them. Everything else is listed now:
/// manifests for the resolver, and every file as a `File` row.
fn workset(
    env: &Env,
    state: State,
    root: PathBuf,
    entries: Vec<FileInventoryEntry>,
    dirty: FxHashSet<usize>,
) -> Workset<Lazy<SourceFile>> {
    let pipeline = env.lang_id.pipeline();
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
        let in_pipeline = SupportLang::from_path(&path).is_some_and(|l| l.pipeline() == pipeline);
        if decision == Decision::Parse && in_pipeline {
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

impl<T: Send> IntoParallel for Lazy<T> {
    type Item = T;
    fn into_parallel(self) -> impl ParallelIterator<Item = T> {
        self.par_bridge()
    }
}

/// tree-sitter, with the grammar the file's own extension names (TSX inside
/// the TypeScript pipeline).
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
