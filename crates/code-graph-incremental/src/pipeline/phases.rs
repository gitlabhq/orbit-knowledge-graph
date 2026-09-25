//! The phases, in the order they run.

use std::borrow::Cow;
use std::path::PathBuf;

use orbit_utils::fs_walk::{Decision, FileInventoryEntry};
use rayon::prelude::*;
use rustc_hash::FxHashSet;

use super::{
    Context, Error, ItemPhase, Lazy, Listed, Parsed, Phase, SourceFile, Sources, State, Workset,
};
use crate::env::Env;
use crate::inventory::FileReason;
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
            State::new(context.env),
            root,
            entries,
            FxHashSet::default(),
        ))
    }
}

/// Parse entries this crate has a grammar for become the lazy workset, read
/// from `root` when a worker takes them; every other file is listed now.
fn workset(
    state: State,
    root: PathBuf,
    entries: Vec<FileInventoryEntry>,
    dirty: FxHashSet<usize>,
) -> Workset<Lazy<SourceFile>> {
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
        let reason = match (decision, label.skip) {
            (Decision::ListOnly, Some(skip)) => FileReason::Filter(skip),
            _ => FileReason::None,
        };
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
