//! Named workflows. Each one composes phases and nothing else; the CLI and
//! tests compose their own when they need to stop somewhere in between.
//! Input is production's file inventory (see `inventory`).

use std::path::Path;

use orbit_utils::fs_walk::FileInventoryEntry;

use crate::error::Error;
use crate::pipeline::{
    Canonicalize, Changes, Context, Each, Insert, ItemPhase, Link, Parse, Pipeline, Prepare,
    ReindexInput, Remap, Resolve, Resolved, Rewrite, Sources, State,
};

/// Every file of the repository: `Parse` entries go through parse,
/// rewrite, link and cross-file resolution; everything else becomes a
/// `File` row carrying the reason it was not parsed. `Parse` entries are
/// read from `root` as workers take them.
pub fn index<'e, S>(
    context: Context<'e>,
    root: &Path,
    inventory: S,
) -> Result<Pipeline<'e, Resolved>, Error>
where
    S: IntoIterator<Item = FileInventoryEntry>,
{
    let sources = Sources {
        root: root.to_path_buf(),
        entries: inventory.into_iter().collect(),
    };
    Pipeline::new(context, sources)
        .then(Prepare)?
        .then(Each(Parse.pipe(Rewrite).pipe(Canonicalize).pipe(Link)))?
        .then(Insert)?
        .then(Resolve)
}

/// Only the changed files go through the per-file phases; resolution
/// revisits them and everything that depended on what they replaced.
pub fn reindex<'e>(
    context: Context<'e>,
    state: State,
    root: &Path,
    changes: Changes,
) -> Result<Pipeline<'e, Resolved>, Error> {
    let input = ReindexInput {
        state,
        root: root.to_path_buf(),
        changes,
    };
    Pipeline::new(context, input)
        .then(Remap)?
        .then(Each(Parse.pipe(Rewrite).pipe(Canonicalize).pipe(Link)))?
        .then(Insert)?
        .then(Resolve)
}
