//! Named workflows. Each one composes phases and nothing else; callers
//! compose their own when they need to stop somewhere in between.

use std::sync::Arc;

use orbit_utils::files::Vfs;

use crate::error::Error;
use crate::pipeline::{
    Canonicalize, Changes, Context, Each, Insert, ItemPhase, Link, Parse, Pipeline, Prepare,
    ReindexInput, Remap, Resolve, Resolved, Rewrite, State,
};

/// Every file of the repository: parse entries go through parse, rewrite,
/// link and cross-file resolution; everything else becomes a `File` row
/// carrying the reason it was not parsed.
pub fn index<'e>(context: Context<'e>, repo: Arc<Vfs>) -> Result<Pipeline<'e, Resolved>, Error> {
    Pipeline::new(context, repo)
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
    changes: Changes,
) -> Result<Pipeline<'e, Resolved>, Error> {
    let input = ReindexInput { state, changes };
    Pipeline::new(context, input)
        .then(Remap)?
        .then(Each(Parse.pipe(Rewrite).pipe(Canonicalize).pipe(Link)))?
        .then(Insert)?
        .then(Resolve)
}
