//! Named workflows. Each one composes phases and nothing else; callers
//! compose their own when they need to stop somewhere in between.

use std::path::Path;

use orbit_utils::fs_walk::FileInventoryEntry;

use crate::error::Error;
use crate::pipeline::{
    Canonicalize, Context, Each, Insert, ItemPhase, Link, Parse, Pipeline, Prepare, Resolve,
    Resolved, Rewrite, Sources,
};

/// Every file of the repository: parse entries go through parse, rewrite,
/// link and cross-file resolution; everything else becomes a `File` row
/// carrying the reason it was not parsed.
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
