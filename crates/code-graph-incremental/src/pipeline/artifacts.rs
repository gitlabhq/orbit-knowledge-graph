//! What flows through the pipeline. Each artifact is a checkpoint: holding
//! one says which phases may follow.

use std::sync::{Arc, Mutex};

use crate::inventory::FileReason;
use arrow::record_batch::RecordBatch;
use orbit_utils::files::{File, Vfs};
use rustc_hash::{FxHashMap, FxHashSet};

use super::{SourceFile, State};
use crate::tree::{Edge, Tree};

/// A repository's classified files; parse entries are read from `repo` as
/// workers take them.
pub struct Sources {
    pub repo: Arc<Vfs>,
    pub entries: Vec<File>,
}

/// Files changed since the graph was built, already classified.
pub struct Changes {
    pub changed: Vec<File>,
    pub removed: Vec<String>,
}

pub struct ReindexInput {
    pub state: State,
    pub repo: Arc<Vfs>,
    pub changes: Changes,
}

/// The graph so far plus the items still moving through the per-item
/// phases; `dirty` is every file the resolver must revisit.
pub struct Workset<C> {
    pub state: State,
    pub items: C,
    pub dirty: FxHashSet<usize>,
    pub listed: Listed,
}

pub type Lazy<T> = Box<dyn Iterator<Item = T> + Send>;

/// What the inventory held besides parseable code: manifests for the
/// resolver, every other file with the reason it was not parsed, and each
/// parse candidate's size so a killed or unreadable one still gets a row.
#[derive(Default)]
pub struct Listed {
    pub(super) manifests: Vec<SourceFile>,
    pub(super) files: Vec<(String, u64, FileReason)>,
    pub(super) candidates: FxHashMap<String, u64>,
    /// Candidates the content passes turned down when a worker read them.
    pub(super) rejected: Arc<Mutex<Vec<(String, u64, FileReason)>>>,
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
