//! What flows through the pipeline. Each artifact is a checkpoint: holding
//! one says which phases may follow.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use orbit_utils::files::Vfs;
use rustc_hash::{FxHashMap, FxHashSet};

use super::{SourceFile, State};
use crate::tree::{Edge, Tree};

/// A repository, its files decided; parse entries are read as workers take
/// them.
pub type Sources = Arc<Vfs>;

/// Files changed since the graph was built: the changed ones as a
/// repository of their own, the removed ones by path.
pub struct Changes {
    pub changed: Arc<Vfs>,
    pub removed: Vec<String>,
}

pub struct ReindexInput {
    pub state: State,
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

/// What the repository held besides parseable code: manifests for the
/// resolver, and each parse candidate's size so a killed or unreadable one
/// still gets a row. The repository itself answers for every other file.
pub struct Listed {
    pub(super) repo: Arc<Vfs>,
    pub(super) manifests: Vec<SourceFile>,
    /// Manifests the repository lists but could not read; they get a row
    /// that says so.
    pub(super) unread_manifests: Vec<(String, u64)>,
    pub(super) candidates: FxHashMap<String, u64>,
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
