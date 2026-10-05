//! What flows through the pipeline. Each artifact is a checkpoint: holding
//! one says which phases may follow.

use arrow::record_batch::RecordBatch;
use code_graph::v2::config::Role;
use orbit_utils::vfs::Vfs;
use rustc_hash::FxHashSet;
use std::sync::Arc;

use super::{SourceFile, State};
use crate::tree::{Edge, Tree};

pub type Sources = Arc<Vfs<Role>>;

/// Files changed since the graph was built, already classified.
pub struct Changes {
    pub changed: Sources,
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

pub struct Listed {
    pub(super) repo: Sources,
    pub(super) manifests: Vec<SourceFile>,
    pub(super) candidates: FxHashSet<String>,
    pub(super) unread_manifests: FxHashSet<String>,
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
