//! What flows through the pipeline. Each artifact is a checkpoint: holding
//! one says which phases may follow.

use std::path::PathBuf;

use arrow::record_batch::RecordBatch;
use code_graph::v2::error::FileReason;
use orbit_utils::fs_walk::FileInventoryEntry;
use rustc_hash::{FxHashMap, FxHashSet};

use super::{SourceFile, State};
use crate::tree::{Edge, Tree};

/// A repository's classified files; parse entries are read from `root` as
/// workers take them.
pub struct Sources {
    pub root: PathBuf,
    pub entries: Vec<FileInventoryEntry>,
}

/// Files changed since the graph was built, already classified.
pub struct Changes {
    pub changed: Vec<FileInventoryEntry>,
    pub removed: Vec<String>,
}

pub struct ReindexInput {
    pub state: State,
    pub root: PathBuf,
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
#[allow(dead_code)]
#[derive(Default)]
pub struct Listed {
    manifests: Vec<SourceFile>,
    files: Vec<(String, u64, FileReason)>,
    candidates: FxHashMap<String, u64>,
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
