//! What flows through the pipeline. Each artifact is a checkpoint: holding
//! one says which phases may follow.

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use orbit_utils::vfs::Vfs;
use rustc_hash::FxHashSet;

use super::{SourceFile, State};
use crate::sentinel::Killed;
use crate::tree::{Compact, Edge, Tree};

pub type Sources = Arc<Vfs<()>>;

pub fn candidates(
    repository: &Vfs<()>,
    max_files: usize,
) -> impl Iterator<Item = &orbit_utils::vfs::File<'static, ()>> {
    repository
        .files()
        .filter(|file| {
            file.keeps()
                && crate::treesitter::SupportLang::from_path(&file.path)
                    .is_some_and(crate::treesitter::SupportLang::has_rules)
        })
        .take(if max_files == 0 {
            usize::MAX
        } else {
            max_files
        })
}

pub struct ReindexInput {
    pub state: State,
    pub removed: Vec<String>,
}

/// The graph so far plus the items still moving through the per-item
/// phases; `dirty` is every file the resolver must revisit.
pub struct Workset<C> {
    pub state: State,
    pub items: C,
    pub dirty: FxHashSet<u32>,
}

pub struct SourcePaths {
    pub paths: Vec<String>,
}

/// The tree-sitter tree, source attached.
pub struct Parsed(pub Tree, pub Option<Killed>);

/// Rewrite rules applied; language nodes still present.
pub struct Rewritten(pub Tree, pub Option<Killed>);

/// Only canonical nodes remain; the source text is gone.
pub struct Canonical(pub Tree<Compact>, pub Option<Killed>);

pub struct LinkedFile {
    pub tree: Tree<Compact>,
    pub edges: Vec<Edge>,
    pub limited: Option<Killed>,
}

pub struct DirtyGraph {
    pub state: State,
    pub dirty: FxHashSet<u32>,
    /// The trees this run added: everything it read, parsed or not.
    pub inserted: std::ops::Range<u32>,
}

pub struct Resolved {
    pub state: State,
    pub resolved: Vec<u32>,
    /// Every file whose graph this run may have changed: the ones it added,
    /// resolved again, or gave other project tags. The others are as the
    /// snapshot had them.
    pub touched: FxHashSet<u32>,
}

pub struct Displayed {
    pub state: State,
}

pub struct Exported {
    pub state: State,
    pub tables: Vec<(String, RecordBatch)>,
}

impl super::Labelled for SourceFile {
    fn label(&self) -> &str {
        &self.path
    }
    fn bytes(&self) -> u64 {
        self.content.len() as u64
    }
}

impl super::Labelled for Parsed {
    fn label(&self) -> &str {
        &self.0.label
    }
}

impl super::Labelled for Rewritten {
    fn label(&self) -> &str {
        &self.0.label
    }
}

impl super::Labelled for Canonical {
    fn label(&self) -> &str {
        &self.0.label
    }
}

impl super::Labelled for LinkedFile {
    fn label(&self) -> &str {
        &self.tree.label
    }
}
