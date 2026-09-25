//! The file classification production runs before indexing, for callers to
//! run the same way: a `CodeFilter` decides per file whether to parse it,
//! load it for resolvers, or only record it, and why. The pipeline consumes
//! the resulting inventory; it never classifies.

use std::path::Path;

use code_graph::v2::config::{CodeFilter, detect_language_from_path};
pub use code_graph::v2::error::{AbortPhase, FileFault, FileReason, FileSkip};
use orbit_utils::fs_walk::{
    Decision, FileInventory, FileInventoryEntry, FileStreamHooks, StreamError, step, walk_dir,
};

const MAX_FILE_BYTES: u64 = 5 * 1024 * 1024;

pub fn code_filter() -> CodeFilter {
    CodeFilter::new(Some(MAX_FILE_BYTES), None, detect_language_from_path)
}

/// Walk a repository on disk, honouring `.gitignore`, and classify every file.
pub fn walk(root: &Path) -> Result<FileInventory, StreamError> {
    walk_dir(root, &mut code_filter())
}

/// Classify the named files under `root`; for a change set, where a full
/// walk is not wanted.
pub fn classify(root: &Path, paths: impl IntoIterator<Item = String>) -> Vec<FileInventoryEntry> {
    let mut hooks = code_filter();
    let mut content = Vec::new();
    paths
        .into_iter()
        .map(|path| {
            let abs = root.join(&path);
            let mut meta = FileInventoryEntry {
                path,
                size: std::fs::metadata(&abs).map_or(0, |m| m.len()),
                decision: Decision::ListOnly,
                label: Default::default(),
            };
            let settled = step(&mut hooks, &meta, &mut content, |buf| {
                std::io::Read::read_to_end(&mut std::fs::File::open(&abs)?, buf).map(|_| ())
            });
            (meta.decision, meta.label) = settled.unwrap_or_else(|_| hooks.on_non_regular(&meta));
            meta
        })
        .collect()
}

/// The reason a file that overran a budget carries, in production's labels.
pub fn timeout(phase: &str) -> FileReason {
    FileReason::Skip(FileSkip::Timeout(match phase {
        "tree-sitter" => AbortPhase::Parse,
        "rewrite" => AbortPhase::Walk,
        "link" => AbortPhase::Ssa,
        _ => AbortPhase::Sentinel,
    }))
}
