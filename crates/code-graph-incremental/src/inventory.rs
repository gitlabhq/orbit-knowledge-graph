//! Which files of a repository this crate sees, decided the way production
//! decides: one `CodeFilter` per run says per file whether it parses, loads
//! for resolvers, or is only recorded, and why.

use std::path::Path;
use std::sync::Arc;

use code_graph::v2::config::{CodeFilter, detect_language_from_path};
pub use code_graph::v2::error::{AbortPhase, FileFault, FileReason, FileSkip};
use orbit_utils::files::{Inventory, SourceError, Vfs, disk};

const MAX_FILE_BYTES: u64 = 5 * 1024 * 1024;

pub fn code_filter() -> CodeFilter {
    CodeFilter::new(Some(MAX_FILE_BYTES), None, detect_language_from_path)
}

/// Every file of a repository on disk, honouring `.gitignore`, and the
/// filesystem its loadable files are reachable through.
pub fn walk(root: &Path) -> Result<(Arc<Vfs>, Inventory), SourceError> {
    let repo = Vfs::default();
    let inventory = disk::discover(root, &code_filter(), &repo)?;
    Ok((Arc::new(repo), inventory))
}

/// The named files under `root`; for a change set, where a walk is not wanted.
pub fn classify(root: &Path, paths: Vec<String>) -> Result<(Arc<Vfs>, Inventory), SourceError> {
    let repo = Vfs::default();
    let inventory = disk::discover_paths(root, paths, &code_filter(), &repo)?;
    Ok((Arc::new(repo), inventory))
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
