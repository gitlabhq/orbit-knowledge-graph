use std::path::Path;

use sha2::{Digest, Sha256};

/// SHA-256 hash of the branch name, used as a filesystem-safe directory name.
pub fn hashed_branch_name(branch: &str) -> String {
    let hash = Sha256::digest(branch.as_bytes());
    format!("{:x}", hash)
}

/// Synchronous directory size calculation — must be called from `spawn_blocking`.
pub fn directory_size(path: &Path) -> u64 {
    walkdir::WalkDir::new(path)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.metadata().ok())
        .filter(|metadata| metadata.is_file())
        .map(|metadata| metadata.len())
        .sum()
}
