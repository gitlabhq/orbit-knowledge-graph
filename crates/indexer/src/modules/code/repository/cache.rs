use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use code_graph::v2::{FileInventoryEntry, config::is_excluded_from_indexing};
use futures::StreamExt;
use sha2::{Digest, Sha256};
use tokio_util::io::{StreamReader, SyncIoBridge};

use super::archive::{ArchiveError, extract_tar_gz_from_reader};
use super::service::ByteStream;
use crate::modules::code::metrics::CodeMetrics;

#[derive(Debug, thiserror::Error)]
pub enum RepositoryCacheError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("archive extraction failed: {0}")]
    Archive(String),

    /// Archive stream ended before any entry was extracted. Surfaced so the
    /// resolver can classify this as an empty-repository outcome instead of
    /// a retryable processing failure.
    #[error("archive contained no entries (empty or truncated stream)")]
    EmptyArchive,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedRepository {
    pub path: PathBuf,
    pub file_inventory: Arc<[FileInventoryEntry]>,
}

impl std::ops::Deref for CachedRepository {
    type Target = Path;

    fn deref(&self) -> &Self::Target {
        &self.path
    }
}

#[async_trait]
pub trait RepositoryCache: Send + Sync {
    async fn extract_archive(
        &self,
        project_id: i64,
        branch: &str,
        archive_stream: ByteStream,
    ) -> Result<CachedRepository, RepositoryCacheError>;

    /// Remove a single extracted working tree by its path (the path returned in
    /// `CachedRepository`). Scoped to that tree so it never destroys another
    /// in-flight task's extraction for the same project/branch.
    async fn remove_repository(&self, path: &Path) -> Result<(), RepositoryCacheError>;
}

const CACHE_DIR_NAME: &str = "gkg-repository-cache";
const REPOSITORY_DIR: &str = "repository";

/// Per-execution sequence so concurrent or NATS-redelivered tasks for the same
/// (project_id, branch) extract into distinct working trees.
static EXTRACT_SEQ: AtomicU64 = AtomicU64::new(0);

pub struct LocalRepositoryCache {
    base_dir: PathBuf,
    max_file_size: u64,
    metrics: CodeMetrics,
}

impl LocalRepositoryCache {
    pub fn new(base_dir: PathBuf, max_file_size: u64, metrics: CodeMetrics) -> Self {
        Self {
            base_dir,
            max_file_size,
            metrics,
        }
    }

    pub fn default_dir() -> PathBuf {
        std::env::temp_dir().join(CACHE_DIR_NAME)
    }

    fn branch_dir(&self, project_id: i64, branch: &str) -> PathBuf {
        self.base_dir
            .join(project_id.to_string())
            .join(hashed_branch_name(branch))
    }
}

fn hashed_branch_name(branch: &str) -> String {
    let hash = Sha256::digest(branch.as_bytes());
    format!("{:x}", hash)
}

#[async_trait]
impl RepositoryCache for LocalRepositoryCache {
    async fn extract_archive(
        &self,
        project_id: i64,
        branch: &str,
        archive_stream: ByteStream,
    ) -> Result<CachedRepository, RepositoryCacheError> {
        // Unique per execution. A single shared (project_id, branch) dir let a
        // NATS-redelivered or concurrent task `remove_dir_all` an in-flight
        // task's tree mid-pipeline, corrupting its inventory and tombstoning
        // live data. A distinct dir per extraction removes that race entirely.
        let seq = EXTRACT_SEQ.fetch_add(1, Ordering::Relaxed);
        let repo_dir = self
            .branch_dir(project_id, branch)
            .join(format!("{REPOSITORY_DIR}-{}-{seq}", std::process::id()));
        tokio::fs::create_dir_all(&repo_dir).await?;

        let reader = StreamReader::new(archive_stream.map(|r| r.map_err(std::io::Error::other)));
        let handle = tokio::runtime::Handle::current();
        let repo_dir_owned = repo_dir.clone();
        let max_file_size = self.max_file_size;
        let metrics = self.metrics.clone();
        let file_inventory = tokio::task::spawn_blocking(move || {
            let bridge = SyncIoBridge::new_with_handle(reader, handle);
            extract_tar_gz_from_reader(bridge, &repo_dir_owned, |rel_path, size| {
                if size > max_file_size {
                    metrics.record_archive_entry_skipped("oversize", size);
                    return false;
                }
                if is_excluded_from_indexing(rel_path) {
                    metrics.record_archive_entry_skipped("excluded_extension", size);
                    return false;
                }
                true
            })
        })
        .await
        .map_err(|e| RepositoryCacheError::Archive(format!("task join error: {e}")))?
        .map_err(|e| match e {
            ArchiveError::EmptyArchive => RepositoryCacheError::EmptyArchive,
            other => RepositoryCacheError::Archive(other.to_string()),
        })?;

        Ok(CachedRepository {
            path: repo_dir,
            file_inventory: Arc::from(file_inventory),
        })
    }

    async fn remove_repository(&self, path: &Path) -> Result<(), RepositoryCacheError> {
        match tokio::fs::remove_dir_all(path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }

        // Best-effort prune of the now-empty branch and project dirs. `remove_dir`
        // (not `_all`) only succeeds when empty, so a sibling task's in-flight
        // tree is left untouched.
        if let Some(branch_dir) = path.parent() {
            let _ = tokio::fs::remove_dir(branch_dir).await;
            if let Some(project_dir) = branch_dir.parent() {
                let _ = tokio::fs::remove_dir(project_dir).await;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn create_cache() -> (TempDir, LocalRepositoryCache) {
        create_cache_with_size(u64::MAX)
    }

    fn create_cache_with_size(max_file_size: u64) -> (TempDir, LocalRepositoryCache) {
        let temp_dir = TempDir::new().unwrap();
        let cache = LocalRepositoryCache::new(
            temp_dir.path().to_path_buf(),
            max_file_size,
            CodeMetrics::default(),
        );
        (temp_dir, cache)
    }

    fn archive_stream(data: Vec<u8>) -> ByteStream {
        Box::pin(futures::stream::once(async {
            Ok(bytes::Bytes::from(data))
        }))
    }

    fn build_tar_gz(files: &[(&str, &[u8])]) -> Vec<u8> {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write;

        let mut tar_builder = tar::Builder::new(Vec::new());
        for (path, content) in files {
            let mut header = tar::Header::new_gnu();
            header.set_path(path).unwrap();
            header.set_size(content.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar_builder.append(&header, &content[..]).unwrap();
        }
        let tar_bytes = tar_builder.into_inner().unwrap();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::fast());
        encoder.write_all(&tar_bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[tokio::test]
    async fn extract_archive_populates_directory() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[
            ("project-abc123/src/main.rs", b"fn main() {}"),
            ("project-abc123/src/lib.rs", b"pub mod lib;"),
        ]);

        let path = cache
            .extract_archive(42, "main", archive_stream(archive))
            .await
            .unwrap();

        let content = tokio::fs::read_to_string(path.join("src/main.rs"))
            .await
            .unwrap();
        assert_eq!(content, "fn main() {}");
        let content = tokio::fs::read_to_string(path.join("src/lib.rs"))
            .await
            .unwrap();
        assert_eq!(content, "pub mod lib;");
    }

    #[tokio::test]
    async fn extract_archive_uses_a_unique_dir_per_call() {
        let (_dir, cache) = create_cache();
        let first = build_tar_gz(&[("project-commit1/old_file.rs", b"old content")]);
        let path_a = cache
            .extract_archive(42, "main", archive_stream(first))
            .await
            .unwrap();

        let second = build_tar_gz(&[("project-commit2/new_file.rs", b"new content")]);
        let path_b = cache
            .extract_archive(42, "main", archive_stream(second))
            .await
            .unwrap();

        // Concurrent / NATS-redelivered tasks for the same (project, branch)
        // must not share a working tree, so a second extract never clobbers the
        // first mid-pipeline.
        assert_ne!(path_a.path, path_b.path);
        assert!(path_a.join("old_file.rs").exists());
        assert!(path_b.join("new_file.rs").exists());
        assert!(!path_b.join("old_file.rs").exists());
    }

    #[tokio::test]
    async fn remove_repository_removes_only_its_own_tree() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[("file.rs", b"content")]);
        let keep = cache
            .extract_archive(42, "main", archive_stream(archive.clone()))
            .await
            .unwrap();
        let drop = cache
            .extract_archive(42, "main", archive_stream(archive))
            .await
            .unwrap();
        assert!(keep.exists() && drop.exists());

        cache.remove_repository(&drop.path).await.unwrap();

        assert!(!drop.exists());
        // A sibling in-flight extraction for the same project/branch survives.
        assert!(keep.exists());
    }

    #[tokio::test]
    async fn remove_repository_succeeds_when_no_directory_exists() {
        let (dir, cache) = create_cache();

        cache
            .remove_repository(&dir.path().join("missing"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn separate_branches_are_independent() {
        let (dir, cache) = create_cache();
        let archive = build_tar_gz(&[("file.rs", b"content")]);

        let path_main = cache
            .extract_archive(42, "main", archive_stream(archive.clone()))
            .await
            .unwrap();
        let path_dev = cache
            .extract_archive(42, "develop", archive_stream(archive))
            .await
            .unwrap();

        assert!(path_main.exists());
        assert!(path_dev.exists());

        cache.remove_repository(&path_main.path).await.unwrap();
        assert!(!path_main.exists());
        assert!(path_dev.exists());

        // Project directory still exists because "develop" branch is present
        let project_dir = dir.path().join("42");
        assert!(project_dir.exists());

        cache.remove_repository(&path_dev.path).await.unwrap();
        assert!(!path_dev.exists());

        // Project directory pruned once its last branch tree is removed
        assert!(
            !project_dir.exists(),
            "project directory should be pruned when its last extraction is removed"
        );
    }

    #[tokio::test]
    async fn separate_projects_are_independent() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[("file.rs", b"content")]);

        let path_1 = cache
            .extract_archive(1, "main", archive_stream(archive.clone()))
            .await
            .unwrap();
        let path_2 = cache
            .extract_archive(2, "main", archive_stream(archive))
            .await
            .unwrap();

        assert!(path_1.exists());
        assert!(path_2.exists());
    }

    #[tokio::test]
    async fn branch_dir_hashes_branch_name() {
        let (dir, cache) = create_cache();

        let path = cache.branch_dir(42, "main");

        let expected_hash = hashed_branch_name("main");
        assert_eq!(path, dir.path().join(format!("42/{expected_hash}")));
    }

    #[tokio::test]
    async fn extract_archive_reports_empty_archive_for_empty_body() {
        let (_dir, cache) = create_cache();

        let err = cache
            .extract_archive(42, "main", archive_stream(Vec::new()))
            .await
            .unwrap_err();

        assert!(
            matches!(err, RepositoryCacheError::EmptyArchive),
            "expected EmptyArchive, got {err:?}"
        );
    }

    #[tokio::test]
    async fn extract_archive_reports_empty_archive_for_truncated_gzip() {
        // First 3 bytes of a gzip header only. GzDecoder fails mid-read with
        // an UnexpectedEof-shaped error, which we classify as EmptyArchive.
        let truncated: Vec<u8> = vec![0x1f, 0x8b, 0x08];

        let (_dir, cache) = create_cache();

        let err = cache
            .extract_archive(42, "main", archive_stream(truncated))
            .await
            .unwrap_err();

        assert!(
            matches!(err, RepositoryCacheError::EmptyArchive),
            "expected EmptyArchive, got {err:?}"
        );
    }

    #[tokio::test]
    async fn branch_dir_hashes_away_path_traversal_characters() {
        let (dir, cache) = create_cache();

        let safe_path = cache.branch_dir(42, "../../../tmp/evil");

        assert!(safe_path.starts_with(dir.path().join("42")));
        assert!(!safe_path.to_string_lossy().contains(".."));
    }

    #[tokio::test]
    async fn extract_archive_drops_excluded_extensions_and_keeps_resolver_inputs() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[
            // Source.
            ("project-abc/src/main.rs", b"fn main() {}"),
            // Excluded extensions: dropped at extraction.
            ("project-abc/assets/logo.png", b"\x89PNG\r\n\x1a\nfake"),
            ("project-abc/static/banner.gif", b"GIF89a"),
            ("project-abc/fonts/Inter.woff2", b""),
            ("project-abc/dist/build.zip", b"PK"),
            // Resolver inputs: must survive even though they aren't
            // parsable source. Inclusion filters historically dropped
            // these and silently broke cross-crate / cross-module
            // resolution.
            (
                "project-abc/Cargo.toml",
                b"[workspace]\nmembers = [\"src/foo\"]\n",
            ),
            ("project-abc/Cargo.lock", b"# generated"),
            ("project-abc/package.json", b"{}\n"),
            ("project-abc/tsconfig.json", b"{\"compilerOptions\":{}}\n"),
            ("project-abc/.gitignore", b"target/\n"),
            ("project-abc/README.md", b"# Title"),
        ]);

        let path = cache
            .extract_archive(7, "main", archive_stream(archive))
            .await
            .unwrap();
        let inventory_paths: Vec<_> = path
            .file_inventory
            .iter()
            .map(|entry| entry.path.as_str())
            .collect();
        assert!(
            inventory_paths.contains(&"assets/logo.png"),
            "filtered files should still be present in archive inventory"
        );
        assert!(
            inventory_paths.contains(&"README.md"),
            "retained non-parsable files should be present in archive inventory"
        );

        // Source kept.
        assert!(path.join("src/main.rs").exists());
        // Excluded extensions dropped.
        assert!(!path.join("assets/logo.png").exists());
        assert!(!path.join("static/banner.gif").exists());
        assert!(!path.join("fonts/Inter.woff2").exists());
        assert!(!path.join("dist/build.zip").exists());
        // Resolver inputs preserved.
        assert!(path.join("Cargo.toml").exists());
        assert!(path.join("Cargo.lock").exists());
        assert!(path.join("package.json").exists());
        assert!(path.join("tsconfig.json").exists());
        assert!(path.join(".gitignore").exists());
        // Anything outside the denylist passes through, even if the
        // parser will ignore it later.
        assert!(path.join("README.md").exists());
    }

    #[tokio::test]
    async fn extract_archive_skips_files_above_max_size() {
        let (_dir, cache) = create_cache_with_size(64);
        let archive = build_tar_gz(&[
            ("project-abc/small.rs", b"fn s() {}"),
            ("project-abc/big.rs", &vec![b'x'; 4096][..]),
        ]);

        let path = cache
            .extract_archive(7, "main", archive_stream(archive))
            .await
            .unwrap();

        assert!(
            path.file_inventory
                .iter()
                .any(|entry| entry.path == "big.rs"),
            "oversize files should still be present in archive inventory"
        );
        assert!(path.join("small.rs").exists());
        assert!(
            !path.join("big.rs").exists(),
            "files larger than max_file_size must not be written to disk"
        );
    }
}
