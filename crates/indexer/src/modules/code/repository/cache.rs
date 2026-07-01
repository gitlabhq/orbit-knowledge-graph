use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use code_graph::v2::FileInventoryEntry;
use code_graph::v2::config::{CodeFilter, FilterSkip, detect_language_from_path};
use futures::StreamExt;
use gkg_utils::archive::extract_tar_gz;
use gkg_utils::fs_stream::StreamError;
use rustc_hash::FxHashMap;
use sha2::{Digest, Sha256};
use tokio_util::io::{StreamReader, SyncIoBridge};
use tracing::warn;
use uuid::Uuid;

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

    /// The repository exceeded the total-bytes cap. Like `EmptyArchive`, the
    /// resolver treats it as an empty repo (checkpoint), not a retryable failure.
    #[error("repository exceeded the total-bytes cap")]
    RepositoryTooLarge,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedRepository {
    pub path: PathBuf,
    pub file_inventory: Arc<[FileInventoryEntry]>,
    /// Per-path reason for files the stream settled as bare nodes, carried to the
    /// pipeline so each File node's `gl_file.reason` reflects the stream skip.
    pub stream_reasons: FxHashMap<String, FilterSkip>,
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

    /// Remove one run's extraction tree, leaving any concurrent run's tree untouched.
    async fn invalidate(&self, path: &Path) -> Result<(), RepositoryCacheError>;
}

const CACHE_DIR_NAME: &str = "gkg-repository-cache";
const REPOSITORY_DIR: &str = "repository";

pub struct LocalRepositoryCache {
    base_dir: PathBuf,
    max_file_size: u64,
    max_total_bytes: u64,
    metrics: CodeMetrics,
}

impl LocalRepositoryCache {
    pub fn new(
        base_dir: PathBuf,
        max_file_size: u64,
        max_total_bytes: u64,
        metrics: CodeMetrics,
    ) -> Self {
        Self {
            base_dir,
            max_file_size,
            max_total_bytes,
            metrics,
        }
    }

    pub fn default_dir() -> PathBuf {
        std::env::temp_dir().join(CACHE_DIR_NAME)
    }

    pub async fn purge_all(&self) -> Result<(), RepositoryCacheError> {
        match tokio::fs::remove_dir_all(&self.base_dir).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        tokio::fs::create_dir_all(&self.base_dir).await?;
        Ok(())
    }

    fn branch_dir(&self, project_id: i64, branch: &str) -> PathBuf {
        self.base_dir
            .join(project_id.to_string())
            .join(hashed_branch_name(branch))
    }

    /// A unique per-run directory so two workers racing the same repo never share (and clobber) a tree.
    fn repository_dir(&self, project_id: i64, branch: &str) -> PathBuf {
        self.branch_dir(project_id, branch)
            .join(Uuid::new_v4().to_string())
            .join(REPOSITORY_DIR)
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
        let repo_dir = self.repository_dir(project_id, branch);

        tokio::fs::create_dir_all(&repo_dir).await?;

        let reader = StreamReader::new(archive_stream.map(|r| r.map_err(std::io::Error::other)));
        let handle = tokio::runtime::Handle::current();
        let repo_dir_owned = repo_dir.clone();
        let mut filter = CodeFilter::new(
            self.max_file_size,
            self.max_total_bytes,
            detect_language_from_path,
        );
        let extracted = tokio::task::spawn_blocking(move || {
            let bridge = SyncIoBridge::new_with_handle(reader, handle);
            extract_tar_gz(bridge, &repo_dir_owned, &mut filter).map(|inv| (inv, filter))
        })
        .await
        .map_err(|e| RepositoryCacheError::Archive(format!("task join error: {e}")))?;

        // Extraction streams files to disk as it goes, so a mid-stream failure (cap exceeded,
        // truncated body) leaves partial output. Remove it before returning so a too-large repo
        // does not orphan up to max_total_bytes on disk every time it is re-attempted.
        let (file_inventory, filter) = match extracted {
            Ok(ok) => ok,
            Err(e) => {
                let run_dir = repo_dir.parent().unwrap_or(&repo_dir);
                if let Err(cleanup) = tokio::fs::remove_dir_all(run_dir).await {
                    warn!(?run_dir, error = %cleanup, "failed to clean partial extraction after error");
                }
                return Err(match e {
                    StreamError::Empty => RepositoryCacheError::EmptyArchive,
                    StreamError::Cap(_) => RepositoryCacheError::RepositoryTooLarge,
                    StreamError::Io(io) => RepositoryCacheError::Archive(io.to_string()),
                });
            }
        };

        for (reason, tally) in filter.skips() {
            self.metrics
                .record_archive_entry_skipped(reason.into(), tally.count, tally.bytes);
        }

        Ok(CachedRepository {
            path: repo_dir,
            file_inventory: Arc::from(file_inventory),
            stream_reasons: filter.file_reasons().clone(),
        })
    }

    async fn invalidate(&self, path: &Path) -> Result<(), RepositoryCacheError> {
        // Remove the per-run `<run-uuid>` dir, then best-effort prune the empty branch/project dirs.
        let run_dir = path.parent().unwrap_or(path);
        match tokio::fs::remove_dir_all(run_dir).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        if let Some(branch_dir) = run_dir.parent() {
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
            0,
            CodeMetrics::default(),
        );
        (temp_dir, cache)
    }

    fn create_cache_with_total_cap(max_total_bytes: u64) -> (TempDir, LocalRepositoryCache) {
        let temp_dir = TempDir::new().unwrap();
        let cache = LocalRepositoryCache::new(
            temp_dir.path().to_path_buf(),
            u64::MAX,
            max_total_bytes,
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
    async fn concurrent_extractions_of_one_repo_get_isolated_dirs() {
        let (_dir, cache) = create_cache();
        let first_archive = build_tar_gz(&[("project-commit1/old_file.rs", b"old content")]);
        let first = cache
            .extract_archive(42, "main", archive_stream(first_archive))
            .await
            .unwrap();

        let second_archive = build_tar_gz(&[("project-commit2/new_file.rs", b"new content")]);
        let second = cache
            .extract_archive(42, "main", archive_stream(second_archive))
            .await
            .unwrap();

        assert_ne!(first.path, second.path);
        assert!(first.path.join("old_file.rs").exists());
        assert!(!first.path.join("new_file.rs").exists());
        assert!(second.path.join("new_file.rs").exists());
        assert!(!second.path.join("old_file.rs").exists());
    }

    #[tokio::test]
    async fn purge_all_clears_all_cached_repositories() {
        let (dir, cache) = create_cache();
        let archive = build_tar_gz(&[("file.rs", b"content")]);

        let path_1 = cache
            .extract_archive(1, "main", archive_stream(archive.clone()))
            .await
            .unwrap();
        let path_2 = cache
            .extract_archive(2, "develop", archive_stream(archive))
            .await
            .unwrap();
        assert!(path_1.exists());
        assert!(path_2.exists());

        cache.purge_all().await.unwrap();

        assert!(!path_1.exists());
        assert!(!path_2.exists());
        let mut entries = tokio::fs::read_dir(dir.path()).await.unwrap();
        assert!(
            entries.next_entry().await.unwrap().is_none(),
            "scratch base dir must be empty after purge"
        );
    }

    #[tokio::test]
    async fn purge_all_recreates_missing_base_dir() {
        let temp_dir = TempDir::new().unwrap();
        let base = temp_dir.path().join("not-yet-created");
        let cache = LocalRepositoryCache::new(base.clone(), u64::MAX, 0, CodeMetrics::default());

        cache.purge_all().await.unwrap();

        assert!(base.exists());
    }

    #[tokio::test]
    async fn cap_exceeded_leaves_no_partial_extraction_on_disk() {
        let (dir, cache) = create_cache_with_total_cap(8);
        // Two 6-byte files: the first fits, the second trips the 8-byte total cap mid-stream.
        let archive = build_tar_gz(&[
            ("repo-abc/first.rs", b"aaaaaa"),
            ("repo-abc/second.rs", b"bbbbbb"),
        ]);

        let result = cache
            .extract_archive(42, "main", archive_stream(archive))
            .await;

        assert!(matches!(
            result,
            Err(RepositoryCacheError::RepositoryTooLarge)
        ));
        let branch_dir = dir.path().join("42").join(hashed_branch_name("main"));
        let orphaned = match tokio::fs::read_dir(&branch_dir).await {
            Ok(mut entries) => entries.next_entry().await.unwrap().is_some(),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
            Err(e) => panic!("{e}"),
        };
        assert!(
            !orphaned,
            "a too-large repo must not orphan its extraction dir on disk"
        );
    }

    #[tokio::test]
    async fn invalidate_removes_directory() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[("file.rs", b"content")]);
        let repo = cache
            .extract_archive(42, "main", archive_stream(archive))
            .await
            .unwrap();
        assert!(repo.path.exists());

        cache.invalidate(&repo.path).await.unwrap();

        assert!(!repo.path.exists());
    }

    #[tokio::test]
    async fn invalidate_succeeds_when_no_directory_exists() {
        let (dir, cache) = create_cache();

        cache
            .invalidate(&dir.path().join("42/branch/run/repository"))
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

        cache.invalidate(&path_main.path).await.unwrap();
        assert!(!path_main.exists());
        assert!(path_dev.exists());

        let project_dir = dir.path().join("42");
        assert!(project_dir.exists());

        cache.invalidate(&path_dev.path).await.unwrap();
        assert!(!path_dev.exists());

        assert!(
            !project_dir.exists(),
            "project directory should be removed when all branches are invalidated"
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
            ("project-abc/src/main.rs", b"fn main() {}"),
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

        assert!(path.join("src/main.rs").exists());
        assert!(!path.join("assets/logo.png").exists());
        assert!(!path.join("static/banner.gif").exists());
        assert!(!path.join("fonts/Inter.woff2").exists());
        assert!(!path.join("dist/build.zip").exists());
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

    #[tokio::test]
    async fn extract_archive_drops_binary_content_under_size_cap() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[
            ("project-abc/src/main.rs", b"fn main() {}"),
            ("project-abc/model/weights.onnx", b"\x00\x01\x02\x00blob"),
        ]);

        let path = cache
            .extract_archive(7, "main", archive_stream(archive))
            .await
            .unwrap();

        assert!(
            path.file_inventory
                .iter()
                .any(|entry| entry.path == "model/weights.onnx"),
            "binary files should still be present in archive inventory"
        );
        assert!(path.join("src/main.rs").exists());
        assert!(
            !path.join("model/weights.onnx").exists(),
            "binary content must not be written to disk"
        );
    }
}
