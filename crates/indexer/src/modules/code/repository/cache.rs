use std::path::PathBuf;

use async_trait::async_trait;
use code_graph::v2::config::is_parsable;
use futures::StreamExt;
use sha2::{Digest, Sha256};
use tokio_util::io::{StreamReader, SyncIoBridge};

use super::service::ByteStream;
use crate::modules::code::archive::{ArchiveError, extract_tar_gz_from_reader};
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

#[async_trait]
pub trait RepositoryCache: Send + Sync {
    async fn extract_archive(
        &self,
        project_id: i64,
        branch: &str,
        archive_stream: ByteStream,
    ) -> Result<PathBuf, RepositoryCacheError>;

    async fn invalidate(&self, project_id: i64, branch: &str) -> Result<(), RepositoryCacheError>;
}

const CACHE_DIR_NAME: &str = "gkg-repository-cache";
const REPOSITORY_DIR: &str = "repository";

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

    fn repository_dir(&self, project_id: i64, branch: &str) -> PathBuf {
        self.branch_dir(project_id, branch).join(REPOSITORY_DIR)
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
    ) -> Result<PathBuf, RepositoryCacheError> {
        let repo_dir = self.repository_dir(project_id, branch);

        match tokio::fs::remove_dir_all(&repo_dir).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        tokio::fs::create_dir_all(&repo_dir).await?;

        let reader = StreamReader::new(archive_stream.map(|r| r.map_err(std::io::Error::other)));
        let handle = tokio::runtime::Handle::current();
        let repo_dir_owned = repo_dir.clone();
        let max_file_size = self.max_file_size;
        let metrics = self.metrics.clone();
        tokio::task::spawn_blocking(move || {
            let bridge = SyncIoBridge::new_with_handle(reader, handle);
            extract_tar_gz_from_reader(bridge, &repo_dir_owned, |rel_path, size| {
                if size > max_file_size {
                    metrics.record_archive_entry_skipped("oversize", size);
                    return false;
                }
                if !is_parsable(rel_path) {
                    metrics.record_archive_entry_skipped("non_parsable", size);
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

        Ok(repo_dir)
    }

    async fn invalidate(&self, project_id: i64, branch: &str) -> Result<(), RepositoryCacheError> {
        let branch_dir = self.branch_dir(project_id, branch);
        match tokio::fs::remove_dir_all(&branch_dir).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }

        let project_dir = self.base_dir.join(project_id.to_string());
        let _ = tokio::fs::remove_dir(&project_dir).await;

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
    async fn extract_archive_replaces_existing_files() {
        let (_dir, cache) = create_cache();
        let first_archive = build_tar_gz(&[("project-commit1/old_file.rs", b"old content")]);
        cache
            .extract_archive(42, "main", archive_stream(first_archive))
            .await
            .unwrap();

        let second_archive = build_tar_gz(&[("project-commit2/new_file.rs", b"new content")]);
        let path = cache
            .extract_archive(42, "main", archive_stream(second_archive))
            .await
            .unwrap();

        assert!(!path.join("old_file.rs").exists());
        let content = tokio::fs::read_to_string(path.join("new_file.rs"))
            .await
            .unwrap();
        assert_eq!(content, "new content");
    }

    #[tokio::test]
    async fn invalidate_removes_directory() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[("file.rs", b"content")]);
        let path = cache
            .extract_archive(42, "main", archive_stream(archive))
            .await
            .unwrap();
        assert!(path.exists());

        cache.invalidate(42, "main").await.unwrap();

        assert!(!path.exists());
    }

    #[tokio::test]
    async fn invalidate_succeeds_when_no_directory_exists() {
        let (_dir, cache) = create_cache();

        cache.invalidate(42, "main").await.unwrap();
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

        cache.invalidate(42, "main").await.unwrap();
        assert!(!path_main.exists());
        assert!(path_dev.exists());

        // Project directory still exists because "develop" branch is present
        let project_dir = dir.path().join("42");
        assert!(project_dir.exists());

        cache.invalidate(42, "develop").await.unwrap();
        assert!(!path_dev.exists());

        // Project directory removed after all branches invalidated
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
    async fn extract_archive_skips_non_parsable_files() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[
            ("project-abc/src/main.rs", b"fn main() {}"),
            ("project-abc/assets/logo.png", b"\x89PNG\r\n\x1a\nfake"),
            ("project-abc/Cargo.lock", b"# generated lockfile"),
            ("project-abc/README.md", b"# Title"),
        ]);

        let path = cache
            .extract_archive(7, "main", archive_stream(archive))
            .await
            .unwrap();

        assert!(path.join("src/main.rs").exists());
        assert!(!path.join("assets/logo.png").exists());
        assert!(!path.join("Cargo.lock").exists());
        assert!(!path.join("README.md").exists());
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

        assert!(path.join("small.rs").exists());
        assert!(
            !path.join("big.rs").exists(),
            "files larger than max_file_size must not be written to disk"
        );
    }

    #[tokio::test]
    async fn extract_archive_skips_excluded_suffixes() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[
            ("project-abc/src/app.js", b"console.log(1)"),
            ("project-abc/vendor/jquery.min.js", b"!function(){}"),
        ]);

        let path = cache
            .extract_archive(7, "main", archive_stream(archive))
            .await
            .unwrap();

        assert!(path.join("src/app.js").exists());
        assert!(
            !path.join("vendor/jquery.min.js").exists(),
            "*.min.js suffix must be filtered before extraction"
        );
    }
}
