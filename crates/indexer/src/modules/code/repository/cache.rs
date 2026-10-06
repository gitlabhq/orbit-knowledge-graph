use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use code_graph::v2::config::{CodeFilter, Role, detect_language_from_path};
use futures::StreamExt;
use orbit_utils::vfs::{Decision, Limits, Options, SourceError, Vfs, sources::Archive};

use tokio_util::io::{StreamReader, SyncIoBridge};

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

#[derive(Debug)]
pub struct CachedRepository {
    pub files: Arc<Vfs<Role>>,
}

#[async_trait]
pub trait RepositoryCache: Send + Sync {
    async fn extract_archive(
        &self,
        archive_stream: ByteStream,
    ) -> Result<CachedRepository, RepositoryCacheError>;
}

const CACHE_DIR_NAME: &str = "gkg-repository-cache";

struct CancelLoad(Arc<AtomicBool>);

impl Drop for CancelLoad {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

pub struct LocalRepositoryCache {
    base_dir: PathBuf,
    max_file_size: u64,
    max_total_bytes: u64,
    resident_bytes: u64,
    compress_spill: bool,
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
            resident_bytes: 0,
            compress_spill: false,
            metrics,
        }
    }

    pub fn default_dir() -> PathBuf {
        std::env::temp_dir().join(CACHE_DIR_NAME)
    }

    pub fn with_storage(mut self, resident_bytes: u64, compress_spill: bool) -> Self {
        self.resident_bytes = resident_bytes;
        self.compress_spill = compress_spill;
        self
    }
}

#[async_trait]
impl RepositoryCache for LocalRepositoryCache {
    async fn extract_archive(
        &self,
        archive_stream: ByteStream,
    ) -> Result<CachedRepository, RepositoryCacheError> {
        tokio::fs::create_dir_all(&self.base_dir).await?;
        let reader = StreamReader::new(archive_stream.map(|r| r.map_err(std::io::Error::other)));
        let handle = tokio::runtime::Handle::current();
        let to_cap = |v: u64| if v == 0 { None } else { Some(v) };
        let filter = CodeFilter::new(detect_language_from_path);
        let limits = Limits {
            file_bytes: to_cap(self.max_file_size),
            total_bytes: to_cap(self.max_total_bytes),
            resident_bytes: Some(self.resident_bytes),
            ..Limits::default()
        };
        let cancel = CancelLoad(Arc::new(AtomicBool::new(false)));
        let cancelled = cancel.0.clone();
        let options = Options {
            scratch_dir: Some(self.base_dir.clone()),
            compress_spill: self.compress_spill,
            cancelled: Some(Box::new(move || cancelled.load(Ordering::Relaxed))),
        };
        let extracted = tokio::task::spawn_blocking(move || {
            let bridge = SyncIoBridge::new_with_handle(reader, handle);
            Vfs::load(Archive(bridge), filter, limits, options)
        })
        .await
        .map_err(|e| RepositoryCacheError::Archive(format!("task join error: {e}")))?;

        let files = match extracted {
            Ok(files) => files,
            Err(e) => {
                return Err(match e {
                    SourceError::Empty => RepositoryCacheError::EmptyArchive,
                    SourceError::Cap(_) => RepositoryCacheError::RepositoryTooLarge,
                    SourceError::Io(io) => RepositoryCacheError::Archive(io.to_string()),
                    SourceError::Cancelled => {
                        RepositoryCacheError::Archive("load cancelled".into())
                    }
                });
            }
        };

        for file in files.files() {
            if let Decision::List(reason) | Decision::Drop(reason) = file.decision() {
                self.metrics
                    .record_archive_entry_skipped(reason, 1, file.size);
            }
        }

        Ok(CachedRepository {
            files: Arc::new(files),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::code::repository::service::test_utils::build_tar_gz;
    use code_graph::v2::config::SkipReason;
    use std::path::Path;
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

    #[tokio::test]
    async fn storage_options_preserve_archive_contents() {
        let dir = TempDir::new().unwrap();
        let body = b"pub fn source() {}\n".repeat(100);
        for resident in [0, 4096] {
            for compress in [false, true] {
                let cache = LocalRepositoryCache::new(
                    dir.path().into(),
                    u64::MAX,
                    0,
                    CodeMetrics::default(),
                )
                .with_storage(resident, compress);
                let repo = cache
                    .extract_archive(archive_stream(build_tar_gz(&[("root/source.rs", &body)])))
                    .await
                    .unwrap();
                assert_eq!(
                    &*repo.files.read(Path::new("source.rs")).unwrap(),
                    body.as_slice()
                );
                let usage = repo.files.usage();
                if resident == 0 {
                    assert_eq!(usage.resident, 0);
                    assert!(usage.spilled > 0);
                    if compress {
                        assert!(usage.spilled < body.len() as u64);
                    }
                } else {
                    assert_eq!(usage.resident, body.len() as u64);
                    assert_eq!(usage.spilled, 0);
                }
            }
        }
    }

    #[tokio::test]
    async fn extract_archive_loads_files() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[
            ("project-abc123/src/main.rs", b"fn main() {}"),
            ("project-abc123/src/lib.rs", b"pub mod lib;"),
        ]);

        let path = cache
            .extract_archive(archive_stream(archive))
            .await
            .unwrap();

        assert_eq!(
            &*path.files.read(Path::new("src/main.rs")).unwrap(),
            b"fn main() {}"
        );
        assert_eq!(
            &*path.files.read(Path::new("src/lib.rs")).unwrap(),
            b"pub mod lib;"
        );
    }

    #[tokio::test]
    async fn concurrent_extractions_of_one_repo_get_isolated_stores() {
        let (_dir, cache) = create_cache();
        let first_archive = build_tar_gz(&[("project-commit1/old_file.rs", b"old content")]);
        let first = cache
            .extract_archive(archive_stream(first_archive))
            .await
            .unwrap();

        let second_archive = build_tar_gz(&[("project-commit2/new_file.rs", b"new content")]);
        let second = cache
            .extract_archive(archive_stream(second_archive))
            .await
            .unwrap();

        assert!(!Arc::ptr_eq(&first.files, &second.files));
        assert!(first.files.read(Path::new("old_file.rs")).is_ok());
        assert!(first.files.stat(Path::new("new_file.rs")).is_err());
        assert!(second.files.read(Path::new("new_file.rs")).is_ok());
        assert!(second.files.stat(Path::new("old_file.rs")).is_err());
    }

    #[tokio::test]
    async fn loading_repositories_does_not_create_named_files() {
        let (dir, cache) = create_cache();
        let archive = build_tar_gz(&[("root/file.rs", b"content")]);

        let path_1 = cache
            .extract_archive(archive_stream(archive.clone()))
            .await
            .unwrap();
        let path_2 = cache
            .extract_archive(archive_stream(archive))
            .await
            .unwrap();
        assert!(path_1.files.read(Path::new("file.rs")).is_ok());
        assert!(path_2.files.read(Path::new("file.rs")).is_ok());
        let mut entries = tokio::fs::read_dir(dir.path()).await.unwrap();
        assert!(
            entries.next_entry().await.unwrap().is_none(),
            "scratch base dir must be empty after purge"
        );
    }

    #[tokio::test]
    async fn loading_creates_a_missing_scratch_directory() {
        let temp_dir = TempDir::new().unwrap();
        let base = temp_dir.path().join("not-yet-created");
        let cache = LocalRepositoryCache::new(base.clone(), u64::MAX, 0, CodeMetrics::default());

        let archive = build_tar_gz(&[("root/file.rs", b"content")]);
        let repo = cache
            .extract_archive(archive_stream(archive))
            .await
            .unwrap();

        assert!(base.exists());
        assert_eq!(&*repo.files.read(Path::new("file.rs")).unwrap(), b"content");
    }

    #[tokio::test]
    async fn cap_exceeded_leaves_no_partial_extraction_on_disk() {
        let (dir, cache) = create_cache_with_total_cap(8);
        // Two 6-byte files: the first fits, the second trips the 8-byte total cap mid-stream.
        let archive = build_tar_gz(&[
            ("repo-abc/first.rs", b"aaaaaa"),
            ("repo-abc/second.rs", b"bbbbbb"),
        ]);

        let result = cache.extract_archive(archive_stream(archive)).await;

        assert!(matches!(
            result,
            Err(RepositoryCacheError::RepositoryTooLarge)
        ));
        let mut entries = tokio::fs::read_dir(dir.path()).await.unwrap();
        assert!(
            entries.next_entry().await.unwrap().is_none(),
            "a too-large repo must not orphan its extraction dir on disk"
        );
    }

    #[tokio::test]
    async fn dropping_repository_releases_the_store_without_files_on_disk() {
        let (dir, cache) = create_cache();
        let archive = build_tar_gz(&[("root/file.rs", b"content")]);
        let repo = cache
            .extract_archive(archive_stream(archive))
            .await
            .unwrap();
        let files = Arc::downgrade(&repo.files);
        assert!(repo.files.read(Path::new("file.rs")).is_ok());

        drop(repo);

        assert!(files.upgrade().is_none());
        let mut entries = tokio::fs::read_dir(dir.path()).await.unwrap();
        assert!(entries.next_entry().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn concurrent_repositories_are_independent() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[("root/file.rs", b"content")]);

        let path_1 = cache
            .extract_archive(archive_stream(archive.clone()))
            .await
            .unwrap();
        let path_2 = cache
            .extract_archive(archive_stream(archive))
            .await
            .unwrap();

        assert!(!Arc::ptr_eq(&path_1.files, &path_2.files));
        assert!(path_1.files.read(Path::new("file.rs")).is_ok());
        assert!(path_2.files.read(Path::new("file.rs")).is_ok());

        drop(path_1);
        assert!(
            path_2.files.read(Path::new("file.rs")).is_ok(),
            "dropping one must not touch the other"
        );
    }

    #[tokio::test]
    async fn extract_archive_reports_empty_archive_for_empty_body() {
        let (_dir, cache) = create_cache();

        let err = cache
            .extract_archive(archive_stream(Vec::new()))
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
            .extract_archive(archive_stream(truncated))
            .await
            .unwrap_err();

        assert!(
            matches!(err, RepositoryCacheError::EmptyArchive),
            "expected EmptyArchive, got {err:?}"
        );
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
            .extract_archive(archive_stream(archive))
            .await
            .unwrap();
        let inventory_paths: Vec<_> = path
            .files
            .files()
            .map(|entry| entry.path.as_ref())
            .collect();
        assert!(
            inventory_paths.contains(&"assets/logo.png"),
            "filtered files should still be present in archive inventory"
        );
        assert!(
            inventory_paths.contains(&"README.md"),
            "retained non-parsable files should be present in archive inventory"
        );

        for name in [
            "src/main.rs",
            "Cargo.toml",
            "Cargo.lock",
            "package.json",
            "tsconfig.json",
            ".gitignore",
            "README.md",
        ] {
            assert!(path.files.read(Path::new(name)).is_ok(), "{name}");
        }
        for name in [
            "assets/logo.png",
            "static/banner.gif",
            "fonts/Inter.woff2",
            "dist/build.zip",
        ] {
            assert_eq!(
                path.files.read(Path::new(name)).unwrap_err().kind(),
                std::io::ErrorKind::Unsupported
            );
        }
    }

    #[tokio::test]
    async fn extract_archive_skips_files_above_max_size() {
        let (_dir, cache) = create_cache_with_size(64);
        let archive = build_tar_gz(&[
            ("project-abc/small.rs", b"fn s() {}"),
            ("project-abc/big.rs", &vec![b'x'; 4096][..]),
        ]);

        let path = cache
            .extract_archive(archive_stream(archive))
            .await
            .unwrap();

        assert!(
            path.files.files().any(|entry| entry.path == "big.rs"),
            "oversize files should still be present in archive inventory"
        );
        assert!(path.files.read(Path::new("small.rs")).is_ok());
        assert!(
            path.files.read(Path::new("big.rs")).is_err(),
            "files larger than max_file_size must not be written to disk"
        );
    }

    #[tokio::test]
    async fn extract_archive_records_lfs_pointers_without_writing_them() {
        let (_dir, cache) = create_cache();
        let pointer = b"version https://git-lfs.github.com/spec/v1\n\
            oid sha256:4d7a214614ab2935c943f9e0ff69d22eadbb8f32b1258daaa5e2ca24d17e2393\n\
            size 5242880\n";
        let archive = build_tar_gz(&[
            ("project-abc/src/main.rs", b"fn main() {}"),
            ("project-abc/data/train.csv", pointer),
        ]);

        let path = cache
            .extract_archive(archive_stream(archive))
            .await
            .unwrap();

        assert!(
            path.files
                .files()
                .any(|entry| entry.path == "data/train.csv"),
            "LFS pointers should still be present in archive inventory"
        );
        assert_eq!(
            path.files
                .stat(Path::new("data/train.csv"))
                .unwrap()
                .decision,
            Some(Decision::List(SkipReason::LfsPointer.into()))
        );
        assert!(path.files.read(Path::new("src/main.rs")).is_ok());
        assert!(path.files.read(Path::new("data/train.csv")).is_err());
    }

    #[tokio::test]
    async fn extract_archive_drops_binary_content_under_size_cap() {
        let (_dir, cache) = create_cache();
        let archive = build_tar_gz(&[
            ("project-abc/src/main.rs", b"fn main() {}"),
            ("project-abc/model/weights.onnx", b"\x00\x01\x02\x00blob"),
        ]);

        let path = cache
            .extract_archive(archive_stream(archive))
            .await
            .unwrap();

        assert!(
            path.files
                .files()
                .any(|entry| entry.path == "model/weights.onnx"),
            "binary files should still be present in archive inventory"
        );
        assert!(path.files.read(Path::new("src/main.rs")).is_ok());
        assert!(
            path.files.read(Path::new("model/weights.onnx")).is_err(),
            "binary content must not be written to disk"
        );
    }
}
