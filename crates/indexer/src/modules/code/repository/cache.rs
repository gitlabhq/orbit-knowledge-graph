use std::sync::Arc;

use async_trait::async_trait;
use code_graph::v2::config::{CodeFilter, detect_language_from_path};
use futures::StreamExt;
use orbit_utils::files::{SourceError, Vfs, tar};
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

/// A repository's files: a node for everything the archive held, bytes in
/// memory up to the budget and spilled past it for what loads.
#[derive(Debug)]
pub struct CachedRepository {
    pub files: Arc<Vfs>,
}

#[async_trait]
pub trait RepositoryCache: Send + Sync {
    async fn extract_archive(
        &self,
        archive_stream: ByteStream,
    ) -> Result<CachedRepository, RepositoryCacheError>;
}

pub struct LocalRepositoryCache {
    max_file_size: u64,
    max_total_bytes: u64,
    memory_budget: u64,
    metrics: CodeMetrics,
}

impl LocalRepositoryCache {
    pub fn new(
        max_file_size: u64,
        max_total_bytes: u64,
        memory_budget: u64,
        metrics: CodeMetrics,
    ) -> Self {
        Self {
            max_file_size,
            max_total_bytes,
            memory_budget,
            metrics,
        }
    }
}

#[async_trait]
impl RepositoryCache for LocalRepositoryCache {
    async fn extract_archive(
        &self,
        archive_stream: ByteStream,
    ) -> Result<CachedRepository, RepositoryCacheError> {
        let reader = StreamReader::new(archive_stream.map(|r| r.map_err(std::io::Error::other)));
        let handle = tokio::runtime::Handle::current();
        let to_cap = |v: u64| if v == 0 { None } else { Some(v) };
        let filter = CodeFilter::new(
            to_cap(self.max_file_size),
            to_cap(self.max_total_bytes),
            detect_language_from_path,
        );
        let filter = Arc::new(filter);
        let files = Arc::new(Vfs::new(filter.clone(), Some(self.memory_budget)));
        let extracted = tokio::task::spawn_blocking({
            let files = files.clone();
            move || {
                let bridge = SyncIoBridge::new_with_handle(reader, handle);
                tar::extract(bridge, &files)
            }
        })
        .await
        .map_err(|e| RepositoryCacheError::Archive(format!("task join error: {e}")))?;

        extracted.map_err(|e| match e {
            SourceError::Empty => RepositoryCacheError::EmptyArchive,
            SourceError::Cap(_) => RepositoryCacheError::RepositoryTooLarge,
            SourceError::Io(io) => RepositoryCacheError::Archive(io.to_string()),
        })?;

        for (reason, tally) in filter.skips() {
            self.metrics
                .record_archive_entry_skipped(reason.into(), tally.count, tally.bytes);
        }

        Ok(CachedRepository { files })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::modules::code::repository::service::test_utils::build_tar_gz;
    use code_graph::v2::config::SkipReason;
    use std::path::Path;

    fn create_cache() -> LocalRepositoryCache {
        create_cache_with_size(u64::MAX)
    }

    fn create_cache_with_size(max_file_size: u64) -> LocalRepositoryCache {
        LocalRepositoryCache::new(max_file_size, 0, u64::MAX, CodeMetrics::default())
    }

    fn create_cache_with_total_cap(max_total_bytes: u64) -> LocalRepositoryCache {
        LocalRepositoryCache::new(u64::MAX, max_total_bytes, u64::MAX, CodeMetrics::default())
    }

    fn archive_stream(data: Vec<u8>) -> ByteStream {
        Box::pin(futures::stream::once(async {
            Ok(bytes::Bytes::from(data))
        }))
    }

    #[tokio::test]
    async fn extract_archive_populates_the_repository_filesystem() {
        let cache = create_cache();
        let archive = build_tar_gz(&[
            ("project-abc123/src/main.rs", b"fn main() {}"),
            ("project-abc123/src/lib.rs", b"pub mod lib;"),
        ]);

        let path = cache
            .extract_archive(archive_stream(archive))
            .await
            .unwrap();

        let content = path.files.read_to_string(Path::new("src/main.rs")).unwrap();
        assert_eq!(content, "fn main() {}");
        let content = path.files.read_to_string(Path::new("src/lib.rs")).unwrap();
        assert_eq!(content, "pub mod lib;");
    }

    #[tokio::test]
    async fn a_repository_over_the_total_cap_is_too_large() {
        let cache = create_cache_with_total_cap(8);
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
    }

    #[tokio::test]
    async fn concurrent_repositories_are_independent() {
        let cache = create_cache();
        let first = build_tar_gz(&[("project-commit1/old_file.rs", b"old content")]);
        let second = build_tar_gz(&[("project-commit2/new_file.rs", b"new content")]);

        let first = cache.extract_archive(archive_stream(first)).await.unwrap();
        let second = cache.extract_archive(archive_stream(second)).await.unwrap();

        assert!(first.files.exists(Path::new("old_file.rs")));
        assert!(!first.files.exists(Path::new("new_file.rs")));
        assert!(second.files.exists(Path::new("new_file.rs")));
        assert!(!second.files.exists(Path::new("old_file.rs")));
    }

    #[tokio::test]
    async fn extract_archive_reports_empty_archive_for_empty_body() {
        let cache = create_cache();

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

        let cache = create_cache();

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
        let cache = create_cache();
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
            .into_iter()
            .map(|entry| entry.path)
            .collect();
        assert!(
            inventory_paths.iter().any(|p| p == "assets/logo.png"),
            "filtered files should still be present in archive inventory"
        );
        assert!(
            inventory_paths.iter().any(|p| p == "README.md"),
            "retained non-parsable files should be present in archive inventory"
        );

        assert!(path.files.exists(Path::new("src/main.rs")));
        assert!(path.files.read(Path::new("assets/logo.png")).is_err());
        assert!(path.files.read(Path::new("static/banner.gif")).is_err());
        assert!(path.files.read(Path::new("fonts/Inter.woff2")).is_err());
        assert!(path.files.read(Path::new("dist/build.zip")).is_err());
        assert!(path.files.exists(Path::new("Cargo.toml")));
        assert!(path.files.exists(Path::new("Cargo.lock")));
        assert!(path.files.exists(Path::new("package.json")));
        assert!(path.files.exists(Path::new("tsconfig.json")));
        assert!(path.files.exists(Path::new(".gitignore")));
        // Anything outside the denylist passes through, even if the
        // parser will ignore it later.
        assert!(path.files.exists(Path::new("README.md")));
    }

    #[tokio::test]
    async fn extract_archive_skips_files_above_max_size() {
        let cache = create_cache_with_size(64);
        let archive = build_tar_gz(&[
            ("project-abc/small.rs", b"fn s() {}"),
            ("project-abc/big.rs", &vec![b'x'; 4096][..]),
        ]);

        let path = cache
            .extract_archive(archive_stream(archive))
            .await
            .unwrap();

        assert!(
            path.files.exists(Path::new("big.rs")),
            "oversize files should still be present in archive inventory"
        );
        assert!(path.files.exists(Path::new("small.rs")));
        assert!(
            path.files.read(Path::new("big.rs")).is_err(),
            "files larger than max_file_size must not be written to disk"
        );
    }

    #[tokio::test]
    async fn extract_archive_records_lfs_pointers_without_writing_them() {
        let cache = create_cache();
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
            path.files.exists(Path::new("data/train.csv")),
            "LFS pointers should still be present in archive inventory"
        );
        assert_eq!(
            path.files.file(Path::new("data/train.csv")).unwrap().skip,
            Some(SkipReason::LfsPointer)
        );
        assert!(path.files.exists(Path::new("src/main.rs")));
        assert!(path.files.read(Path::new("data/train.csv")).is_err());
    }

    #[tokio::test]
    async fn extract_archive_drops_binary_content_under_size_cap() {
        let cache = create_cache();
        let archive = build_tar_gz(&[
            ("project-abc/src/main.rs", b"fn main() {}"),
            ("project-abc/model/weights.onnx", b"\x00\x01\x02\x00blob"),
        ]);

        let path = cache
            .extract_archive(archive_stream(archive))
            .await
            .unwrap();

        assert!(
            path.files.exists(Path::new("model/weights.onnx")),
            "binary files should still be present in archive inventory"
        );
        assert!(path.files.exists(Path::new("src/main.rs")));
        assert!(
            path.files.read(Path::new("model/weights.onnx")).is_err(),
            "binary content must not be written to disk"
        );
    }
}
