use std::path::{Path, PathBuf};
use std::sync::Arc;

use code_graph::v2::config::is_excluded_from_indexing;
use futures::StreamExt;
use tempfile::TempDir;
use tokio_util::io::{StreamReader, SyncIoBridge};
use tracing::{info, warn};

use super::service::{RepositoryService, RepositoryServiceError};
use crate::handler::HandlerError;
use crate::modules::code::archive::{ArchiveError, extract_tar_gz_from_reader};
use crate::modules::code::metrics::CodeMetrics;
use gitlab_client::GitlabClientError;

const TEMP_DIR_PREFIX: &str = "gkg-repo-";

/// RAII guard owning the temp directory with the extracted archive.
/// Directory is deleted when the guard drops.
#[derive(Debug)]
pub struct RepoDir {
    path: PathBuf,
    _temp_dir: TempDir,
}

impl RepoDir {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveError {
    #[error("project has no repository content ({reason}): {detail}")]
    EmptyRepository {
        reason: EmptyRepositoryReason,
        detail: String,
    },

    #[error(transparent)]
    Other(#[from] HandlerError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmptyRepositoryReason {
    NotFound,
    ServerError,
    EmptyArchive,
}

impl EmptyRepositoryReason {
    pub fn as_metric_label(self) -> &'static str {
        match self {
            Self::NotFound => "not_found",
            Self::ServerError => "server_error",
            Self::EmptyArchive => "empty_archive",
        }
    }
}

impl std::fmt::Display for EmptyRepositoryReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_metric_label())
    }
}

fn classify_download_error(
    err: &RepositoryServiceError,
) -> Option<(EmptyRepositoryReason, String)> {
    match err {
        RepositoryServiceError::GitlabApi(GitlabClientError::NotFound(id)) => Some((
            EmptyRepositoryReason::NotFound,
            format!("project {id} not found (404)"),
        )),
        RepositoryServiceError::GitlabApi(GitlabClientError::ServerError {
            project_id,
            status,
        }) => Some((
            EmptyRepositoryReason::ServerError,
            format!("archive endpoint returned {status} for project {project_id} (repository likely missing)"),
        )),
        _ => None,
    }
}

pub struct RepositoryResolver {
    repository_service: Arc<dyn RepositoryService>,
    max_file_size: u64,
    metrics: CodeMetrics,
}

impl RepositoryResolver {
    pub fn new(
        repository_service: Arc<dyn RepositoryService>,
        max_file_size: u64,
        metrics: CodeMetrics,
    ) -> Self {
        Self {
            repository_service,
            max_file_size,
            metrics,
        }
    }

    pub async fn resolve(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: Option<&str>,
    ) -> Result<RepoDir, ResolveError> {
        let ref_name = commit_sha.unwrap_or(branch);

        info!(project_id, branch, ref_name, "downloading repository archive");

        let archive_stream = match self
            .repository_service
            .download_archive(project_id, ref_name)
            .await
        {
            Ok(stream) => stream,
            Err(err) => {
                if let Some((reason, detail)) = classify_download_error(&err) {
                    return Err(ResolveError::EmptyRepository { reason, detail });
                }
                return Err(
                    HandlerError::Processing(format!("failed to download archive: {err}")).into(),
                );
            }
        };

        let temp_dir = TempDir::with_prefix(TEMP_DIR_PREFIX)
            .map_err(|e| HandlerError::Processing(format!("failed to create temp dir: {e}")))?;
        let repo_dir = temp_dir.path().to_path_buf();

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
                if is_excluded_from_indexing(rel_path) {
                    metrics.record_archive_entry_skipped("excluded_extension", size);
                    return false;
                }
                true
            })
        })
        .await
        .map_err(|e| HandlerError::Processing(format!("archive task join error: {e}")))?
        .map_err(|e| match e {
            ArchiveError::EmptyArchive => ResolveError::EmptyRepository {
                reason: EmptyRepositoryReason::EmptyArchive,
                detail: format!(
                    "archive contained no entries for project {project_id} ref {ref_name}"
                ),
            },
            other => HandlerError::Processing(format!("failed to extract archive: {other}")).into(),
        })?;

        Ok(RepoDir {
            path: repo_dir,
            _temp_dir: temp_dir,
        })
    }
}

/// Clean up stale temp dirs from previous crashed runs. Call at indexer startup.
pub async fn cleanup_stale_temp_dirs() {
    let tmp = std::env::temp_dir();
    let mut entries = match tokio::fs::read_dir(&tmp).await {
        Ok(e) => e,
        Err(_) => return,
    };
    let mut removed = 0u64;
    while let Ok(Some(entry)) = entries.next_entry().await {
        if let Some(name) = entry.file_name().to_str() {
            if name.starts_with(TEMP_DIR_PREFIX) {
                if let Err(e) = tokio::fs::remove_dir_all(entry.path()).await {
                    warn!(path = %entry.path().display(), %e, "failed to clean stale repo temp dir");
                } else {
                    removed += 1;
                }
            }
        }
    }
    if removed > 0 {
        info!(removed, "cleaned up stale repository temp directories");
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;
    use crate::modules::code::repository::service::RepositoryServiceError;
    use async_trait::async_trait;

    use std::sync::atomic::{AtomicUsize, Ordering};

    struct ScriptedRepositoryService {
        archive: parking_lot::Mutex<Vec<u8>>,
        fail_downloads: parking_lot::Mutex<bool>,
        download_error: parking_lot::Mutex<Option<RepositoryServiceError>>,
        download_count: AtomicUsize,
    }

    impl ScriptedRepositoryService {
        fn with_archive(files: &[(&str, &str)], ref_name: &str) -> Arc<Self> {
            Arc::new(Self {
                archive: parking_lot::Mutex::new(build_test_tar_gz(files, ref_name)),
                fail_downloads: parking_lot::Mutex::new(false),
                download_error: parking_lot::Mutex::new(None),
                download_count: AtomicUsize::new(0),
            })
        }

        fn with_raw_archive(bytes: Vec<u8>) -> Arc<Self> {
            Arc::new(Self {
                archive: parking_lot::Mutex::new(bytes),
                fail_downloads: parking_lot::Mutex::new(false),
                download_error: parking_lot::Mutex::new(None),
                download_count: AtomicUsize::new(0),
            })
        }

        fn with_download_error(error: RepositoryServiceError) -> Arc<Self> {
            Arc::new(Self {
                archive: parking_lot::Mutex::new(Vec::new()),
                fail_downloads: parking_lot::Mutex::new(false),
                download_error: parking_lot::Mutex::new(Some(error)),
                download_count: AtomicUsize::new(0),
            })
        }

        fn set_fail_downloads(&self, fail: bool) {
            *self.fail_downloads.lock() = fail;
        }

        fn download_count(&self) -> usize {
            self.download_count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl RepositoryService for ScriptedRepositoryService {
        async fn project_info(
            &self,
            project_id: i64,
        ) -> Result<gitlab_client::ProjectInfo, RepositoryServiceError> {
            Ok(gitlab_client::ProjectInfo {
                project_id,
                default_branch: "main".to_string(),
            })
        }

        async fn download_archive(
            &self,
            _project_id: i64,
            _ref_name: &str,
        ) -> Result<super::super::service::ByteStream, RepositoryServiceError> {
            self.download_count.fetch_add(1, Ordering::SeqCst);
            if let Some(err) = self.download_error.lock().take() {
                return Err(err);
            }
            if *self.fail_downloads.lock() {
                return Err(RepositoryServiceError::Archive(
                    "simulated download failure".to_string(),
                ));
            }
            let data = self.archive.lock().clone();
            Ok(Box::pin(futures::stream::once(async {
                Ok(bytes::Bytes::from(data))
            })))
        }
    }

    fn build_test_tar_gz(files: &[(&str, &str)], ref_name: &str) -> Vec<u8> {
        let mut tar_builder = tar::Builder::new(Vec::new());
        for (path, content) in files {
            let content_bytes = content.as_bytes();
            let mut header = tar::Header::new_gnu();
            let archive_path = format!("project-{ref_name}/{path}");
            header.set_path(&archive_path).unwrap();
            header.set_size(content_bytes.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            tar_builder.append(&header, content_bytes).unwrap();
        }
        let tar_bytes = tar_builder.into_inner().unwrap();
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&tar_bytes).unwrap();
        encoder.finish().unwrap()
    }

    fn create_resolver(service: Arc<ScriptedRepositoryService>) -> RepositoryResolver {
        RepositoryResolver::new(
            service as Arc<dyn RepositoryService>,
            u64::MAX,
            CodeMetrics::default(),
        )
    }

    #[tokio::test]
    async fn resolve_downloads_archive() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let resolver = create_resolver(service);

        let guard = resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        assert!(guard.path().join("src/main.rs").exists());
        let content = std::fs::read_to_string(guard.path().join("src/main.rs")).unwrap();
        assert_eq!(content, "fn main() {}");
    }

    #[tokio::test]
    async fn resolve_uses_branch_when_no_commit_sha() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "main");
        let resolver = create_resolver(service);

        let guard = resolver.resolve(1, "main", None).await.unwrap();
        assert!(guard.path().join("src/main.rs").exists());
    }

    #[tokio::test]
    async fn drop_guard_cleans_up() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let resolver = create_resolver(service);

        let guard = resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        let path = guard.path().to_path_buf();
        assert!(path.exists());

        drop(guard);
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn resolve_same_commit_downloads_every_time() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let resolver = create_resolver(Arc::clone(&service));

        resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        resolver.resolve(1, "main", Some("abc123")).await.unwrap();

        assert_eq!(service.download_count(), 3);
    }

    #[tokio::test]
    async fn resolve_propagates_download_error() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let resolver = create_resolver(Arc::clone(&service));

        service.set_fail_downloads(true);
        let result = resolver.resolve(1, "main", Some("abc123")).await;
        assert!(matches!(result, Err(ResolveError::Other(_))));
    }

    #[tokio::test]
    async fn resolve_maps_archive_404_to_empty_repository() {
        let service = ScriptedRepositoryService::with_download_error(
            RepositoryServiceError::GitlabApi(gitlab_client::GitlabClientError::NotFound(42)),
        );
        let resolver = create_resolver(service);

        let err = resolver.resolve(42, "main", None).await.unwrap_err();
        assert!(matches!(err, ResolveError::EmptyRepository { reason: EmptyRepositoryReason::NotFound, .. }));
    }

    #[tokio::test]
    async fn resolve_maps_archive_5xx_to_empty_repository() {
        let service = ScriptedRepositoryService::with_download_error(
            RepositoryServiceError::GitlabApi(gitlab_client::GitlabClientError::ServerError {
                project_id: 42,
                status: 500,
            }),
        );
        let resolver = create_resolver(service);

        let err = resolver.resolve(42, "main", None).await.unwrap_err();
        assert!(matches!(err, ResolveError::EmptyRepository { reason: EmptyRepositoryReason::ServerError, .. }));
    }

    #[tokio::test]
    async fn resolve_maps_empty_archive_body_to_empty_repository() {
        let service = ScriptedRepositoryService::with_raw_archive(Vec::new());
        let resolver = create_resolver(service);

        let err = resolver.resolve(42, "main", None).await.unwrap_err();
        assert!(matches!(err, ResolveError::EmptyRepository { reason: EmptyRepositoryReason::EmptyArchive, .. }));
    }

    #[tokio::test]
    async fn resolve_non_empty_errors_fall_through_to_other() {
        let service = ScriptedRepositoryService::with_download_error(
            RepositoryServiceError::GitlabApi(gitlab_client::GitlabClientError::Unauthorized),
        );
        let resolver = create_resolver(service);

        let err = resolver.resolve(42, "main", None).await.unwrap_err();
        assert!(matches!(err, ResolveError::Other(_)));
    }

    #[tokio::test]
    async fn multiple_projects_get_independent_dirs() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let resolver = create_resolver(Arc::clone(&service));

        let guard1 = resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        let guard2 = resolver.resolve(2, "main", Some("abc123")).await.unwrap();

        assert_ne!(guard1.path(), guard2.path());
        assert!(guard1.path().join("src/main.rs").exists());
        assert!(guard2.path().join("src/main.rs").exists());
    }
}
