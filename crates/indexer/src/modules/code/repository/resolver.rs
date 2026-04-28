use std::path::PathBuf;
use std::sync::Arc;

use tracing::info;

use super::cache::{RepositoryCache, RepositoryCacheError};
use super::service::{RepositoryService, RepositoryServiceError};
use crate::handler::HandlerError;
use crate::modules::code::metrics::CodeMetrics;
use gitlab_client::GitlabClientError;

/// Errors produced when resolving a repository snapshot for indexing.
///
/// `EmptyRepository` is a recognized terminal outcome: the project record
/// exists but has no Gitaly content (no refs, or no repository storage at
/// all). These should be checkpointed as "indexed empty" instead of retried.
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
    /// HTTP 200 OK with an empty or truncated archive body. Distinct from
    /// `NotFound` so dashboards can separate real 404s from quietly-empty 200s.
    EmptyArchive,
}

impl EmptyRepositoryReason {
    pub fn as_metric_label(self) -> &'static str {
        match self {
            EmptyRepositoryReason::NotFound => "not_found",
            EmptyRepositoryReason::ServerError => "server_error",
            EmptyRepositoryReason::EmptyArchive => "empty_archive",
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
            format!(
                "archive endpoint returned {status} for project {project_id} (repository likely missing)"
            ),
        )),
        _ => None,
    }
}

pub struct RepositoryResolver {
    repository_service: Arc<dyn RepositoryService>,
    cache: Arc<dyn RepositoryCache>,
    #[allow(dead_code)]
    metrics: CodeMetrics,
}

impl RepositoryResolver {
    pub fn new(
        repository_service: Arc<dyn RepositoryService>,
        cache: Arc<dyn RepositoryCache>,
        metrics: CodeMetrics,
    ) -> Self {
        Self {
            repository_service,
            cache,
            metrics,
        }
    }

    pub async fn resolve(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: Option<&str>,
    ) -> Result<PathBuf, ResolveError> {
        let ref_name = commit_sha.unwrap_or(branch);
        self.full_download(project_id, branch, ref_name).await
    }

    pub async fn cleanup(
        &self,
        project_id: i64,
        branch: &str,
    ) -> Result<(), super::cache::RepositoryCacheError> {
        self.cache.invalidate(project_id, branch).await
    }

    async fn full_download(
        &self,
        project_id: i64,
        branch: &str,
        ref_name: &str,
    ) -> Result<PathBuf, ResolveError> {
        info!(
            project_id,
            branch, ref_name, "downloading repository archive"
        );

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

        match self
            .cache
            .extract_archive(project_id, branch, archive_stream)
            .await
        {
            Ok(path) => Ok(path),
            Err(RepositoryCacheError::EmptyArchive) => Err(ResolveError::EmptyRepository {
                reason: EmptyRepositoryReason::EmptyArchive,
                detail: format!(
                    "archive contained no entries for project {project_id} ref {ref_name} (200 OK with empty body)"
                ),
            }),
            Err(e) => {
                Err(HandlerError::Processing(format!("failed to extract archive: {e}")).into())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use super::*;
    use crate::modules::code::repository::cache::{LocalRepositoryCache, RepositoryCache};
    use crate::modules::code::repository::service::RepositoryServiceError;
    use async_trait::async_trait;
    use parking_lot::Mutex;

    use std::sync::atomic::{AtomicUsize, Ordering};

    struct ScriptedRepositoryService {
        archive: Mutex<Vec<u8>>,
        fail_downloads: Mutex<bool>,
        download_error: Mutex<Option<RepositoryServiceError>>,
        download_count: AtomicUsize,
    }

    impl ScriptedRepositoryService {
        fn with_archive(files: &[(&str, &str)], ref_name: &str) -> Arc<Self> {
            Arc::new(Self {
                archive: Mutex::new(build_test_tar_gz(files, ref_name)),
                fail_downloads: Mutex::new(false),
                download_error: Mutex::new(None),
                download_count: AtomicUsize::new(0),
            })
        }

        fn with_raw_archive(bytes: Vec<u8>) -> Arc<Self> {
            Arc::new(Self {
                archive: Mutex::new(bytes),
                fail_downloads: Mutex::new(false),
                download_error: Mutex::new(None),
                download_count: AtomicUsize::new(0),
            })
        }

        fn with_download_error(error: RepositoryServiceError) -> Arc<Self> {
            Arc::new(Self {
                archive: Mutex::new(Vec::new()),
                fail_downloads: Mutex::new(false),
                download_error: Mutex::new(Some(error)),
                download_count: AtomicUsize::new(0),
            })
        }

        fn set_archive(&self, files: &[(&str, &str)], ref_name: &str) {
            *self.archive.lock() = build_test_tar_gz(files, ref_name);
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

    fn create_resolver(
        service: Arc<ScriptedRepositoryService>,
    ) -> (tempfile::TempDir, RepositoryResolver) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let metrics = CodeMetrics::default();
        let cache: Arc<dyn RepositoryCache> = Arc::new(LocalRepositoryCache::new(
            temp_dir.path().to_path_buf(),
            u64::MAX,
            metrics.clone(),
        ));
        let resolver =
            RepositoryResolver::new(service as Arc<dyn RepositoryService>, cache, metrics);
        (temp_dir, resolver)
    }

    #[tokio::test]
    async fn resolve_downloads_archive() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let (_dir, resolver) = create_resolver(service);

        let path = resolver.resolve(1, "main", Some("abc123")).await.unwrap();

        assert!(path.join("src/main.rs").exists());
        let content = std::fs::read_to_string(path.join("src/main.rs")).unwrap();
        assert_eq!(content, "fn main() {}");
    }

    #[tokio::test]
    async fn resolve_always_downloads_fresh_copy() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "commit1");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        let path1 = resolver.resolve(1, "main", Some("commit1")).await.unwrap();
        assert!(path1.join("src/main.rs").exists());

        service.set_archive(&[("src/new.rs", "fn new() {}")], "commit2");
        let path2 = resolver.resolve(1, "main", Some("commit2")).await.unwrap();

        assert!(path2.join("src/new.rs").exists());
        assert!(!path2.join("src/main.rs").exists());
    }

    #[tokio::test]
    async fn resolve_uses_branch_when_no_commit_sha() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "main");
        let (_dir, resolver) = create_resolver(service);

        let path = resolver.resolve(1, "main", None).await.unwrap();

        assert!(path.join("src/main.rs").exists());
    }

    #[tokio::test]
    async fn cleanup_removes_downloaded_files() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let (_dir, resolver) = create_resolver(service);

        let path = resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        assert!(path.exists());

        resolver.cleanup(1, "main").await.unwrap();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn resolve_same_commit_downloads_every_time() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        resolver.resolve(1, "main", Some("abc123")).await.unwrap();

        assert_eq!(service.download_count(), 3);
    }

    #[tokio::test]
    async fn cleanup_is_idempotent() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let (_dir, resolver) = create_resolver(service);

        resolver.resolve(1, "main", Some("abc123")).await.unwrap();

        resolver.cleanup(1, "main").await.unwrap();
        resolver.cleanup(1, "main").await.unwrap();
    }

    #[tokio::test]
    async fn resolve_works_after_cleanup() {
        let service = ScriptedRepositoryService::with_archive(&[("src/main.rs", "v1")], "commit1");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        let path1 = resolver.resolve(1, "main", Some("commit1")).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(path1.join("src/main.rs")).unwrap(),
            "v1"
        );

        resolver.cleanup(1, "main").await.unwrap();

        service.set_archive(&[("src/main.rs", "v2")], "commit2");
        let path2 = resolver.resolve(1, "main", Some("commit2")).await.unwrap();
        assert_eq!(
            std::fs::read_to_string(path2.join("src/main.rs")).unwrap(),
            "v2"
        );
    }

    #[tokio::test]
    async fn resolve_propagates_download_error() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        service.set_fail_downloads(true);
        let result = resolver.resolve(1, "main", Some("abc123")).await;

        assert!(matches!(result, Err(ResolveError::Other(_))));
    }

    #[tokio::test]
    async fn resolve_maps_archive_404_to_empty_repository() {
        let service = ScriptedRepositoryService::with_download_error(
            RepositoryServiceError::GitlabApi(gitlab_client::GitlabClientError::NotFound(42)),
        );
        let (_dir, resolver) = create_resolver(service);

        let err = resolver.resolve(42, "main", None).await.unwrap_err();

        match err {
            ResolveError::EmptyRepository { reason, detail } => {
                assert_eq!(reason, EmptyRepositoryReason::NotFound);
                assert!(detail.contains("not found"), "detail was {detail}");
            }
            other => panic!("expected EmptyRepository, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resolve_maps_archive_5xx_to_empty_repository() {
        let service = ScriptedRepositoryService::with_download_error(
            RepositoryServiceError::GitlabApi(gitlab_client::GitlabClientError::ServerError {
                project_id: 42,
                status: 500,
            }),
        );
        let (_dir, resolver) = create_resolver(service);

        let err = resolver.resolve(42, "main", None).await.unwrap_err();

        match err {
            ResolveError::EmptyRepository { reason, detail } => {
                assert_eq!(reason, EmptyRepositoryReason::ServerError);
                assert!(detail.contains("500"), "detail was {detail}");
            }
            other => panic!("expected EmptyRepository, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resolve_maps_empty_archive_body_to_empty_repository() {
        let service = ScriptedRepositoryService::with_raw_archive(Vec::new());
        let (_dir, resolver) = create_resolver(service);

        let err = resolver.resolve(42, "main", None).await.unwrap_err();

        match err {
            ResolveError::EmptyRepository { reason, detail } => {
                assert_eq!(reason, EmptyRepositoryReason::EmptyArchive);
                assert!(
                    detail.contains("no entries") || detail.contains("empty body"),
                    "detail was {detail}"
                );
            }
            other => panic!("expected EmptyRepository, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn resolve_non_empty_errors_fall_through_to_other() {
        let service = ScriptedRepositoryService::with_download_error(
            RepositoryServiceError::GitlabApi(gitlab_client::GitlabClientError::Unauthorized),
        );
        let (_dir, resolver) = create_resolver(service);

        let err = resolver.resolve(42, "main", None).await.unwrap_err();
        assert!(matches!(err, ResolveError::Other(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn cleanup_without_prior_download_does_not_error() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let (_dir, resolver) = create_resolver(service);

        resolver.cleanup(1, "main").await.unwrap();
    }

    #[tokio::test]
    async fn multiple_projects_are_independent() {
        let service =
            ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")], "abc123");
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        let path1 = resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        let path2 = resolver.resolve(2, "main", Some("abc123")).await.unwrap();

        assert_ne!(path1, path2);
        assert!(path1.join("src/main.rs").exists());
        assert!(path2.join("src/main.rs").exists());

        resolver.cleanup(1, "main").await.unwrap();
        assert!(!path1.exists());
        assert!(path2.exists());
    }
}
