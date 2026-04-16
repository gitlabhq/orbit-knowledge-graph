use std::path::PathBuf;
use std::sync::Arc;

use tracing::info;

use super::cache::RepositoryCache;
use super::service::RepositoryService;
use crate::handler::HandlerError;
use crate::modules::code::metrics::CodeMetrics;

pub struct RepositoryResolver {
    repository_service: Arc<dyn RepositoryService>,
    cache: Arc<dyn RepositoryCache>,
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
    ) -> Result<PathBuf, HandlerError> {
        let ref_name = commit_sha.unwrap_or(branch);
        self.metrics.record_resolution_strategy("full_download");
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
    ) -> Result<PathBuf, HandlerError> {
        info!(
            project_id,
            branch, ref_name, "downloading repository archive"
        );

        let archive_stream = self
            .repository_service
            .download_archive(project_id, ref_name)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to download archive: {e}")))?;

        self.cache
            .extract_archive(project_id, branch, archive_stream)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to extract archive: {e}")))
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
        download_count: AtomicUsize,
    }

    impl ScriptedRepositoryService {
        fn with_archive(files: &[(&str, &str)], ref_name: &str) -> Arc<Self> {
            Arc::new(Self {
                archive: Mutex::new(build_test_tar_gz(files, ref_name)),
                fail_downloads: Mutex::new(false),
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
        let cache: Arc<dyn RepositoryCache> =
            Arc::new(LocalRepositoryCache::new(temp_dir.path().to_path_buf()));
        let resolver = RepositoryResolver::new(
            service as Arc<dyn RepositoryService>,
            cache,
            CodeMetrics::default(),
        );
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

        assert!(result.is_err());
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
