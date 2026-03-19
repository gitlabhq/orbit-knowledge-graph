use std::path::PathBuf;
use std::sync::Arc;

use tracing::info;

use super::cache::RepositoryCache;
use super::service::RepositoryService;
use crate::handler::HandlerError;
use crate::modules::code::archive;

pub struct RepositoryResolver {
    repository_service: Arc<dyn RepositoryService>,
    cache: Arc<dyn RepositoryCache>,
}

impl RepositoryResolver {
    pub fn new(
        repository_service: Arc<dyn RepositoryService>,
        cache: Arc<dyn RepositoryCache>,
    ) -> Self {
        Self {
            repository_service,
            cache,
        }
    }

    pub fn repository_service(&self) -> &Arc<dyn RepositoryService> {
        &self.repository_service
    }

    pub async fn resolve(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: Option<&str>,
    ) -> Result<PathBuf, HandlerError> {
        let ref_name = commit_sha.unwrap_or(branch);

        let cached = self
            .cache
            .get(project_id, branch)
            .await
            .map_err(|e| HandlerError::Processing(format!("cache lookup failed: {e}")))?;

        let Some(cached) = cached else {
            return self.full_download(project_id, branch, ref_name).await;
        };

        if cached.commit == ref_name {
            info!(
                project_id,
                branch,
                commit = %ref_name,
                "using cached repository"
            );
            return Ok(cached.path);
        }

        self.cache
            .invalidate(project_id, branch)
            .await
            .map_err(|e| HandlerError::Processing(format!("cache invalidation failed: {e}")))?;
        self.full_download(project_id, branch, ref_name).await
    }

    async fn full_download(
        &self,
        project_id: i64,
        branch: &str,
        commit_sha: &str,
    ) -> Result<PathBuf, HandlerError> {
        info!(project_id, branch, commit = %commit_sha, "starting full repository download");
        let repo_path = self.cache.code_repository_path(project_id, branch);

        let archive_bytes = self
            .repository_service
            .download_archive(project_id, commit_sha)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to download archive: {e}")))?;

        match tokio::fs::remove_dir_all(&repo_path).await {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(HandlerError::Processing(format!(
                    "failed to clean cache directory: {e}"
                )));
            }
        }
        tokio::fs::create_dir_all(&repo_path).await.map_err(|e| {
            HandlerError::Processing(format!("failed to create cache directory: {e}"))
        })?;

        archive::extract_tar_gz(&archive_bytes, &repo_path)
            .map_err(|e| HandlerError::Processing(format!("failed to extract archive: {e}")))?;

        self.cache
            .update_commit(project_id, branch, commit_sha)
            .await
            .map_err(|e| HandlerError::Processing(format!("failed to save cache: {e}")))?;

        Ok(repo_path)
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

    struct ScriptedRepositoryService {
        archive: Mutex<Vec<u8>>,
    }

    impl ScriptedRepositoryService {
        fn with_archive(files: &[(&str, &str)]) -> Arc<Self> {
            Arc::new(Self {
                archive: Mutex::new(build_test_tar_gz(files)),
            })
        }

        fn set_archive(&self, files: &[(&str, &str)]) {
            *self.archive.lock() = build_test_tar_gz(files);
        }
    }

    fn build_test_tar_gz(files: &[(&str, &str)]) -> Vec<u8> {
        let mut tar_builder = tar::Builder::new(Vec::new());
        for (path, content) in files {
            let content_bytes = content.as_bytes();
            let mut header = tar::Header::new_gnu();
            header.set_path(path).unwrap();
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
        ) -> Result<Vec<u8>, RepositoryServiceError> {
            Ok(self.archive.lock().clone())
        }
    }

    fn create_resolver(
        service: Arc<ScriptedRepositoryService>,
    ) -> (tempfile::TempDir, RepositoryResolver) {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let cache: Arc<dyn RepositoryCache> =
            Arc::new(LocalRepositoryCache::new(temp_dir.path().to_path_buf()));
        let resolver = RepositoryResolver::new(service as Arc<dyn RepositoryService>, cache);
        (temp_dir, resolver)
    }

    #[tokio::test]
    async fn resolve_cache_miss_does_full_download() {
        let service = ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")]);
        let (_dir, resolver) = create_resolver(service);

        let path = resolver.resolve(1, "main", Some("abc123")).await.unwrap();

        assert!(path.join("src/main.rs").exists());
        let content = std::fs::read_to_string(path.join("src/main.rs")).unwrap();
        assert_eq!(content, "fn main() {}");
    }

    #[tokio::test]
    async fn resolve_cache_hit_returns_cached_path() {
        let service = ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")]);
        let (_dir, resolver) = create_resolver(service);

        let first_path = resolver.resolve(1, "main", Some("abc123")).await.unwrap();
        let second_path = resolver.resolve(1, "main", Some("abc123")).await.unwrap();

        assert_eq!(first_path, second_path);
    }

    #[tokio::test]
    async fn resolve_stale_cache_does_full_redownload() {
        let service = ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")]);
        let (_dir, resolver) = create_resolver(Arc::clone(&service));

        resolver.resolve(1, "main", Some("commit1")).await.unwrap();

        service.set_archive(&[("src/new.rs", "fn new() {}")]);

        let path = resolver.resolve(1, "main", Some("commit2")).await.unwrap();

        assert!(path.join("src/new.rs").exists());
        assert!(!path.join("src/main.rs").exists());
    }

    #[tokio::test]
    async fn resolve_uses_branch_when_no_commit_sha() {
        let service = ScriptedRepositoryService::with_archive(&[("src/main.rs", "fn main() {}")]);
        let (_dir, resolver) = create_resolver(service);

        let path = resolver.resolve(1, "main", None).await.unwrap();

        assert!(path.join("src/main.rs").exists());
    }
}
