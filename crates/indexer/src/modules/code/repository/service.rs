//! Repository operations backed by the Rails internal API.

use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::Stream;
use gitlab_client::{GitlabClient, GitlabClientError, ProjectInfo};
use moka::future::Cache;

pub type ByteStream =
    Pin<Box<dyn Stream<Item = Result<bytes::Bytes, RepositoryServiceError>> + Send>>;

#[derive(Debug, thiserror::Error)]
pub enum RepositoryServiceError {
    #[error("GitLab API error: {0}")]
    GitlabApi(#[from] GitlabClientError),

    #[error("archive extraction failed: {0}")]
    Archive(String),

    #[error("force push detected for project {0}")]
    ForcePush(i64),
}

impl RepositoryServiceError {
    pub fn is_force_push(&self) -> bool {
        matches!(self, Self::ForcePush(_))
    }
}

#[async_trait]
pub trait RepositoryService: Send + Sync {
    async fn project_info(&self, project_id: i64) -> Result<ProjectInfo, RepositoryServiceError>;

    async fn download_archive(
        &self,
        project_id: i64,
        ref_name: &str,
    ) -> Result<Vec<u8>, RepositoryServiceError>;

    async fn changed_paths(
        &self,
        project_id: i64,
        from_sha: &str,
        to_sha: &str,
    ) -> Result<ByteStream, RepositoryServiceError>;

    async fn download_blobs(
        &self,
        project_id: i64,
        from_sha: &str,
        to_sha: &str,
    ) -> Result<ByteStream, RepositoryServiceError>;
}

fn map_gitlab_error(error: GitlabClientError) -> RepositoryServiceError {
    match error {
        GitlabClientError::ForcePush(project_id) => RepositoryServiceError::ForcePush(project_id),
        other => RepositoryServiceError::GitlabApi(other),
    }
}

pub struct RailsRepositoryService {
    gitlab_client: Arc<GitlabClient>,
}

impl RailsRepositoryService {
    pub fn create(gitlab_client: Arc<GitlabClient>) -> Arc<dyn RepositoryService> {
        Arc::new(Self { gitlab_client })
    }
}

#[async_trait]
impl RepositoryService for RailsRepositoryService {
    async fn project_info(&self, project_id: i64) -> Result<ProjectInfo, RepositoryServiceError> {
        Ok(self.gitlab_client.project_info(project_id).await?)
    }

    async fn download_archive(
        &self,
        project_id: i64,
        ref_name: &str,
    ) -> Result<Vec<u8>, RepositoryServiceError> {
        Ok(self
            .gitlab_client
            .download_archive(project_id, ref_name)
            .await?)
    }

    async fn changed_paths(
        &self,
        project_id: i64,
        from_sha: &str,
        to_sha: &str,
    ) -> Result<ByteStream, RepositoryServiceError> {
        use futures::StreamExt;

        let stream = self
            .gitlab_client
            .changed_paths(project_id, from_sha, to_sha)
            .await
            .map_err(map_gitlab_error)?;

        Ok(Box::pin(stream.map(|r| r.map_err(map_gitlab_error))))
    }

    async fn download_blobs(
        &self,
        project_id: i64,
        from_sha: &str,
        to_sha: &str,
    ) -> Result<ByteStream, RepositoryServiceError> {
        use futures::StreamExt;

        let stream = self
            .gitlab_client
            .download_blobs(project_id, from_sha, to_sha)
            .await
            .map_err(map_gitlab_error)?;

        Ok(Box::pin(stream.map(|r| r.map_err(map_gitlab_error))))
    }
}

pub struct CachingRepositoryService {
    inner: Arc<dyn RepositoryService>,
    cache: Cache<i64, ProjectInfo>,
}

impl CachingRepositoryService {
    pub fn create(inner: Arc<dyn RepositoryService>) -> Arc<dyn RepositoryService> {
        let cache = Cache::builder()
            .max_capacity(1000)
            .time_to_live(Duration::from_secs(3600))
            .build();

        Arc::new(Self { inner, cache })
    }
}

#[async_trait]
impl RepositoryService for CachingRepositoryService {
    async fn project_info(&self, project_id: i64) -> Result<ProjectInfo, RepositoryServiceError> {
        if let Some(cached) = self.cache.get(&project_id).await {
            return Ok(cached);
        }

        let info = self.inner.project_info(project_id).await?;
        self.cache.insert(project_id, info.clone()).await;
        Ok(info)
    }

    async fn download_archive(
        &self,
        project_id: i64,
        ref_name: &str,
    ) -> Result<Vec<u8>, RepositoryServiceError> {
        match self.inner.download_archive(project_id, ref_name).await {
            Ok(bytes) => Ok(bytes),
            Err(error) => {
                self.cache.invalidate(&project_id).await;
                Err(error)
            }
        }
    }

    async fn changed_paths(
        &self,
        project_id: i64,
        from_sha: &str,
        to_sha: &str,
    ) -> Result<ByteStream, RepositoryServiceError> {
        self.inner.changed_paths(project_id, from_sha, to_sha).await
    }

    async fn download_blobs(
        &self,
        project_id: i64,
        from_sha: &str,
        to_sha: &str,
    ) -> Result<ByteStream, RepositoryServiceError> {
        self.inner
            .download_blobs(project_id, from_sha, to_sha)
            .await
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub fn make_project_info(project_id: i64, default_branch: &str) -> ProjectInfo {
        ProjectInfo {
            project_id,
            default_branch: default_branch.to_string(),
        }
    }

    pub struct MockRepositoryService {
        default_branches: Mutex<HashMap<i64, String>>,
    }

    impl MockRepositoryService {
        pub fn with_default_branch(project_id: i64, branch: &str) -> Arc<Self> {
            Self::with_default_branches(vec![(project_id, branch)])
        }

        pub fn with_default_branches(entries: Vec<(i64, &str)>) -> Arc<Self> {
            let map = entries
                .into_iter()
                .map(|(id, branch)| (id, branch.to_string()))
                .collect();
            Arc::new(Self {
                default_branches: Mutex::new(map),
            })
        }
    }

    #[async_trait]
    impl RepositoryService for MockRepositoryService {
        async fn project_info(
            &self,
            project_id: i64,
        ) -> Result<ProjectInfo, RepositoryServiceError> {
            let default_branch = self
                .default_branches
                .lock()
                .get(&project_id)
                .cloned()
                .ok_or_else(|| {
                    RepositoryServiceError::Archive(format!(
                        "no default branch for project {project_id}"
                    ))
                })?;

            Ok(make_project_info(project_id, &default_branch))
        }

        async fn download_archive(
            &self,
            _project_id: i64,
            _ref_name: &str,
        ) -> Result<Vec<u8>, RepositoryServiceError> {
            Ok(Vec::new())
        }

        async fn changed_paths(
            &self,
            _project_id: i64,
            _from_sha: &str,
            _to_sha: &str,
        ) -> Result<ByteStream, RepositoryServiceError> {
            Ok(Box::pin(futures::stream::empty()))
        }

        async fn download_blobs(
            &self,
            _project_id: i64,
            _from_sha: &str,
            _to_sha: &str,
        ) -> Result<ByteStream, RepositoryServiceError> {
            Ok(Box::pin(futures::stream::empty()))
        }
    }

    pub struct CountingRepositoryService {
        pub inner: Arc<dyn RepositoryService>,
        pub project_info_call_count: AtomicUsize,
        pub download_should_fail: Mutex<bool>,
    }

    impl CountingRepositoryService {
        pub fn wrapping(inner: Arc<dyn RepositoryService>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                project_info_call_count: AtomicUsize::new(0),
                download_should_fail: Mutex::new(false),
            })
        }

        pub fn project_info_call_count(&self) -> usize {
            self.project_info_call_count.load(Ordering::SeqCst)
        }

        pub fn set_download_should_fail(&self, should_fail: bool) {
            *self.download_should_fail.lock() = should_fail;
        }
    }

    #[async_trait]
    impl RepositoryService for CountingRepositoryService {
        async fn project_info(
            &self,
            project_id: i64,
        ) -> Result<ProjectInfo, RepositoryServiceError> {
            self.project_info_call_count.fetch_add(1, Ordering::SeqCst);
            self.inner.project_info(project_id).await
        }

        async fn download_archive(
            &self,
            project_id: i64,
            ref_name: &str,
        ) -> Result<Vec<u8>, RepositoryServiceError> {
            if *self.download_should_fail.lock() {
                return Err(RepositoryServiceError::Archive(
                    "simulated download failure".to_string(),
                ));
            }
            self.inner.download_archive(project_id, ref_name).await
        }

        async fn changed_paths(
            &self,
            project_id: i64,
            from_sha: &str,
            to_sha: &str,
        ) -> Result<ByteStream, RepositoryServiceError> {
            self.inner.changed_paths(project_id, from_sha, to_sha).await
        }

        async fn download_blobs(
            &self,
            project_id: i64,
            from_sha: &str,
            to_sha: &str,
        ) -> Result<ByteStream, RepositoryServiceError> {
            self.inner
                .download_blobs(project_id, from_sha, to_sha)
                .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::{CountingRepositoryService, MockRepositoryService};

    fn build_caching_service(
        counting: Arc<test_utils::CountingRepositoryService>,
    ) -> Arc<dyn RepositoryService> {
        let cache = Cache::builder()
            .max_capacity(1000)
            .time_to_live(Duration::from_secs(3600))
            .build();

        Arc::new(CachingRepositoryService {
            inner: counting,
            cache,
        })
    }

    #[tokio::test]
    async fn project_info_returns_cached_result_on_second_call() {
        let mock = MockRepositoryService::with_default_branch(1, "main");
        let counting = CountingRepositoryService::wrapping(mock);
        let service = build_caching_service(Arc::clone(&counting));

        let first = service.project_info(1).await.unwrap();
        let second = service.project_info(1).await.unwrap();

        assert_eq!(first.default_branch, "main");
        assert_eq!(second.default_branch, "main");
        assert_eq!(counting.project_info_call_count(), 1);
    }

    #[tokio::test]
    async fn project_info_caches_per_project() {
        let mock = MockRepositoryService::with_default_branches(vec![(1, "main"), (2, "develop")]);
        let counting = CountingRepositoryService::wrapping(mock);
        let service = build_caching_service(Arc::clone(&counting));

        let info_1 = service.project_info(1).await.unwrap();
        let info_2 = service.project_info(2).await.unwrap();

        assert_eq!(info_1.default_branch, "main");
        assert_eq!(info_2.default_branch, "develop");
        assert_eq!(counting.project_info_call_count(), 2);

        service.project_info(1).await.unwrap();
        service.project_info(2).await.unwrap();
        assert_eq!(counting.project_info_call_count(), 2);
    }

    #[tokio::test]
    async fn download_failure_invalidates_cache() {
        let mock = MockRepositoryService::with_default_branch(1, "main");
        let counting = CountingRepositoryService::wrapping(mock);
        let service = build_caching_service(Arc::clone(&counting));

        service.project_info(1).await.unwrap();
        assert_eq!(counting.project_info_call_count(), 1);

        counting.set_download_should_fail(true);
        let result = service.download_archive(1, "main").await;
        assert!(result.is_err());

        counting.set_download_should_fail(false);
        service.project_info(1).await.unwrap();
        assert_eq!(counting.project_info_call_count(), 2);
    }

    #[tokio::test]
    async fn download_success_preserves_cache() {
        let mock = MockRepositoryService::with_default_branch(1, "main");
        let counting = CountingRepositoryService::wrapping(mock);
        let service = build_caching_service(Arc::clone(&counting));

        service.project_info(1).await.unwrap();
        assert_eq!(counting.project_info_call_count(), 1);

        service.download_archive(1, "main").await.unwrap();

        service.project_info(1).await.unwrap();
        assert_eq!(counting.project_info_call_count(), 1);
    }

    #[tokio::test]
    async fn project_info_error_is_not_cached() {
        let mock = MockRepositoryService::with_default_branch(1, "main");
        let counting = CountingRepositoryService::wrapping(mock);
        let service = build_caching_service(Arc::clone(&counting));

        let result = service.project_info(99).await;
        assert!(result.is_err());

        let result = service.project_info(99).await;
        assert!(result.is_err());
        assert_eq!(counting.project_info_call_count(), 2);
    }
}
