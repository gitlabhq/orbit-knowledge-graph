//! Repository operations backed by the Rails internal API and Gitaly.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use gitaly_client::{GitalyClient, GitalyError, GitalyRepositoryConfig, RepositorySource};
use gitlab_client::{GitlabClient, GitlabClientError, RepositoryInfo};
use moka::future::Cache;

fn gitaly_config_from(info: &RepositoryInfo) -> GitalyRepositoryConfig {
    GitalyRepositoryConfig {
        address: info.gitaly_connection_info.address.clone(),
        storage: info.gitaly_connection_info.storage.clone(),
        relative_path: info.gitaly_connection_info.path.clone(),
        token: info.gitaly_connection_info.token.clone(),
    }
}

#[async_trait]
pub trait RepositoryService: Send + Sync {
    async fn repository_info(&self, project_id: i64) -> Result<RepositoryInfo, GitalyError>;
    async fn extract_repository(
        &self,
        repository: &RepositoryInfo,
        target_dir: &Path,
        commit_id: &str,
    ) -> Result<(), GitalyError>;
}

pub struct GitLabRepositoryService {
    gitlab_client: Arc<GitlabClient>,
}

impl GitLabRepositoryService {
    pub fn create(gitlab_client: Arc<GitlabClient>) -> Arc<dyn RepositoryService> {
        Arc::new(Self { gitlab_client })
    }
}

fn gitlab_error_to_gitaly(error: GitlabClientError) -> GitalyError {
    GitalyError::Config(error.to_string())
}

#[async_trait]
impl RepositoryService for GitLabRepositoryService {
    async fn repository_info(&self, project_id: i64) -> Result<RepositoryInfo, GitalyError> {
        self.gitlab_client
            .repository_info(project_id)
            .await
            .map_err(gitlab_error_to_gitaly)
    }

    async fn extract_repository(
        &self,
        repository: &RepositoryInfo,
        target_dir: &Path,
        commit_id: &str,
    ) -> Result<(), GitalyError> {
        let config = gitaly_config_from(repository);
        let client = GitalyClient::connect(config).await?;
        RepositorySource::extract_to(&client, target_dir, Some(commit_id)).await
    }
}

pub struct CachingRepositoryService {
    inner: Arc<dyn RepositoryService>,
    cache: Cache<i64, RepositoryInfo>,
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
    async fn repository_info(&self, project_id: i64) -> Result<RepositoryInfo, GitalyError> {
        if let Some(cached) = self.cache.get(&project_id).await {
            return Ok(cached);
        }

        let info = self.inner.repository_info(project_id).await?;
        self.cache.insert(project_id, info.clone()).await;
        Ok(info)
    }

    async fn extract_repository(
        &self,
        repository: &RepositoryInfo,
        target_dir: &Path,
        commit_id: &str,
    ) -> Result<(), GitalyError> {
        match self
            .inner
            .extract_repository(repository, target_dir, commit_id)
            .await
        {
            Ok(()) => Ok(()),
            Err(error) => {
                self.cache.invalidate(&repository.project_id).await;
                Err(error)
            }
        }
    }
}

#[cfg(test)]
pub mod test_utils {
    use super::*;
    use gitlab_client::GitalyConnectionInfo;
    use parking_lot::Mutex;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    pub fn make_repository_info(project_id: i64, default_branch: &str) -> RepositoryInfo {
        RepositoryInfo {
            project_id,
            default_branch: default_branch.to_string(),
            gitaly_connection_info: GitalyConnectionInfo {
                address: String::new(),
                token: None,
                storage: String::new(),
                path: String::new(),
            },
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
        async fn repository_info(&self, project_id: i64) -> Result<RepositoryInfo, GitalyError> {
            let default_branch = self
                .default_branches
                .lock()
                .get(&project_id)
                .cloned()
                .ok_or_else(|| {
                    GitalyError::Config(format!("no default branch for project {project_id}"))
                })?;

            Ok(make_repository_info(project_id, &default_branch))
        }

        async fn extract_repository(
            &self,
            _repository: &RepositoryInfo,
            _target_dir: &Path,
            _commit_id: &str,
        ) -> Result<(), GitalyError> {
            Ok(())
        }
    }

    pub struct CountingRepositoryService {
        pub inner: Arc<dyn RepositoryService>,
        pub repository_info_call_count: AtomicUsize,
        pub extract_should_fail: Mutex<bool>,
    }

    impl CountingRepositoryService {
        pub fn wrapping(inner: Arc<dyn RepositoryService>) -> Arc<Self> {
            Arc::new(Self {
                inner,
                repository_info_call_count: AtomicUsize::new(0),
                extract_should_fail: Mutex::new(false),
            })
        }

        pub fn repository_info_call_count(&self) -> usize {
            self.repository_info_call_count.load(Ordering::SeqCst)
        }

        pub fn set_extract_should_fail(&self, should_fail: bool) {
            *self.extract_should_fail.lock() = should_fail;
        }
    }

    #[async_trait]
    impl RepositoryService for CountingRepositoryService {
        async fn repository_info(&self, project_id: i64) -> Result<RepositoryInfo, GitalyError> {
            self.repository_info_call_count
                .fetch_add(1, Ordering::SeqCst);
            self.inner.repository_info(project_id).await
        }

        async fn extract_repository(
            &self,
            repository: &RepositoryInfo,
            target_dir: &Path,
            commit_id: &str,
        ) -> Result<(), GitalyError> {
            if *self.extract_should_fail.lock() {
                return Err(GitalyError::Config("simulated gitaly failure".to_string()));
            }
            self.inner
                .extract_repository(repository, target_dir, commit_id)
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
    async fn repository_info_returns_cached_result_on_second_call() {
        let mock = MockRepositoryService::with_default_branch(1, "main");
        let counting = CountingRepositoryService::wrapping(mock);
        let service = build_caching_service(Arc::clone(&counting));

        let first = service.repository_info(1).await.unwrap();
        let second = service.repository_info(1).await.unwrap();

        assert_eq!(first.default_branch, "main");
        assert_eq!(second.default_branch, "main");
        assert_eq!(counting.repository_info_call_count(), 1);
    }

    #[tokio::test]
    async fn repository_info_caches_per_project() {
        let mock = MockRepositoryService::with_default_branches(vec![(1, "main"), (2, "develop")]);
        let counting = CountingRepositoryService::wrapping(mock);
        let service = build_caching_service(Arc::clone(&counting));

        let info_1 = service.repository_info(1).await.unwrap();
        let info_2 = service.repository_info(2).await.unwrap();

        assert_eq!(info_1.default_branch, "main");
        assert_eq!(info_2.default_branch, "develop");
        assert_eq!(counting.repository_info_call_count(), 2);

        // Repeated calls should not increase count
        service.repository_info(1).await.unwrap();
        service.repository_info(2).await.unwrap();
        assert_eq!(counting.repository_info_call_count(), 2);
    }

    #[tokio::test]
    async fn extract_failure_invalidates_cache() {
        let mock = MockRepositoryService::with_default_branch(1, "main");
        let counting = CountingRepositoryService::wrapping(mock);
        let service = build_caching_service(Arc::clone(&counting));

        // Populate cache
        let info = service.repository_info(1).await.unwrap();
        assert_eq!(counting.repository_info_call_count(), 1);

        // Simulate Gitaly failure during extract
        counting.set_extract_should_fail(true);
        let result = service
            .extract_repository(&info, Path::new("/tmp"), "abc123")
            .await;
        assert!(result.is_err());

        // Cache should be invalidated, so next call fetches again
        counting.set_extract_should_fail(false);
        service.repository_info(1).await.unwrap();
        assert_eq!(counting.repository_info_call_count(), 2);
    }

    #[tokio::test]
    async fn extract_success_preserves_cache() {
        let mock = MockRepositoryService::with_default_branch(1, "main");
        let counting = CountingRepositoryService::wrapping(mock);
        let service = build_caching_service(Arc::clone(&counting));

        let info = service.repository_info(1).await.unwrap();
        assert_eq!(counting.repository_info_call_count(), 1);

        // Successful extract should not invalidate cache
        service
            .extract_repository(&info, Path::new("/tmp"), "abc123")
            .await
            .unwrap();

        service.repository_info(1).await.unwrap();
        assert_eq!(counting.repository_info_call_count(), 1);
    }

    #[tokio::test]
    async fn repository_info_error_is_not_cached() {
        let mock = MockRepositoryService::with_default_branch(1, "main");
        let counting = CountingRepositoryService::wrapping(mock);
        let service = build_caching_service(Arc::clone(&counting));

        // Project 99 doesn't exist in the mock
        let result = service.repository_info(99).await;
        assert!(result.is_err());

        // Should try again (not cache the error)
        let result = service.repository_info(99).await;
        assert!(result.is_err());
        assert_eq!(counting.repository_info_call_count(), 2);
    }
}
