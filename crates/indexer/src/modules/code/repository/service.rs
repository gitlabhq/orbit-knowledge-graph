use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::{Stream, StreamExt};
use gitaly_protos::proto::repository_service_client::RepositoryServiceClient;
use gitaly_protos::proto::{GetArchiveRequest, get_archive_request};
use gitlab_client::{
    GitalyProxyError, GitlabClient, GitlabClientError, ProjectInfo, StatusClass, classify_status,
    proxy_reason,
};
use moka::future::Cache;
use rand::RngExt;
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio_util::io::ReaderStream;
use tonic::{Code, Status};

use crate::modules::code::metrics::CodeMetrics;

pub type ByteStream =
    Pin<Box<dyn Stream<Item = Result<bytes::Bytes, RepositoryServiceError>> + Send>>;

#[derive(Debug, thiserror::Error)]
pub enum RepositoryServiceError {
    #[error("GitLab API error: {0}")]
    GitlabApi(#[from] GitlabClientError),

    #[error("Gitaly proxy error: {0}")]
    GitalyProxy(#[from] GitalyProxyError),

    #[error("archive extraction failed: {0}")]
    Archive(String),
}

impl From<std::io::Error> for RepositoryServiceError {
    fn from(error: std::io::Error) -> Self {
        Self::Archive(error.to_string())
    }
}

const RETRY_BASE: Duration = Duration::from_secs(1);
const RETRY_CAP: Duration = Duration::from_secs(30);

#[derive(Clone)]
struct ArchiveRetry {
    max_attempts: usize,
    sleep: Arc<
        dyn Fn(Duration) -> Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync,
    >,
    jitter: Arc<dyn Fn(Duration) -> Duration + Send + Sync>,
}

impl ArchiveRetry {
    fn new(max_attempts: usize) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
            sleep: Arc::new(|duration| Box::pin(tokio::time::sleep(duration))),
            jitter: Arc::new(|cap| {
                let upper = u64::try_from(cap.as_millis()).unwrap_or(u64::MAX).max(1);
                Duration::from_millis(rand::rng().random_range(0..upper))
            }),
        }
    }

    async fn wait(&self, completed_attempts: usize) {
        let exponent = u32::try_from(completed_attempts.saturating_sub(1)).unwrap_or(u32::MAX);
        let cap = RETRY_BASE
            .checked_mul(2_u32.saturating_pow(exponent))
            .unwrap_or(RETRY_CAP)
            .min(RETRY_CAP);
        (self.sleep)((self.jitter)(cap)).await;
    }
}

pub struct GitalyRepositoryService {
    gitlab_client: Arc<GitlabClient>,
    rails: Arc<dyn RepositoryService>,
    with_fallback: bool,
    retry: ArchiveRetry,
    metrics: CodeMetrics,
}

impl GitalyRepositoryService {
    pub fn create(
        gitlab_client: Arc<GitlabClient>,
        rails: Arc<dyn RepositoryService>,
        with_fallback: bool,
        stream_retry_max_attempts: usize,
        metrics: CodeMetrics,
    ) -> Arc<dyn RepositoryService> {
        Arc::new(Self {
            gitlab_client,
            rails,
            with_fallback,
            retry: ArchiveRetry::new(stream_retry_max_attempts),
            metrics,
        })
    }

    async fn open_archive(
        client: Arc<GitlabClient>,
        project_id: i64,
        ref_name: String,
    ) -> Result<tonic::Streaming<gitaly_protos::proto::GetArchiveResponse>, GitalyProxyError> {
        client
            .with_gitaly_channel(project_id, move |channel| {
                let ref_name = ref_name.clone();
                async move {
                    RepositoryServiceClient::new(channel.channel())
                        .get_archive(GetArchiveRequest {
                            repository: Some(channel.repository()),
                            commit_id: ref_name,
                            prefix: String::new(),
                            format: get_archive_request::Format::TarGz as i32,
                            path: Vec::new(),
                            exclude: Vec::new(),
                            elide_path: false,
                            include_lfs_blobs: true,
                        })
                        .await
                        .map(tonic::Response::into_inner)
                }
            })
            .await
    }

    async fn open_archive_with_backpressure_retry(
        &self,
        project_id: i64,
        ref_name: String,
    ) -> Result<tonic::Streaming<gitaly_protos::proto::GetArchiveResponse>, GitalyProxyError> {
        let mut attempt = 1;
        loop {
            match Self::open_archive(
                Arc::clone(&self.gitlab_client),
                project_id,
                ref_name.clone(),
            )
            .await
            {
                Err(GitalyProxyError::Rpc(status))
                    if is_max_concurrent_streams(&status) && attempt < self.retry.max_attempts =>
                {
                    self.retry.wait(attempt).await;
                    attempt += 1;
                }
                result => return result,
            }
        }
    }

    fn should_fallback(error: &GitalyProxyError) -> bool {
        matches!(
            error,
            GitalyProxyError::Forbidden { .. }
                | GitalyProxyError::NotAvailable { .. }
                | GitalyProxyError::Busy { .. }
                | GitalyProxyError::PolicyDenied { .. }
        )
    }

    async fn download_proxy_archive(
        &self,
        project_id: i64,
        ref_name: String,
        mut stream: tonic::Streaming<gitaly_protos::proto::GetArchiveResponse>,
    ) -> Result<ByteStream, RepositoryServiceError> {
        let mut attempt = 1;
        loop {
            let mut file = tokio::fs::File::from_std(tempfile::tempfile()?);
            let mut saw_frame = false;
            loop {
                match stream.message().await {
                    Ok(Some(response)) => {
                        saw_frame = true;
                        file.write_all(&response.data).await?;
                    }
                    Ok(None) => {
                        file.rewind().await?;
                        self.metrics.record_gitaly_transport("workhorse_ws", "ok");
                        return Ok(Box::pin(ReaderStream::new(file).map(|result| {
                            result
                                .map_err(|error| RepositoryServiceError::Archive(error.to_string()))
                        })));
                    }
                    Err(status) if classify_status(&status) == StatusClass::StreamDeadline => {
                        self.metrics.record_gitaly_stream_deadline();
                        self.metrics
                            .record_gitaly_transport("workhorse_ws", "stream_deadline");
                        return Err(GitalyProxyError::StreamDeadline.into());
                    }
                    Err(status) if saw_frame && is_restartable_stream_cut(&status) => {
                        if attempt >= self.retry.max_attempts {
                            self.metrics
                                .record_gitaly_transport("workhorse_ws", "retry_exhausted");
                            return Err(GitalyProxyError::Rpc(status).into());
                        }
                        self.metrics.record_gitaly_restart(restart_reason(&status));
                        self.retry.wait(attempt).await;
                        attempt += 1;
                        stream = self
                            .open_archive_with_backpressure_retry(project_id, ref_name.clone())
                            .await?;
                        break;
                    }
                    Err(status) => return Err(GitalyProxyError::from(status).into()),
                }
            }
        }
    }
}

#[async_trait]
impl RepositoryService for GitalyRepositoryService {
    async fn project_info(&self, project_id: i64) -> Result<ProjectInfo, RepositoryServiceError> {
        self.rails.project_info(project_id).await
    }

    async fn download_archive(
        &self,
        project_id: i64,
        ref_name: &str,
    ) -> Result<ByteStream, RepositoryServiceError> {
        let ref_name = ref_name.to_owned();
        let stream = match self
            .open_archive_with_backpressure_retry(project_id, ref_name.clone())
            .await
        {
            Ok(stream) => stream,
            Err(error) if self.with_fallback && Self::should_fallback(&error) => {
                if matches!(error, GitalyProxyError::PolicyDenied { .. }) {
                    tracing::warn!(project_id, %error, "Gitaly proxy policy denied GetArchive; falling back to Rails HTTP");
                }
                self.metrics
                    .record_gitaly_transport("workhorse_ws", "fallback");
                return self.rails.download_archive(project_id, &ref_name).await;
            }
            Err(error) => return Err(error.into()),
        };

        self.download_proxy_archive(project_id, ref_name, stream)
            .await
    }
}

fn is_restartable_stream_cut(status: &Status) -> bool {
    proxy_reason(status).is_none()
        && matches!(
            status.code(),
            Code::Unavailable | Code::Internal | Code::Unknown
        )
}

fn is_max_concurrent_streams(status: &Status) -> bool {
    status.code() == Code::ResourceExhausted
        && proxy_reason(status) == Some("max_concurrent_streams")
}

fn restart_reason(status: &Status) -> &'static str {
    match status.code() {
        Code::Unavailable => "unavailable",
        Code::Internal => "internal",
        Code::Unknown => "unknown",
        _ => "other",
    }
}

#[async_trait]
pub trait RepositoryService: Send + Sync {
    async fn project_info(&self, project_id: i64) -> Result<ProjectInfo, RepositoryServiceError>;

    async fn download_archive(
        &self,
        project_id: i64,
        ref_name: &str,
    ) -> Result<ByteStream, RepositoryServiceError>;
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
    ) -> Result<ByteStream, RepositoryServiceError> {
        use futures::StreamExt;

        let stream = self
            .gitlab_client
            .download_archive(project_id, ref_name)
            .await?;

        Ok(Box::pin(
            stream.map(|r| r.map_err(RepositoryServiceError::GitlabApi)),
        ))
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
    ) -> Result<ByteStream, RepositoryServiceError> {
        match self.inner.download_archive(project_id, ref_name).await {
            Ok(stream) => Ok(stream),
            Err(error) => {
                self.cache.invalidate(&project_id).await;
                Err(error)
            }
        }
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
        download_errors: Mutex<HashMap<i64, RepositoryServiceError>>,
        project_info_errors: Mutex<HashMap<i64, RepositoryServiceError>>,
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
                download_errors: Mutex::new(HashMap::new()),
                project_info_errors: Mutex::new(HashMap::new()),
            })
        }

        pub fn set_download_error(&self, project_id: i64, error: RepositoryServiceError) {
            self.download_errors.lock().insert(project_id, error);
        }

        pub fn set_project_info_error(&self, project_id: i64, error: RepositoryServiceError) {
            self.project_info_errors.lock().insert(project_id, error);
        }
    }

    #[async_trait]
    impl RepositoryService for MockRepositoryService {
        async fn project_info(
            &self,
            project_id: i64,
        ) -> Result<ProjectInfo, RepositoryServiceError> {
            if let Some(err) = self.project_info_errors.lock().remove(&project_id) {
                return Err(err);
            }

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
            project_id: i64,
            _ref_name: &str,
        ) -> Result<ByteStream, RepositoryServiceError> {
            if let Some(err) = self.download_errors.lock().remove(&project_id) {
                return Err(err);
            }
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
        ) -> Result<ByteStream, RepositoryServiceError> {
            if *self.download_should_fail.lock() {
                return Err(RepositoryServiceError::Archive(
                    "simulated download failure".to_string(),
                ));
            }
            self.inner.download_archive(project_id, ref_name).await
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

        let _stream = service.download_archive(1, "main").await.unwrap();

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
