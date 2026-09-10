use std::sync::Arc;
use std::time::Duration;

use futures::FutureExt;
use gitaly_protos::proto::blob_service_client::BlobServiceClient;
use gitaly_protos::proto::list_blobs_response::Blob as BlobChunk;
use gitaly_protos::proto::{ListBlobsRequest, ListBlobsResponse};
use gitlab_client::{
    GitalyProxyChannel, GitalyProxyError, GitlabClient, StatusClass, classify_status,
};
use indexer::modules::code::repository::blob_stream::ResolvedBlob;
use moka::future::Cache;
use orbit_server_config::GitalyProxyConfig;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

const MAX_BLOB_SIZE: usize = 1024 * 1024;

struct CachedChannel {
    channel: Arc<GitalyProxyChannel>,
    streams: Arc<Semaphore>,
    _session_permit: OwnedSemaphorePermit,
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ChannelAcquireError {
    #[error("proxy session admission saturated")]
    SessionAdmission,
    #[error(transparent)]
    Proxy(#[from] GitalyProxyError),
}

#[derive(Debug, thiserror::Error)]
pub(super) enum ProxyBlobError {
    #[error("global proxy stream admission saturated")]
    StreamAdmission,
    #[error("project proxy stream admission saturated")]
    ChannelStreamAdmission,
    #[error("proxy is temporarily unavailable for this project")]
    NegativeCached,
    #[error(transparent)]
    Acquire(Arc<ChannelAcquireError>),
    #[error(transparent)]
    Proxy(#[from] GitalyProxyError),
}

impl ProxyBlobError {
    pub(super) fn allows_rails_fallback(&self) -> bool {
        match self {
            Self::StreamAdmission | Self::ChannelStreamAdmission | Self::NegativeCached => true,
            Self::Acquire(error) => match error.as_ref() {
                ChannelAcquireError::SessionAdmission => true,
                ChannelAcquireError::Proxy(error) => proxy_error_allows_fallback(error),
            },
            Self::Proxy(error) => proxy_error_allows_fallback(error),
        }
    }

    pub(super) fn outcome(&self) -> &'static str {
        match self {
            Self::StreamAdmission => "stream_saturated",
            Self::ChannelStreamAdmission => "channel_stream_saturated",
            Self::NegativeCached => "negative_cached",
            Self::Acquire(error) => match error.as_ref() {
                ChannelAcquireError::SessionAdmission => "session_saturated",
                ChannelAcquireError::Proxy(GitalyProxyError::NotAvailable { .. }) => {
                    "not_available"
                }
                ChannelAcquireError::Proxy(_) => "connect_error",
            },
            Self::Proxy(GitalyProxyError::PolicyDenied { .. }) => "policy_denied",
            Self::Proxy(GitalyProxyError::StreamDeadline) => "stream_deadline",
            Self::Proxy(_) => "rpc_error",
        }
    }
}

fn proxy_error_allows_fallback(error: &GitalyProxyError) -> bool {
    matches!(
        error,
        GitalyProxyError::Forbidden { .. }
            | GitalyProxyError::NotAvailable { .. }
            | GitalyProxyError::Busy { .. }
            | GitalyProxyError::PolicyDenied { .. }
    )
}

pub(super) struct ProxyBlobService {
    client: Arc<GitlabClient>,
    channels: Cache<i64, Arc<CachedChannel>>,
    negative: Cache<i64, ()>,
    sessions: Arc<Semaphore>,
    streams: Arc<Semaphore>,
    streams_per_channel: usize,
}

impl ProxyBlobService {
    pub(super) fn new(client: Arc<GitlabClient>, config: &GitalyProxyConfig) -> Self {
        let eviction_client = Arc::clone(&client);
        let channels = Cache::builder()
            .max_capacity(config.webserver_channel_cache_capacity)
            .time_to_idle(Duration::from_secs(
                config.webserver_channel_idle_timeout_secs,
            ))
            .async_eviction_listener(move |_project_id, cached: Arc<CachedChannel>, _cause| {
                let client = Arc::clone(&eviction_client);
                async move {
                    client.invalidate_gitaly_channel(&cached.channel);
                }
                .boxed()
            })
            .build();
        let negative = Cache::builder()
            .max_capacity(config.webserver_channel_cache_capacity)
            .time_to_live(Duration::from_secs(
                config.webserver_negative_cache_ttl_secs,
            ))
            .build();

        Self {
            client,
            channels,
            negative,
            sessions: Arc::new(Semaphore::new(config.webserver_max_sessions)),
            streams: Arc::new(Semaphore::new(config.webserver_max_inflight_streams)),
            streams_per_channel: config.webserver_max_inflight_streams_per_channel,
        }
    }

    async fn channel(&self, project_id: i64) -> Result<Arc<CachedChannel>, ProxyBlobError> {
        if self.negative.get(&project_id).await.is_some() {
            return Err(ProxyBlobError::NegativeCached);
        }

        if let Some(cached) = self.channels.get(&project_id).await {
            if cached.channel.accepts_new_streams() {
                return Ok(cached);
            }
            self.channels.invalidate(&project_id).await;
        }

        let client = Arc::clone(&self.client);
        let sessions = Arc::clone(&self.sessions);
        let streams_per_channel = self.streams_per_channel;
        let acquired = self
            .channels
            .try_get_with(project_id, async move {
                let permit = sessions
                    .try_acquire_owned()
                    .map_err(|_| ChannelAcquireError::SessionAdmission)?;
                let channel = client.gitaly_channel(project_id).await?;
                Ok::<_, ChannelAcquireError>(Arc::new(CachedChannel {
                    channel,
                    streams: Arc::new(Semaphore::new(streams_per_channel)),
                    _session_permit: permit,
                }))
            })
            .await;

        match acquired {
            Ok(cached) => {
                self.channels.run_pending_tasks().await;
                Ok(cached)
            }
            Err(error) => {
                if matches!(
                    error.as_ref(),
                    ChannelAcquireError::Proxy(GitalyProxyError::NotAvailable { .. })
                ) {
                    self.negative.insert(project_id, ()).await;
                }
                Err(ProxyBlobError::Acquire(error))
            }
        }
    }

    async fn evict(&self, project_id: i64, cached: &Arc<CachedChannel>) {
        self.client.invalidate_gitaly_channel(&cached.channel);
        self.channels.invalidate(&project_id).await;
        self.channels.run_pending_tasks().await;
    }

    async fn open_stream(
        channel: &Arc<GitalyProxyChannel>,
        revisions: Vec<String>,
    ) -> Result<tonic::Streaming<ListBlobsResponse>, Status> {
        BlobServiceClient::new(channel.channel())
            .list_blobs(ListBlobsRequest {
                repository: Some(channel.repository()),
                revisions,
                bytes_limit: -1,
                with_paths: false,
                ..Default::default()
            })
            .await
            .map(tonic::Response::into_inner)
    }

    pub(super) async fn fetch(
        &self,
        project_id: i64,
        revisions: &[String],
    ) -> Result<Vec<ResolvedBlob>, ProxyBlobError> {
        let _stream_permit = Arc::clone(&self.streams)
            .try_acquire_owned()
            .map_err(|_| ProxyBlobError::StreamAdmission)?;
        let mut cached = self.channel(project_id).await?;
        let mut _channel_stream_permit = Some(
            Arc::clone(&cached.streams)
                .try_acquire_owned()
                .map_err(|_| ProxyBlobError::ChannelStreamAdmission)?,
        );
        let mut stream = match Self::open_stream(&cached.channel, revisions.to_vec()).await {
            Ok(stream) => stream,
            Err(status) if classify_status(&status) == StatusClass::StaleSession => {
                drop(_channel_stream_permit.take());
                self.evict(project_id, &cached).await;
                cached = self.channel(project_id).await?;
                _channel_stream_permit = Some(
                    Arc::clone(&cached.streams)
                        .try_acquire_owned()
                        .map_err(|_| ProxyBlobError::ChannelStreamAdmission)?,
                );
                match Self::open_stream(&cached.channel, revisions.to_vec()).await {
                    Ok(stream) => stream,
                    Err(status) if classify_status(&status) == StatusClass::StaleSession => {
                        self.evict(project_id, &cached).await;
                        return Err(GitalyProxyError::StaleAfterRetry(status).into());
                    }
                    Err(status) => {
                        self.evict(project_id, &cached).await;
                        return Err(GitalyProxyError::from(status).into());
                    }
                }
            }
            Err(status) => {
                self.evict(project_id, &cached).await;
                return Err(GitalyProxyError::from(status).into());
            }
        };

        let result = drain_stream(&mut stream).await;
        if result.is_err() {
            self.evict(project_id, &cached).await;
        }
        result.map_err(|status| GitalyProxyError::from(status).into())
    }

    #[cfg(test)]
    pub(super) async fn run_cache_maintenance(&self) {
        self.channels.run_pending_tasks().await;
        self.negative.run_pending_tasks().await;
    }
}

async fn drain_stream(
    stream: &mut tonic::Streaming<ListBlobsResponse>,
) -> Result<Vec<ResolvedBlob>, Status> {
    let mut resolved = Vec::new();
    let mut current: Option<ResolvedBlob> = None;

    while let Some(response) = stream.message().await? {
        for chunk in response.blobs {
            append_chunk(&mut resolved, &mut current, chunk)?;
        }
    }
    if let Some(blob) = current {
        resolved.push(blob);
    }
    Ok(resolved)
}

fn append_chunk(
    resolved: &mut Vec<ResolvedBlob>,
    current: &mut Option<ResolvedBlob>,
    chunk: BlobChunk,
) -> Result<(), Status> {
    if !chunk.oid.is_empty() {
        if let Some(blob) = current.replace(ResolvedBlob {
            oid: chunk.oid,
            data: chunk.data,
        }) {
            resolved.push(blob);
        }
    } else if let Some(blob) = current {
        blob.data.extend_from_slice(&chunk.data);
    }

    if current
        .as_ref()
        .is_some_and(|blob| blob.data.len() > MAX_BLOB_SIZE)
    {
        return Err(Status::resource_exhausted(format!(
            "blob exceeds maximum size of {MAX_BLOB_SIZE} bytes"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gitlab_client::test_support::{FakeWorkhorse, Preauth, StreamPlan, direct, serve};
    use tonic::Code;

    fn config() -> GitalyProxyConfig {
        GitalyProxyConfig {
            webserver_channel_cache_capacity: 2,
            webserver_max_sessions: 3,
            webserver_max_inflight_streams: 2,
            webserver_channel_idle_timeout_secs: 60,
            webserver_negative_cache_ttl_secs: 1,
            ..Default::default()
        }
    }

    async fn fetch(service: &ProxyBlobService, project_id: i64) -> Result<(), ProxyBlobError> {
        service
            .fetch(project_id, &["HEAD:src/lib.rs".to_owned()])
            .await
            .map(|_| ())
    }

    async fn wait_for_rpcs(fake: &FakeWorkhorse, expected: usize) {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while fake.rpcs() < expected {
            assert!(tokio::time::Instant::now() < deadline);
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    #[tokio::test]
    async fn reuses_one_project_channel_across_requests() {
        let fake = FakeWorkhorse::start(Preauth::ok("600"), serve(1, 0)).await;
        let service = ProxyBlobService::new(Arc::new(fake.client()), &config());

        let blobs = service
            .fetch(42, &["main:src/lib.rs".to_owned()])
            .await
            .unwrap();
        fetch(&service, 42).await.unwrap();

        assert_eq!(blobs.len(), 1);
        assert_eq!(blobs[0].data, b"content-0");
        assert_eq!(fake.upgrades(), 1);
        assert_eq!(fake.rpcs(), 2);
        let requests = fake.blob_requests();
        assert_eq!(requests[0].revisions, ["main:src/lib.rs"]);
        assert_eq!(requests[0].bytes_limit, -1);
        assert!(!requests[0].with_paths);
    }

    #[tokio::test]
    async fn rotates_before_the_session_expires() {
        let fake = FakeWorkhorse::start(Preauth::ok("2"), serve(1, 0)).await;
        let service = ProxyBlobService::new(Arc::new(fake.client()), &config());

        fetch(&service, 42).await.unwrap();
        fetch(&service, 42).await.unwrap();

        assert_eq!(fake.upgrades(), 2);
        assert_eq!(fake.rpcs(), 2);
    }

    #[tokio::test]
    async fn capacity_evicts_the_least_recently_used_project() {
        let fake = FakeWorkhorse::start(Preauth::ok("600"), serve(1, 0)).await;
        let mut cfg = config();
        cfg.webserver_channel_cache_capacity = 1;
        cfg.webserver_max_sessions = 2;
        let service = ProxyBlobService::new(Arc::new(fake.client()), &cfg);

        fetch(&service, 1).await.unwrap();
        fetch(&service, 2).await.unwrap();
        service.run_cache_maintenance().await;
        fetch(&service, 1).await.unwrap();

        assert_eq!(fake.upgrades(), 3);
        assert_eq!(fake.rpcs(), 3);
    }

    #[tokio::test]
    async fn idle_channels_are_evicted() {
        let fake = FakeWorkhorse::start(Preauth::ok("600"), serve(1, 0)).await;
        let mut cfg = config();
        cfg.webserver_channel_idle_timeout_secs = 1;
        let service = ProxyBlobService::new(Arc::new(fake.client()), &cfg);

        fetch(&service, 42).await.unwrap();
        tokio::time::sleep(Duration::from_millis(1100)).await;
        service.run_cache_maintenance().await;
        fetch(&service, 42).await.unwrap();

        assert_eq!(fake.upgrades(), 2);
    }

    #[tokio::test]
    async fn rpc_errors_evict_the_channel() {
        const PLANS: &[StreamPlan] = &[
            StreamPlan::Reject(Code::Internal, "gitaly failure"),
            StreamPlan::Serve {
                count: 1,
                interval: Duration::ZERO,
            },
        ];
        let fake = FakeWorkhorse::start(Preauth::ok("600"), direct(PLANS)).await;
        let service = ProxyBlobService::new(Arc::new(fake.client()), &config());

        assert!(fetch(&service, 42).await.is_err());
        fetch(&service, 42).await.unwrap();

        assert_eq!(fake.upgrades(), 2);
        assert_eq!(fake.rpcs(), 2);
    }

    #[tokio::test]
    async fn stream_saturation_fails_fast_and_allows_fallback() {
        let fake = FakeWorkhorse::start(Preauth::ok("600"), serve(1, 500)).await;
        let mut cfg = config();
        cfg.webserver_max_inflight_streams = 1;
        let service = Arc::new(ProxyBlobService::new(Arc::new(fake.client()), &cfg));
        let first = {
            let service = Arc::clone(&service);
            tokio::spawn(async move { fetch(&service, 42).await })
        };
        wait_for_rpcs(&fake, 1).await;

        let error = fetch(&service, 42).await.unwrap_err();

        assert!(matches!(error, ProxyBlobError::StreamAdmission));
        assert!(error.allows_rails_fallback());
        first.await.unwrap().unwrap();
        assert_eq!(fake.rpcs(), 1);
    }

    #[tokio::test]
    async fn per_channel_stream_saturation_fails_fast_and_allows_fallback() {
        let fake = FakeWorkhorse::start(Preauth::ok("600"), serve(1, 500)).await;
        let mut cfg = config();
        cfg.webserver_max_inflight_streams_per_channel = 1;
        let service = Arc::new(ProxyBlobService::new(Arc::new(fake.client()), &cfg));
        let first = {
            let service = Arc::clone(&service);
            tokio::spawn(async move { fetch(&service, 42).await })
        };
        wait_for_rpcs(&fake, 1).await;

        let error = fetch(&service, 42).await.unwrap_err();

        assert!(matches!(error, ProxyBlobError::ChannelStreamAdmission));
        assert!(error.allows_rails_fallback());
        first.await.unwrap().unwrap();
        assert_eq!(fake.rpcs(), 1);
    }

    #[tokio::test]
    async fn session_saturation_fails_fast_and_allows_fallback() {
        let fake = FakeWorkhorse::start(Preauth::ok("600"), serve(1, 0)).await;
        let mut cfg = config();
        cfg.webserver_max_sessions = 1;
        let service = ProxyBlobService::new(Arc::new(fake.client()), &cfg);

        fetch(&service, 1).await.unwrap();
        let error = fetch(&service, 2).await.unwrap_err();

        assert!(matches!(
            error,
            ProxyBlobError::Acquire(ref inner)
                if matches!(inner.as_ref(), ChannelAcquireError::SessionAdmission)
        ));
        assert!(error.allows_rails_fallback());
        assert_eq!(fake.upgrades(), 1);
    }

    #[tokio::test]
    async fn not_available_is_negative_cached() {
        let fake =
            FakeWorkhorse::start(Preauth::reject(reqwest::StatusCode::NOT_FOUND), serve(1, 0))
                .await;
        let service = ProxyBlobService::new(Arc::new(fake.client()), &config());

        let first = fetch(&service, 42).await.unwrap_err();
        let second = fetch(&service, 42).await.unwrap_err();

        assert!(first.allows_rails_fallback());
        assert!(matches!(second, ProxyBlobError::NegativeCached));
        assert_eq!(fake.preauth_requests(), 1);
        assert_eq!(fake.upgrades(), 0);
    }

    #[tokio::test]
    async fn negative_cache_expires() {
        let fake =
            FakeWorkhorse::start(Preauth::reject(reqwest::StatusCode::NOT_FOUND), serve(1, 0))
                .await;
        let service = ProxyBlobService::new(Arc::new(fake.client()), &config());

        assert!(fetch(&service, 42).await.is_err());
        tokio::time::sleep(Duration::from_millis(1100)).await;
        service.run_cache_maintenance().await;
        assert!(fetch(&service, 42).await.is_err());

        assert_eq!(fake.preauth_requests(), 2);
    }
}
