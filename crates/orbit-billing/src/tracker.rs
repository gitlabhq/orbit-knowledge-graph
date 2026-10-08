use std::sync::Arc;

use gitlab_client::CloudConnectorTokenCache;
use labkit_events::{BillingEvent, DeliveryFailure, TokenSource};
use opentelemetry::KeyValue;
use orbit_observability::billing::events as spec;
use orbit_server_config::{BillingAuthMode, BillingConfig};
use uuid::Uuid;

use crate::cc_token_source::CloudConnectorTokenSource;
use crate::constants::APP_ID;
use crate::metrics::{
    METRICS, REASON_ABANDONED_AT_SHUTDOWN, REASON_AUTH, REASON_INVALID_EVENT,
    REASON_NON_RETRIABLE_STATUS, REASON_RETRIES_EXHAUSTED, REASON_RETRY_QUEUE_FULL,
    REASON_SERIALIZATION, REASON_UNKNOWN,
};

pub trait BillingTracker: Send + Sync {
    /// Returns the Snowplow event ID assigned to the enqueued event, so callers
    /// can correlate it with delivery-outcome callbacks / logs.
    fn track(&self, event: BillingEvent) -> Result<Uuid, labkit_events::Error>;
}

pub struct SnowplowBillingTracker {
    tracker: Arc<labkit_events::Tracker>,
}

impl SnowplowBillingTracker {
    pub fn from_config(
        config: &BillingConfig,
        cc_token_cache: Option<Arc<CloudConnectorTokenCache>>,
    ) -> Result<Self, labkit_events::Error> {
        let source = Self::token_source(config, cc_token_cache)?;

        let tracker = labkit_events::Tracker::builder(&config.collector_url, APP_ID)
            .batch_size(1)
            .collector_path(labkit_events::AUTH_COLLECTOR_PATH)
            .token_source(source)
            .on_success(Arc::new(|event_ids: &[Uuid]| {
                METRICS.delivered.add(event_ids.len() as u64, &[]);
                tracing::info!(
                    events = event_ids.len(),
                    event_ids = ?event_ids,
                    "billing event delivery: success"
                );
            }))
            .on_failure(Arc::new(|event_ids: &[Uuid], reason: DeliveryFailure| {
                let (reason_label, status) = match reason {
                    DeliveryFailure::NonRetriableStatus(code) => {
                        (REASON_NON_RETRIABLE_STATUS, Some(code))
                    }
                    DeliveryFailure::RetriesExhausted => (REASON_RETRIES_EXHAUSTED, None),
                    DeliveryFailure::Auth => (REASON_AUTH, None),
                    DeliveryFailure::RetryQueueFull => (REASON_RETRY_QUEUE_FULL, None),
                    DeliveryFailure::AbandonedAtShutdown => (REASON_ABANDONED_AT_SHUTDOWN, None),
                    DeliveryFailure::Serialization => (REASON_SERIALIZATION, None),
                    DeliveryFailure::InvalidEvent => (REASON_INVALID_EVENT, None),
                    _ => (REASON_UNKNOWN, None),
                };
                METRICS.delivery_failed.add(
                    event_ids.len() as u64,
                    &[KeyValue::new(spec::labels::REASON, reason_label)],
                );
                tracing::warn!(
                    events = event_ids.len(),
                    event_ids = ?event_ids,
                    reason = reason_label,
                    status = ?status,
                    "billing event delivery: failed"
                );
            }))
            .build()?;

        Ok(Self {
            tracker: Arc::new(tracker),
        })
    }

    pub async fn shutdown(&self) {
        tracing::info!("billing tracker shutdown: draining queued events");
        self.tracker.shutdown().await;
        tracing::info!("billing tracker shutdown: complete");
    }

    fn token_source(
        config: &BillingConfig,
        cc_token_cache: Option<Arc<CloudConnectorTokenCache>>,
    ) -> Result<Arc<dyn TokenSource>, labkit_events::Error> {
        match config.auth_mode {
            BillingAuthMode::Oidc => {
                let oidc_config = labkit_events::oidc::ConfigBuilder::new()
                    .skip_if_unsupported_cloud(true)
                    .build();
                let source = labkit_events::oidc::Source::new(oidc_config)
                    .map_err(|e| labkit_events::Error::Emitter(e.to_string()))?;
                Ok(Arc::new(source))
            }
            BillingAuthMode::CloudConnector => {
                let cache = cc_token_cache.ok_or_else(|| {
                    labkit_events::Error::Emitter(
                        "billing.auth_mode=cloud_connector requires a GitLab client to fetch \
                         the Cloud Connector token"
                            .to_string(),
                    )
                })?;
                Ok(Arc::new(CloudConnectorTokenSource::new(cache)))
            }
        }
    }
}

impl BillingTracker for SnowplowBillingTracker {
    fn track(&self, event: BillingEvent) -> Result<Uuid, labkit_events::Error> {
        self.tracker.track_billing_event(event)
    }
}

#[cfg(any(test, feature = "testkit"))]
#[derive(Default)]
pub struct InMemoryBillingTracker {
    count: std::sync::atomic::AtomicUsize,
}

#[cfg(any(test, feature = "testkit"))]
impl InMemoryBillingTracker {
    pub fn count(&self) -> usize {
        self.count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(any(test, feature = "testkit"))]
impl BillingTracker for InMemoryBillingTracker {
    fn track(&self, _event: BillingEvent) -> Result<Uuid, labkit_events::Error> {
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(Uuid::nil())
    }
}

#[cfg(test)]
pub(crate) struct FailingBillingTracker {
    count: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl FailingBillingTracker {
    pub fn new() -> Self {
        Self {
            count: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    pub fn count(&self) -> usize {
        self.count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
impl BillingTracker for FailingBillingTracker {
    fn track(&self, _event: BillingEvent) -> Result<Uuid, labkit_events::Error> {
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Err(labkit_events::Error::Emitter("test failure".into()))
    }
}

#[cfg(test)]
mod tests {
    use std::future::Future;
    use std::pin::Pin;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use axum::Router;
    use axum::body::Bytes;
    use axum::http::StatusCode;
    use axum::routing::post;
    use gitlab_client::{CloudConnectorToken, CloudConnectorTokenFetcher, GitlabClientError};
    use orbit_server_config::AppConfig;
    use tokio::net::TcpListener;

    use super::*;

    struct StubFetcher;
    impl CloudConnectorTokenFetcher for StubFetcher {
        fn fetch(
            &self,
        ) -> Pin<Box<dyn Future<Output = Result<CloudConnectorToken, GitlabClientError>> + Send + '_>>
        {
            Box::pin(async {
                Ok(CloudConnectorToken {
                    token: "t".into(),
                    exp: i64::MAX,
                })
            })
        }
    }

    fn config(auth_mode: BillingAuthMode) -> BillingConfig {
        BillingConfig {
            enabled: true,
            collector_url: "https://collector.example".into(),
            auth_mode,
            quota: AppConfig::embedded_defaults().billing.quota,
        }
    }

    #[test]
    fn oidc_mode_builds_source_without_a_cache() {
        let source = SnowplowBillingTracker::token_source(&config(BillingAuthMode::Oidc), None);
        assert!(source.is_ok());
    }

    #[test]
    fn cloud_connector_mode_requires_a_cache() {
        let err =
            SnowplowBillingTracker::token_source(&config(BillingAuthMode::CloudConnector), None)
                .err()
                .unwrap();
        assert!(err.to_string().contains("cloud_connector"));
    }

    #[test]
    fn cloud_connector_mode_builds_source_with_a_cache() {
        let cache = Arc::new(CloudConnectorTokenCache::new(Arc::new(StubFetcher)));
        let source = SnowplowBillingTracker::token_source(
            &config(BillingAuthMode::CloudConnector),
            Some(cache),
        );
        assert!(source.is_ok());
    }

    async fn serve(app: Router) -> String {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    async fn slow_collector(delay: Duration) -> (String, Arc<AtomicUsize>) {
        let received = Arc::new(AtomicUsize::new(0));
        let counter = received.clone();
        let app = Router::new().route(
            labkit_events::AUTH_COLLECTOR_PATH,
            post(move |body: Bytes| {
                let counter = counter.clone();
                async move {
                    tokio::time::sleep(delay).await;
                    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
                    let events = payload["data"].as_array().map_or(0, Vec::len);
                    counter.fetch_add(events, Ordering::Relaxed);
                    StatusCode::OK
                }
            }),
        );
        (serve(app).await, received)
    }

    async fn hanging_collector() -> (String, Arc<AtomicUsize>) {
        let requests = Arc::new(AtomicUsize::new(0));
        let counter = requests.clone();
        let app = Router::new().route(
            labkit_events::AUTH_COLLECTOR_PATH,
            post(move || {
                counter.fetch_add(1, Ordering::Relaxed);
                std::future::pending::<StatusCode>()
            }),
        );
        (serve(app).await, requests)
    }

    fn tracker_for(collector_url: String) -> SnowplowBillingTracker {
        let mut config = config(BillingAuthMode::CloudConnector);
        config.collector_url = collector_url;
        let cache = Arc::new(CloudConnectorTokenCache::new(Arc::new(StubFetcher)));
        SnowplowBillingTracker::from_config(&config, Some(cache)).unwrap()
    }

    fn event() -> BillingEvent {
        BillingEvent::builder("orbit", "orbit_query", "SaaS", "request", 1.0)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn shutdown_delivers_queued_events_before_returning() {
        let (url, received) = slow_collector(Duration::from_millis(100)).await;
        let tracker = tracker_for(url);
        for _ in 0..3 {
            tracker.track(event()).unwrap();
        }

        tracker.shutdown().await;

        assert_eq!(received.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn shutdown_yields_to_an_outer_timeout_when_the_collector_hangs() {
        let (url, requests) = hanging_collector().await;
        let tracker = tracker_for(url);
        tracker.track(event()).unwrap();

        let started = Instant::now();
        let outcome = tokio::time::timeout(Duration::from_millis(200), tracker.shutdown()).await;

        assert!(outcome.is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
        assert_eq!(requests.load(Ordering::Relaxed), 1);
    }
}
