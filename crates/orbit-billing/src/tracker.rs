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
    METRICS, REASON_AUTH, REASON_NON_RETRIABLE_STATUS, REASON_RETRIES_EXHAUSTED, REASON_UNKNOWN,
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

#[cfg(test)]
pub(crate) struct InMemoryBillingTracker {
    count: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl InMemoryBillingTracker {
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

    use gitlab_client::{CloudConnectorToken, CloudConnectorTokenFetcher, GitlabClientError};

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
                    expires_at: i64::MAX,
                })
            })
        }
    }

    fn config(auth_mode: BillingAuthMode) -> BillingConfig {
        BillingConfig {
            enabled: true,
            collector_url: "https://collector.example".into(),
            auth_mode,
            quota: Default::default(),
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
}
