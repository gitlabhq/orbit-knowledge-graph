use std::sync::Arc;

use gkg_observability::billing::events as spec;
use gkg_server_config::BillingConfig;
use labkit_events::{BillingEvent, DeliveryFailure};
use opentelemetry::KeyValue;
use uuid::Uuid;

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
    pub fn from_config(config: &BillingConfig) -> Result<Self, labkit_events::Error> {
        let oidc_config = labkit_events::oidc::ConfigBuilder::new()
            .skip_if_unsupported_cloud(true)
            .build();
        let source = labkit_events::oidc::Source::new(oidc_config)
            .map_err(|e| labkit_events::Error::Emitter(e.to_string()))?;

        let tracker = labkit_events::Tracker::builder(&config.collector_url, APP_ID)
            .batch_size(1)
            .collector_path(labkit_events::AUTH_COLLECTOR_PATH)
            .token_source(Arc::new(source))
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
