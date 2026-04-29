use std::sync::Arc;

use gkg_server_config::BillingConfig;
use labkit_events::BillingEvent;

use super::constants::APP_ID;
use super::metrics::METRICS;

pub trait BillingTracker: Send + Sync {
    fn track(&self, event: BillingEvent);
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
            .token_source(Arc::new(source))
            .build()?;

        Ok(Self {
            tracker: Arc::new(tracker),
        })
    }
}

impl BillingTracker for SnowplowBillingTracker {
    fn track(&self, event: BillingEvent) {
        if let Err(e) = self.tracker.track_billing_event(event) {
            let correlation_id = labkit::correlation::current()
                .map(|id| id.as_str().to_string())
                .unwrap_or_default();
            tracing::error!(
                error = %e,
                correlation_id = %correlation_id,
                "failed to track billing event"
            );
            METRICS.track_errors.add(1, &[]);
        }
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
    fn track(&self, _event: BillingEvent) {
        self.count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }
}
