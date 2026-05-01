use std::sync::Arc;

use gkg_server_config::AnalyticsConfig;
use labkit_events::gkg::GkgEvent;

const APP_ID: &str = "gkg-server";

pub trait AnalyticsTracker: Send + Sync {
    fn track(&self, event: GkgEvent);
}

pub struct SnowplowAnalyticsTracker {
    tracker: Arc<labkit_events::Tracker>,
}

impl SnowplowAnalyticsTracker {
    pub fn from_config(config: &AnalyticsConfig) -> Result<Self, labkit_events::Error> {
        let tracker = labkit_events::Tracker::builder(&config.collector_url, APP_ID)
            .batch_size(1)
            .build()?;
        Ok(Self {
            tracker: Arc::new(tracker),
        })
    }
}

impl AnalyticsTracker for SnowplowAnalyticsTracker {
    fn track(&self, event: GkgEvent) {
        if let Err(e) = self.tracker.track_gkg_event(event) {
            tracing::error!(error = %e, "failed to track gkg analytics event");
        }
    }
}

#[cfg(feature = "testkit")]
pub struct InMemoryAnalyticsTracker {
    events: parking_lot::Mutex<Vec<GkgEvent>>,
}

#[cfg(feature = "testkit")]
impl InMemoryAnalyticsTracker {
    pub fn new() -> Self {
        Self {
            events: parking_lot::Mutex::new(Vec::new()),
        }
    }

    pub fn count(&self) -> usize {
        self.events.lock().len()
    }

    pub fn drain(&self) -> Vec<GkgEvent> {
        std::mem::take(&mut *self.events.lock())
    }
}

#[cfg(feature = "testkit")]
impl Default for InMemoryAnalyticsTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "testkit")]
impl AnalyticsTracker for InMemoryAnalyticsTracker {
    fn track(&self, event: GkgEvent) {
        self.events.lock().push(event);
    }
}
