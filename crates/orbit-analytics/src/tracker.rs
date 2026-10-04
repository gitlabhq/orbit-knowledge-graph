use std::sync::Arc;
use std::time::Duration;

use labkit_events::StructuredEvent;
use orbit_server_config::AnalyticsConfig;
use tokio::time::{Instant, MissedTickBehavior};

const APP_ID: &str = "gkg-server";
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

pub trait AnalyticsTracker: Send + Sync {
    fn track(&self, event: StructuredEvent);
}

#[derive(Clone)]
pub struct SnowplowAnalyticsTracker {
    tracker: Arc<labkit_events::Tracker>,
}

impl SnowplowAnalyticsTracker {
    pub fn new(collector_url: &str, app_id: &str) -> Result<Self, labkit_events::Error> {
        let tracker = labkit_events::Tracker::builder(collector_url, app_id).build()?;
        Ok(Self {
            tracker: Arc::new(tracker),
        })
    }

    pub fn from_config(config: &AnalyticsConfig) -> Result<Self, labkit_events::Error> {
        let tracker = Self::new(&config.collector_url, APP_ID)?;
        tracker.spawn_periodic_flush();
        Ok(tracker)
    }

    fn spawn_periodic_flush(&self) {
        let tracker = Arc::downgrade(&self.tracker);
        tokio::spawn(async move {
            let mut ticker =
                tokio::time::interval_at(Instant::now() + FLUSH_INTERVAL, FLUSH_INTERVAL);
            ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let Some(tracker) = tracker.upgrade() else {
                    break;
                };
                tracker.flush();
            }
        });
    }

    pub fn flush(&self) {
        self.tracker.flush();
    }

    pub async fn shutdown(&self) {
        self.tracker.shutdown().await;
    }

    pub async fn drain(&self) {
        if tokio::time::timeout(DRAIN_TIMEOUT, self.shutdown())
            .await
            .is_err()
        {
            tracing::warn!("analytics drain timed out; dropping buffered events");
        }
    }
}

impl AnalyticsTracker for SnowplowAnalyticsTracker {
    fn track(&self, event: StructuredEvent) {
        if let Err(e) = self.tracker.track_structured_event(event) {
            tracing::error!(error = %e, "failed to track analytics event");
        }
    }
}

#[cfg(feature = "testkit")]
pub struct InMemoryAnalyticsTracker {
    events: parking_lot::Mutex<Vec<StructuredEvent>>,
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

    pub fn drain(&self) -> Vec<StructuredEvent> {
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
    fn track(&self, event: StructuredEvent) {
        self.events.lock().push(event);
    }
}
