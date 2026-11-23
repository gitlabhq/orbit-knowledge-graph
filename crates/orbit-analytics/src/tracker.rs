use std::sync::Arc;
use std::time::Duration;

use labkit_events::StructuredEvent;
use orbit_server_config::AnalyticsConfig;
use tokio::time::{Instant, MissedTickBehavior};

use crate::failure_log::{FAILURE_LOG_INTERVAL, FailureAction, FailureLog};

const APP_ID: &str = "gkg-server";
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

pub trait AnalyticsTracker: Send + Sync {
    fn track(&self, event: StructuredEvent);
}

#[derive(Clone)]
pub struct SnowplowAnalyticsTracker {
    tracker: Arc<labkit_events::Tracker>,
    failure_log: Arc<FailureLog>,
}

impl SnowplowAnalyticsTracker {
    pub fn new(collector_url: &str, app_id: &str) -> Result<Self, labkit_events::Error> {
        let tracker = labkit_events::Tracker::builder(collector_url, app_id).build()?;
        Ok(Self {
            tracker: Arc::new(tracker),
            failure_log: Arc::new(FailureLog::new(FAILURE_LOG_INTERVAL)),
        })
    }

    pub fn from_config(config: &AnalyticsConfig) -> Result<Self, labkit_events::Error> {
        Self::start(&config.collector_url)
    }

    pub fn local_from_config(
        config: &AnalyticsConfig,
    ) -> Result<Option<Self>, labkit_events::Error> {
        let Some(url) = config.local_collector() else {
            if !config.local_collector_url.trim().is_empty() {
                orbit_log_fields::warn!(
                    safe(DEPLOYMENT_TYPE) = <&str>::from(config.deployment.kind),
                    "analytics.local_collector_url is for self-managed only, ignoring it"
                );
            }
            return Ok(None);
        };
        orbit_log_fields::info!(
            expanded(URL) = %url,
            "initializing local analytics event tracker"
        );
        Self::start(url).map(Some)
    }

    fn start(collector_url: &str) -> Result<Self, labkit_events::Error> {
        let tracker = Self::new(collector_url, APP_ID)?;
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
}

impl AnalyticsTracker for SnowplowAnalyticsTracker {
    fn track(&self, event: StructuredEvent) {
        let e = match self.tracker.track_structured_event(event) {
            Ok(_) => {
                if let Some(recovery) = self.failure_log.record_success(std::time::Instant::now) {
                    orbit_log_fields::info!(
                        safe(ERROR_COUNT) = recovery.failures,
                        safe(FAILING_FOR_SECS) = recovery.failing_for_secs,
                        "analytics events are being queued again"
                    );
                }
                return;
            }
            Err(e) => e,
        };
        match self.failure_log.record_failure(std::time::Instant::now()) {
            FailureAction::LogFirst { suppressed } => orbit_log_fields::error!(
                safe(ERROR_TYPE) = e.kind(),
                expanded(ERROR_MESSAGE) = %e,
                safe(SUPPRESSED) = suppressed,
                "failed to track analytics event; repeated failures are summarised at most once per 60s"
            ),
            FailureAction::Suppress => orbit_log_fields::debug!(
                safe(ERROR_TYPE) = e.kind(),
                expanded(ERROR_MESSAGE) = %e,
                "failed to track analytics event"
            ),
            FailureAction::LogSummary {
                suppressed,
                failing_for_secs,
            } => orbit_log_fields::error!(
                safe(ERROR_TYPE) = e.kind(),
                expanded(ERROR_MESSAGE) = %e,
                safe(SUPPRESSED) = suppressed,
                safe(FAILING_FOR_SECS) = failing_for_secs,
                "failed to track analytics event; still failing"
            ),
        }
    }
}

pub struct ComposedAnalyticsTracker {
    trackers: Vec<Arc<dyn AnalyticsTracker>>,
}

impl ComposedAnalyticsTracker {
    pub fn new(trackers: Vec<Arc<dyn AnalyticsTracker>>) -> Self {
        Self { trackers }
    }
}

impl AnalyticsTracker for ComposedAnalyticsTracker {
    fn track(&self, event: StructuredEvent) {
        let Some((last, rest)) = self.trackers.split_last() else {
            return;
        };
        for tracker in rest {
            tracker.track(event.clone());
        }
        last.track(event);
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
