//! Snowplow analytics for the indexer: per-dispatch `gkg_indexing_completed`
//! events carrying the resource cost of each indexing run, for cost
//! attribution and throughput baselines (see issue #750).

mod context;
mod observer;

use std::sync::Arc;

use gkg_analytics::{AnalyticsTracker, SnowplowAnalyticsTracker};
use gkg_server_config::AnalyticsConfig;

use crate::observer::IndexingObserver;
pub use observer::SnowplowIndexingObserver;

/// The analytics dependencies a handler needs to emit indexing events: an
/// optional tracker (absent when analytics is disabled) plus the deployment
/// config every event's `orbit_common` context is built from. Cloning is
/// cheap — both fields are `Arc`s.
#[derive(Clone)]
pub struct IndexingAnalytics {
    tracker: Option<Arc<dyn AnalyticsTracker>>,
    config: Arc<AnalyticsConfig>,
}

impl IndexingAnalytics {
    /// Build from config, constructing the Snowplow tracker when analytics is
    /// enabled and a collector URL is set. Errors only on a
    /// misconfigured-but-enabled tracker.
    pub fn from_config(config: &AnalyticsConfig) -> Result<Self, labkit_events::Error> {
        Ok(Self {
            tracker: build_tracker(config)?,
            config: Arc::new(config.clone()),
        })
    }

    /// An instance that emits nothing — for tests and disabled deployments.
    pub fn disabled() -> Self {
        Self {
            tracker: None,
            config: Arc::new(AnalyticsConfig::default()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.tracker.is_some()
    }

    /// The Snowplow observer to attach to a run, or `None` when disabled.
    /// Callers `extend` their observer list with this so a disabled instance
    /// contributes nothing.
    pub fn observer(&self) -> Option<Box<dyn IndexingObserver>> {
        let tracker = self.tracker.clone()?;
        Some(Box::new(SnowplowIndexingObserver::new(
            tracker,
            Arc::clone(&self.config),
        )))
    }
}

fn build_tracker(
    config: &AnalyticsConfig,
) -> Result<Option<Arc<dyn AnalyticsTracker>>, labkit_events::Error> {
    if !config.enabled {
        return Ok(None);
    }
    if config.collector_url.trim().is_empty() {
        tracing::warn!(
            "analytics.enabled=true but analytics.collector_url is empty; indexing analytics disabled"
        );
        return Ok(None);
    }
    let tracker = SnowplowAnalyticsTracker::from_config(config)?;
    Ok(Some(Arc::new(tracker)))
}
