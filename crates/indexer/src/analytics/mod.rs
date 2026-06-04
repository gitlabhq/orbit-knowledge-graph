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

#[derive(Clone)]
pub struct IndexingAnalytics {
    tracker: Option<Arc<dyn AnalyticsTracker>>,
    config: Arc<AnalyticsConfig>,
}

impl IndexingAnalytics {
    pub fn from_config(config: &AnalyticsConfig) -> Result<Self, labkit_events::Error> {
        Ok(Self {
            tracker: build_tracker(config)?,
            config: Arc::new(config.clone()),
        })
    }

    pub fn disabled() -> Self {
        Self {
            tracker: None,
            config: Arc::new(AnalyticsConfig::default()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.tracker.is_some()
    }

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
