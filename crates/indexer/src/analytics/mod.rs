//! Snowplow analytics for the indexer: per-dispatch `gkg_indexing_completed`
//! events carrying the resource cost of each indexing run, for cost
//! attribution and throughput baselines (see issue #750).

mod context;
mod observer;

use std::sync::Arc;

use gitlab_client::GitlabClient;
use orbit_analytics::{ConsentGate, GitlabInstanceWatch, SnowplowAnalyticsTracker};
use orbit_log_fields::warn;
use orbit_server_config::{
    AnalyticsConfig, DeploymentConfig, DeploymentEnvironment, DeploymentKind,
};

use crate::observer::IndexingObserver;
pub use observer::SnowplowIndexingObserver;

#[derive(Clone)]
pub struct IndexingAnalytics {
    tracker: Option<SnowplowAnalyticsTracker>,
    config: Arc<AnalyticsConfig>,
    instance: GitlabInstanceWatch,
}

impl IndexingAnalytics {
    pub fn from_config(config: &AnalyticsConfig) -> Result<Self, labkit_events::Error> {
        Ok(Self {
            tracker: build_tracker(config)?,
            config: Arc::new(config.clone()),
            instance: GitlabInstanceWatch::default(),
        })
    }

    pub fn disabled() -> Self {
        Self {
            tracker: None,
            config: Arc::new(AnalyticsConfig {
                enabled: Some(false),
                collector_url: String::new(),
                deployment: DeploymentConfig {
                    kind: DeploymentKind::SelfManaged,
                    environment: DeploymentEnvironment::Development,
                },
            }),
            instance: GitlabInstanceWatch::default(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.tracker.is_some()
    }

    pub fn observer(&self) -> Option<Box<dyn IndexingObserver>> {
        let tracker = ConsentGate::new(
            Arc::new(self.tracker.clone()?),
            &self.config,
            &self.instance,
        );
        Some(Box::new(SnowplowIndexingObserver::new(
            Arc::new(tracker),
            Arc::clone(&self.config),
            self.instance.clone(),
        )))
    }

    pub fn watch_gitlab_instance(&self, client: Arc<GitlabClient>) {
        if self.is_enabled() {
            self.instance.spawn_refresh(client);
        }
    }

    pub async fn shutdown(&self) {
        if let Some(tracker) = &self.tracker {
            tracker.shutdown().await;
        }
    }
}

fn build_tracker(
    config: &AnalyticsConfig,
) -> Result<Option<SnowplowAnalyticsTracker>, labkit_events::Error> {
    if !config.may_send_events() {
        return Ok(None);
    }
    if config.collector_url.trim().is_empty() {
        if config.enabled == Some(true) {
            warn!(
                "analytics.enabled=true but analytics.collector_url is empty; indexing analytics disabled"
            );
        }
        return Ok(None);
    }
    SnowplowAnalyticsTracker::from_config(config).map(Some)
}
