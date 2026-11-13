//! Snowplow analytics for the indexer: per-dispatch `gkg_indexing_completed`
//! events carrying the resource cost of each indexing run, for cost
//! attribution and throughput baselines (see issue #750), and the dispatcher's
//! daily `orbit_graph_stats` snapshot.

mod context;
mod graph_stats;
mod observer;

use std::sync::Arc;

use gitlab_client::GitlabClient;
use orbit_analytics::{
    AnalyticsTracker, ComposedAnalyticsTracker, ConsentGate, GitlabInstanceWatch,
    SnowplowAnalyticsTracker,
};
use orbit_log_fields::{info, warn};
use orbit_server_config::{
    AnalyticsConfig, DeploymentConfig, DeploymentEnvironment, DeploymentKind,
};

use crate::observer::IndexingObserver;
pub use graph_stats::{GraphStats, GraphStatsReporter};
pub use observer::SnowplowIndexingObserver;

#[derive(Clone)]
pub struct IndexingAnalytics {
    tracker: Option<SnowplowAnalyticsTracker>,
    local: Option<SnowplowAnalyticsTracker>,
    config: Arc<AnalyticsConfig>,
    instance: GitlabInstanceWatch,
}

impl IndexingAnalytics {
    pub fn from_config(config: &AnalyticsConfig) -> Result<Self, labkit_events::Error> {
        Ok(Self {
            tracker: build_tracker(config)?,
            local: SnowplowAnalyticsTracker::local_from_config(config)?,
            config: Arc::new(config.clone()),
            instance: GitlabInstanceWatch::default(),
        })
    }

    pub fn disabled() -> Self {
        Self {
            tracker: None,
            local: None,
            config: Arc::new(AnalyticsConfig {
                enabled: Some(false),
                collector_url: String::new(),
                local_collector_url: String::new(),
                deployment: DeploymentConfig {
                    kind: DeploymentKind::SelfManaged,
                    environment: DeploymentEnvironment::Development,
                },
            }),
            instance: GitlabInstanceWatch::default(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.tracker.is_some() || self.local.is_some()
    }

    pub fn observer(&self) -> Option<Box<dyn IndexingObserver>> {
        let tracker = self.composed_tracker()?;
        Some(Box::new(SnowplowIndexingObserver::new(
            tracker,
            Arc::clone(&self.config),
            self.instance.clone(),
        )))
    }

    pub fn graph_stats_reporter(&self) -> Option<GraphStatsReporter> {
        let tracker = self.composed_tracker()?;
        Some(GraphStatsReporter::new(
            tracker,
            Arc::clone(&self.config),
            self.instance.clone(),
        ))
    }

    /// The GitLab collector behind the consent gate, plus the local collector, which gets every event.
    fn composed_tracker(&self) -> Option<Arc<dyn AnalyticsTracker>> {
        if !self.is_enabled() {
            return None;
        }
        let mut trackers: Vec<Arc<dyn AnalyticsTracker>> = Vec::new();
        if let Some(tracker) = &self.tracker {
            trackers.push(Arc::new(ConsentGate::new(
                Arc::new(tracker.clone()),
                &self.config,
                &self.instance,
            )));
        }
        if let Some(local) = &self.local {
            trackers.push(Arc::new(local.clone()));
        }
        Some(Arc::new(ComposedAnalyticsTracker::new(trackers)))
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
        if let Some(tracker) = &self.local {
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
            if config.local_collector().is_some() {
                warn!(
                    "analytics.enabled=true but analytics.collector_url is empty; indexing analytics go to analytics.local_collector_url only"
                );
            } else {
                warn!(
                    "analytics.enabled=true but analytics.collector_url is empty; indexing analytics disabled"
                );
            }
        }
        return Ok(None);
    }
    info!(
        expanded(URL) = %config.collector_url,
        "initializing indexing analytics event tracker"
    );
    SnowplowAnalyticsTracker::from_config(config).map(Some)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(kind: DeploymentKind) -> AnalyticsConfig {
        AnalyticsConfig {
            enabled: Some(false),
            collector_url: String::new(),
            local_collector_url: "http://dip.example".into(),
            deployment: DeploymentConfig {
                kind,
                environment: DeploymentEnvironment::Production,
            },
        }
    }

    #[test]
    fn local_collector_is_ignored_off_self_managed() {
        for kind in [DeploymentKind::Com, DeploymentKind::Dedicated] {
            let analytics = IndexingAnalytics::from_config(&config(kind)).unwrap();
            assert!(!analytics.is_enabled(), "{kind:?}");
        }
    }
}
