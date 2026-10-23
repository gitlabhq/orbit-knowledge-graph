//! Snowplow analytics for the indexer: per-dispatch `gkg_indexing_completed`
//! events carrying the resource cost of each indexing run, for cost
//! attribution and throughput baselines (see issue #750).

mod context;
mod observer;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use gitlab_client::{GitlabClient, InstanceIdentity};
use orbit_analytics::{AnalyticsTracker, SnowplowAnalyticsTracker};
use orbit_log_fields::{info, warn};
use orbit_server_config::{
    AnalyticsConfig, DeploymentConfig, DeploymentEnvironment, DeploymentKind,
    GitlabClientConfiguration,
};

use crate::observer::IndexingObserver;
pub use observer::SnowplowIndexingObserver;

const INSTANCE_IDENTITY_RETRY_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct IndexingAnalytics {
    tracker: Option<SnowplowAnalyticsTracker>,
    config: Arc<AnalyticsConfig>,
    instance: Arc<OnceLock<InstanceIdentity>>,
}

impl IndexingAnalytics {
    pub fn from_config(config: &AnalyticsConfig) -> Result<Self, labkit_events::Error> {
        Ok(Self {
            tracker: build_tracker(config)?,
            config: Arc::new(config.clone()),
            instance: Arc::default(),
        })
    }

    pub fn disabled() -> Self {
        Self {
            tracker: None,
            config: Arc::new(AnalyticsConfig {
                enabled: false,
                collector_url: String::new(),
                deployment: DeploymentConfig {
                    kind: DeploymentKind::SelfManaged,
                    environment: DeploymentEnvironment::Development,
                },
            }),
            instance: Arc::default(),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.tracker.is_some()
    }

    pub fn observer(&self) -> Option<Box<dyn IndexingObserver>> {
        let tracker: Arc<dyn AnalyticsTracker> = Arc::new(self.tracker.clone()?);
        Some(Box::new(SnowplowIndexingObserver::new(
            tracker,
            Arc::clone(&self.config),
            Arc::clone(&self.instance),
        )))
    }

    pub fn resolve_instance_identity(&self, gitlab: &GitlabClientConfiguration) {
        if !self.is_enabled() {
            return;
        }
        match GitlabClient::new(gitlab.clone()) {
            Ok(client) => {
                tokio::spawn(fetch_instance_identity(client, Arc::clone(&self.instance)));
            }
            Err(error) => warn!(
                expanded(ERROR_MESSAGE) = %error,
                "cannot build GitLab client, indexing events carry no instance identity"
            ),
        }
    }

    pub async fn shutdown(&self) {
        if let Some(tracker) = &self.tracker {
            tracker.shutdown().await;
        }
    }
}

async fn fetch_instance_identity(client: GitlabClient, instance: Arc<OnceLock<InstanceIdentity>>) {
    loop {
        match client.instance_identity().await {
            Ok(Some(identity)) => {
                info!(
                    expanded(HOST) = identity.host_name.as_deref().unwrap_or_default(),
                    "resolved GitLab instance identity for indexing analytics"
                );
                let _ = instance.set(identity);
                return;
            }
            Ok(None) => {
                info!("GitLab serves no instance identity, indexing events carry none");
                return;
            }
            Err(error) => {
                warn!(
                    expanded(ERROR_MESSAGE) = %error,
                    "failed to fetch GitLab instance identity, retrying"
                );
                tokio::time::sleep(INSTANCE_IDENTITY_RETRY_INTERVAL).await;
            }
        }
    }
}

fn build_tracker(
    config: &AnalyticsConfig,
) -> Result<Option<SnowplowAnalyticsTracker>, labkit_events::Error> {
    if !config.enabled {
        return Ok(None);
    }
    if config.collector_url.trim().is_empty() {
        orbit_log_fields::warn!(
            "analytics.enabled=true but analytics.collector_url is empty; indexing analytics disabled"
        );
        return Ok(None);
    }
    SnowplowAnalyticsTracker::from_config(config).map(Some)
}
