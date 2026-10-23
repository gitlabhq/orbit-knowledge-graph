use std::sync::Arc;
use std::time::Duration;

use gitlab_client::{GitlabClient, GitlabInstance, InstanceIdentity};
use labkit_events::StructuredEvent;
use orbit_log_fields::warn;
use orbit_server_config::AnalyticsConfig;
use parking_lot::RwLock;

use crate::AnalyticsTracker;

const REFRESH_INTERVAL: Duration = Duration::from_secs(3600);

#[derive(Clone, Default)]
pub struct GitlabInstanceWatch {
    latest: Arc<RwLock<Option<GitlabInstance>>>,
}

impl GitlabInstanceWatch {
    #[cfg(any(test, feature = "testkit"))]
    pub fn fixed(instance: GitlabInstance) -> Self {
        Self {
            latest: Arc::new(RwLock::new(Some(instance))),
        }
    }

    pub fn spawn_refresh(&self, client: Arc<GitlabClient>) {
        let latest = Arc::clone(&self.latest);
        tokio::spawn(async move {
            loop {
                match client.instance().await {
                    Ok(instance) => *latest.write() = instance,
                    Err(error) => warn!(
                        expanded(ERROR_MESSAGE) = %error,
                        safe(ERROR_TYPE) = <&str>::from(&error),
                        "failed to refresh the GitLab instance, keeping the last known one"
                    ),
                }
                tokio::time::sleep(REFRESH_INTERVAL).await;
            }
        });
    }

    pub fn identity(&self) -> Option<InstanceIdentity> {
        self.latest
            .read()
            .as_ref()
            .map(|instance| instance.identity.clone())
    }

    pub fn product_usage_data_enabled(&self) -> bool {
        self.latest
            .read()
            .as_ref()
            .is_some_and(|instance| instance.settings.product_usage_data_enabled)
    }
}

pub struct ConsentGate {
    tracker: Arc<dyn AnalyticsTracker>,
    enabled: Option<bool>,
    instance: GitlabInstanceWatch,
}

impl ConsentGate {
    pub fn new(
        tracker: Arc<dyn AnalyticsTracker>,
        config: &AnalyticsConfig,
        instance: &GitlabInstanceWatch,
    ) -> Self {
        Self {
            tracker,
            enabled: config.enabled,
            instance: instance.clone(),
        }
    }
}

impl AnalyticsTracker for ConsentGate {
    fn track(&self, event: StructuredEvent) {
        let allowed = self
            .enabled
            .unwrap_or_else(|| self.instance.product_usage_data_enabled());
        if allowed {
            self.tracker.track(event);
        }
    }
}
