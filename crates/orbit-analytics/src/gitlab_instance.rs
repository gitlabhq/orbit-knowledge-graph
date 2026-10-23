use std::sync::Arc;
use std::time::Duration;

use gitlab_client::{GitlabClient, GitlabInstance, InstanceIdentity};
use labkit_events::StructuredEvent;
use orbit_log_fields::warn;
use orbit_server_config::AnalyticsConfig;
use parking_lot::RwLock;

use crate::AnalyticsTracker;

const REFRESH_INTERVAL: Duration = Duration::from_secs(300);

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

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use gitlab_client::InstanceSettings;
    use orbit_server_config::{DeploymentConfig, DeploymentEnvironment, DeploymentKind};

    use super::*;

    #[derive(Default)]
    struct CountingTracker(AtomicUsize);

    impl AnalyticsTracker for CountingTracker {
        fn track(&self, _event: StructuredEvent) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn sent_count(enabled: Option<bool>, instance: &GitlabInstanceWatch) -> usize {
        let counter = Arc::new(CountingTracker::default());
        let config = AnalyticsConfig {
            enabled,
            collector_url: "https://collector.example".to_string(),
            deployment: DeploymentConfig {
                kind: DeploymentKind::SelfManaged,
                environment: DeploymentEnvironment::Production,
            },
        };
        let tracker = ConsentGate::new(counter.clone(), &config, instance);
        tracker.track(StructuredEvent::builder("gkg", "test").build().unwrap());
        counter.0.load(Ordering::Relaxed)
    }

    fn instance_with_product_usage_data(enabled: bool) -> GitlabInstanceWatch {
        GitlabInstanceWatch::fixed(GitlabInstance {
            settings: InstanceSettings {
                product_usage_data_enabled: enabled,
            },
            ..GitlabInstance::default()
        })
    }

    #[test]
    fn unset_config_sends_only_while_gitlab_product_usage_data_is_on() {
        assert_eq!(sent_count(None, &instance_with_product_usage_data(true)), 1);
        assert_eq!(
            sent_count(None, &instance_with_product_usage_data(false)),
            0
        );
        assert_eq!(sent_count(None, &GitlabInstanceWatch::default()), 0);
    }

    #[test]
    fn explicit_config_ignores_the_gitlab_setting() {
        let opted_out = instance_with_product_usage_data(false);

        assert_eq!(sent_count(Some(true), &opted_out), 1);
    }
}
