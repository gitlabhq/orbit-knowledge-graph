use std::sync::Arc;
use std::time::Duration;

use gitlab_client::{GitlabClient, GitlabInstance, InstanceIdentity};
use labkit_events::StructuredEvent;
use orbit_log_fields::warn;
use orbit_server_config::AnalyticsConfig;
use parking_lot::RwLock;

use crate::AnalyticsTracker;

const REFRESH_INTERVAL: Duration = Duration::from_secs(3600);
const RETRY_INTERVAL: Duration = Duration::from_secs(60);

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
        let latest = Arc::downgrade(&self.latest);
        tokio::spawn(async move {
            loop {
                let wait = {
                    let Some(latest) = latest.upgrade() else {
                        break;
                    };
                    match client.instance().await {
                        Ok(instance) => {
                            *latest.write() = instance;
                            REFRESH_INTERVAL
                        }
                        Err(error) => {
                            warn!(
                                expanded(ERROR_MESSAGE) = %error,
                                safe(ERROR_TYPE) = <&str>::from(&error),
                                "failed to refresh the GitLab instance, keeping the last known one"
                            );
                            RETRY_INTERVAL
                        }
                    }
                };
                tokio::time::sleep(wait).await;
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
    use labkit_events::StructuredEvent;
    use orbit_server_config::{
        AnalyticsConfig, DeploymentConfig, DeploymentEnvironment, DeploymentKind,
    };
    use parking_lot::Mutex;

    use super::*;
    use crate::ComposedAnalyticsTracker;

    #[derive(Default)]
    struct Recorder(Mutex<usize>);

    impl AnalyticsTracker for Recorder {
        fn track(&self, _event: StructuredEvent) {
            *self.0.lock() += 1;
        }
    }

    impl Recorder {
        fn count(&self) -> usize {
            *self.0.lock()
        }
    }

    fn config(enabled: Option<bool>) -> AnalyticsConfig {
        AnalyticsConfig {
            enabled,
            collector_url: "https://collector.example".into(),
            local_collector_url: "http://dip.example".into(),
            deployment: DeploymentConfig {
                kind: DeploymentKind::SelfManaged,
                environment: DeploymentEnvironment::Production,
            },
        }
    }

    fn send(enabled: Option<bool>, with_gitlab: bool, with_local: bool) -> (usize, usize) {
        let gitlab = Arc::new(Recorder::default());
        let local = Arc::new(Recorder::default());
        // The default watch has no instance yet, so product usage data reads as off.
        let mut trackers: Vec<Arc<dyn AnalyticsTracker>> = Vec::new();
        if with_gitlab {
            trackers.push(Arc::new(ConsentGate::new(
                gitlab.clone(),
                &config(enabled),
                &GitlabInstanceWatch::default(),
            )));
        }
        if with_local {
            trackers.push(local.clone());
        }
        ComposedAnalyticsTracker::new(trackers).track(
            StructuredEvent::builder("gkg", "gkg_query_executed")
                .build()
                .unwrap(),
        );
        (gitlab.count(), local.count())
    }

    #[test]
    fn local_gets_every_event_whatever_enabled_says() {
        assert_eq!(send(None, true, true), (0, 1));
        assert_eq!(send(Some(false), true, true), (0, 1));
        assert_eq!(send(Some(true), true, true), (1, 1));
    }

    #[test]
    fn gitlab_collector_keeps_todays_rules_without_a_local_one() {
        assert_eq!(send(None, true, false), (0, 0));
        assert_eq!(send(Some(true), true, false), (1, 0));
    }

    #[test]
    fn local_only() {
        assert_eq!(send(Some(false), false, true), (0, 1));
    }
}
