use std::sync::Arc;
use std::time::Duration;

use gitlab_client::{GitlabClient, GitlabInstance, InstanceIdentity};
use labkit_events::StructuredEvent;
use orbit_log_fields::{info, warn};
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
        let watch = self.clone();
        tokio::spawn(async move {
            loop {
                watch.refresh(&client).await;
                tokio::time::sleep(REFRESH_INTERVAL).await;
            }
        });
    }

    pub async fn refresh(&self, client: &GitlabClient) {
        let instance = match client.instance().await {
            Ok(instance) => instance,
            Err(error) => {
                warn!(
                    expanded(ERROR_MESSAGE) = %error,
                    "failed to refresh the GitLab instance, keeping the last known one"
                );
                return;
            }
        };

        let was_enabled = self.product_usage_data_enabled();
        *self.latest.write() = instance;
        match (was_enabled, self.product_usage_data_enabled()) {
            (false, true) => info!("GitLab product usage data is on, analytics events may be sent"),
            (true, false) => {
                info!("GitLab product usage data is off, analytics events are dropped")
            }
            _ => {}
        }
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

pub fn consenting_tracker(
    tracker: Arc<dyn AnalyticsTracker>,
    config: &AnalyticsConfig,
    instance: &GitlabInstanceWatch,
) -> Arc<dyn AnalyticsTracker> {
    if config.follows_gitlab_setting() {
        Arc::new(GitlabSettingGate {
            tracker,
            instance: instance.clone(),
        })
    } else {
        tracker
    }
}

struct GitlabSettingGate {
    tracker: Arc<dyn AnalyticsTracker>,
    instance: GitlabInstanceWatch,
}

impl AnalyticsTracker for GitlabSettingGate {
    fn track(&self, event: StructuredEvent) {
        if self.instance.product_usage_data_enabled() {
            self.tracker.track(event);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use axum::Router;
    use axum::http::StatusCode;
    use axum::routing::get;
    use orbit_server_config::{
        DeploymentConfig, DeploymentEnvironment, DeploymentKind, GitlabClientConfiguration,
    };

    use super::*;

    #[derive(Default)]
    struct CountingTracker(AtomicUsize);

    impl AnalyticsTracker for CountingTracker {
        fn track(&self, _event: StructuredEvent) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    impl CountingTracker {
        fn count(&self) -> usize {
            self.0.load(Ordering::Relaxed)
        }
    }

    fn analytics_config(enabled: Option<bool>) -> AnalyticsConfig {
        AnalyticsConfig {
            enabled,
            collector_url: "https://collector.example".to_string(),
            deployment: DeploymentConfig {
                kind: DeploymentKind::SelfManaged,
                environment: DeploymentEnvironment::Production,
            },
        }
    }

    async fn gitlab_serving(response: Option<serde_json::Value>) -> GitlabClient {
        let app = Router::new().route(
            "/api/v4/internal/orbit/instance",
            get(move || {
                let response = response.clone();
                async move {
                    match response {
                        Some(body) => Ok(axum::Json(body)),
                        None => Err(StatusCode::NOT_FOUND),
                    }
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        GitlabClient::new(GitlabClientConfiguration {
            base_url: format!("http://{addr}"),
            signing_key: "c2VjcmV0".to_string(),
            resolve_host: None,
        })
        .unwrap()
    }

    fn instance_body(product_usage_data_enabled: bool) -> serde_json::Value {
        serde_json::json!({
            "identity": { "host_name": "gitlab.example.com" },
            "settings": { "product_usage_data_enabled": product_usage_data_enabled },
        })
    }

    fn send_one(tracker: &Arc<dyn AnalyticsTracker>) {
        tracker.track(StructuredEvent::builder("gkg", "test").build().unwrap());
    }

    #[tokio::test]
    async fn unset_config_sends_only_while_gitlab_product_usage_data_is_on() {
        let counter = Arc::new(CountingTracker::default());
        let watch = GitlabInstanceWatch::default();
        let tracker = consenting_tracker(counter.clone(), &analytics_config(None), &watch);

        send_one(&tracker);
        watch
            .refresh(&gitlab_serving(Some(instance_body(true))).await)
            .await;
        send_one(&tracker);
        watch
            .refresh(&gitlab_serving(Some(instance_body(false))).await)
            .await;
        send_one(&tracker);

        assert_eq!(counter.count(), 1);
    }

    #[tokio::test]
    async fn explicit_config_ignores_the_gitlab_setting() {
        let counter = Arc::new(CountingTracker::default());
        let watch = GitlabInstanceWatch::default();
        let tracker = consenting_tracker(counter.clone(), &analytics_config(Some(true)), &watch);

        send_one(&tracker);

        assert_eq!(counter.count(), 1);
    }

    #[tokio::test]
    async fn a_gitlab_without_the_endpoint_clears_the_known_instance() {
        let watch = GitlabInstanceWatch::default();
        watch
            .refresh(&gitlab_serving(Some(instance_body(true))).await)
            .await;

        watch.refresh(&gitlab_serving(None).await).await;

        assert_eq!(watch.identity(), None);
        assert!(!watch.product_usage_data_enabled());
    }
}
