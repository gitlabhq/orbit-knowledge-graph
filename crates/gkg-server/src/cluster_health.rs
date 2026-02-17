use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use health_check::HealthStatus;
use tracing::warn;

use crate::proto::{ClusterStatus, ComponentHealth, GetClusterHealthResponse, ReplicaStatus};
use crate::webserver::InfrastructureHealthClient;

pub struct ClusterHealthChecker {
    version: String,
    health_client: Option<InfrastructureHealthClient>,
}

impl ClusterHealthChecker {
    pub fn new(health_check_url: Option<String>) -> Self {
        let health_client = health_check_url.map(InfrastructureHealthClient::new);

        Self {
            version: option_env!("GKG_VERSION")
                .unwrap_or(env!("CARGO_PKG_VERSION"))
                .to_string(),
            health_client,
        }
    }

    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub async fn get_cluster_health(&self) -> GetClusterHealthResponse {
        match &self.health_client {
            Some(client) => self.fetch_real_health(client).await,
            None => {
                warn!("No health-check service configured, returning stubbed data");
                self.stubbed_cluster_health()
            }
        }
    }

    async fn fetch_real_health(
        &self,
        client: &InfrastructureHealthClient,
    ) -> GetClusterHealthResponse {
        let health_status = client.check_or_unavailable().await;
        self.convert_health_status(health_status)
    }

    fn convert_health_status(&self, status: HealthStatus) -> GetClusterHealthResponse {
        let cluster_status = match status.status {
            health_check::Status::Healthy => ClusterStatus::Healthy,
            health_check::Status::Unhealthy => ClusterStatus::Unhealthy,
        };

        let mut components: Vec<ComponentHealth> = status
            .services
            .into_iter()
            .map(|s| {
                let component_status = match s.status {
                    health_check::Status::Healthy => ClusterStatus::Healthy,
                    health_check::Status::Unhealthy => ClusterStatus::Unhealthy,
                };

                ComponentHealth {
                    name: s.name,
                    status: component_status.into(),
                    replicas: Some(ReplicaStatus {
                        ready: s.ready_replicas,
                        desired: s.desired_replicas,
                    }),
                    metrics: HashMap::new(),
                }
            })
            .collect();

        let clickhouse_status = match status.clickhouse.status {
            health_check::Status::Healthy => ClusterStatus::Healthy,
            health_check::Status::Unhealthy => ClusterStatus::Unhealthy,
        };

        let mut clickhouse_metrics = HashMap::new();
        if let Some(error) = status.clickhouse.error {
            clickhouse_metrics.insert("error".to_string(), error);
        }

        components.push(ComponentHealth {
            name: "clickhouse".to_string(),
            status: clickhouse_status.into(),
            replicas: None,
            metrics: clickhouse_metrics,
        });

        GetClusterHealthResponse {
            status: cluster_status.into(),
            timestamp: Utc::now().to_rfc3339(),
            version: self.version.clone(),
            components,
        }
    }

    fn stubbed_cluster_health(&self) -> GetClusterHealthResponse {
        GetClusterHealthResponse {
            status: ClusterStatus::Healthy.into(),
            timestamp: Utc::now().to_rfc3339(),
            version: self.version.clone(),
            components: vec![
                ComponentHealth {
                    name: "webserver".to_string(),
                    status: ClusterStatus::Healthy.into(),
                    replicas: Some(ReplicaStatus {
                        ready: 1,
                        desired: 1,
                    }),
                    metrics: HashMap::from([("mode".to_string(), "stubbed".to_string())]),
                },
                ComponentHealth {
                    name: "indexer".to_string(),
                    status: ClusterStatus::Healthy.into(),
                    replicas: Some(ReplicaStatus {
                        ready: 1,
                        desired: 1,
                    }),
                    metrics: HashMap::from([("mode".to_string(), "stubbed".to_string())]),
                },
                ComponentHealth {
                    name: "clickhouse".to_string(),
                    status: ClusterStatus::Healthy.into(),
                    replicas: None,
                    metrics: HashMap::from([("mode".to_string(), "stubbed".to_string())]),
                },
            ],
        }
    }
}

impl Default for ClusterHealthChecker {
    fn default() -> Self {
        Self::new(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_stubbed_health_returns_healthy() {
        let checker = ClusterHealthChecker::new(None);
        let response = checker.get_cluster_health().await;

        assert_eq!(response.status, ClusterStatus::Healthy as i32);
        assert!(!response.version.is_empty());
        assert!(!response.timestamp.is_empty());
    }

    #[tokio::test]
    async fn test_stubbed_includes_mode_metric() {
        let checker = ClusterHealthChecker::new(None);
        let response = checker.get_cluster_health().await;

        for component in &response.components {
            assert_eq!(
                component.metrics.get("mode"),
                Some(&"stubbed".to_string()),
                "Component {} should have mode=stubbed",
                component.name
            );
        }
    }

    #[test]
    fn test_default_has_no_health_client() {
        let checker = ClusterHealthChecker::default();
        assert!(checker.health_client.is_none());
    }
}
