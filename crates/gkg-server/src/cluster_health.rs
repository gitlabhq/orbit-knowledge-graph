use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use health_check::HealthStatus;
use toon_format::{EncodeOptions, encode};
use tracing::warn;

use crate::proto::{
    ClusterStatus, ComponentHealth, GetClusterHealthResponse, ReplicaStatus, ResponseFormat,
    StructuredClusterHealth, get_cluster_health_response,
};
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

    pub async fn get_cluster_health(&self, format: i32) -> GetClusterHealthResponse {
        let structured = match &self.health_client {
            Some(client) => self.fetch_real_health(client).await,
            None => {
                warn!("No health-check service configured, returning stubbed data");
                self.stubbed_cluster_health()
            }
        };

        if format == ResponseFormat::Llm as i32 {
            let text = Self::format_health_as_toon(&structured);
            GetClusterHealthResponse {
                content: Some(get_cluster_health_response::Content::FormattedText(text)),
            }
        } else {
            GetClusterHealthResponse {
                content: Some(get_cluster_health_response::Content::Structured(structured)),
            }
        }
    }

    async fn fetch_real_health(
        &self,
        client: &InfrastructureHealthClient,
    ) -> StructuredClusterHealth {
        let health_status = client.check_or_unavailable().await;
        self.convert_health_status(health_status)
    }

    fn convert_health_status(&self, status: HealthStatus) -> StructuredClusterHealth {
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

        StructuredClusterHealth {
            status: cluster_status.into(),
            timestamp: Utc::now().to_rfc3339(),
            version: self.version.clone(),
            components,
        }
    }

    fn stubbed_cluster_health(&self) -> StructuredClusterHealth {
        StructuredClusterHealth {
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

    fn format_health_as_toon(health: &StructuredClusterHealth) -> String {
        use serde::Serialize;

        #[derive(Serialize)]
        struct HealthToon {
            status: String,
            timestamp: String,
            version: String,
            components: Vec<ComponentToon>,
        }

        #[derive(Serialize)]
        struct ComponentToon {
            name: String,
            status: String,
            #[serde(skip_serializing_if = "Option::is_none")]
            replicas: Option<String>,
            #[serde(skip_serializing_if = "HashMap::is_empty")]
            metrics: HashMap<String, String>,
        }

        fn status_name(val: i32) -> String {
            match ClusterStatus::try_from(val) {
                Ok(ClusterStatus::Healthy) => "healthy".to_string(),
                Ok(ClusterStatus::Degraded) => "degraded".to_string(),
                Ok(ClusterStatus::Unhealthy) => "unhealthy".to_string(),
                _ => "unknown".to_string(),
            }
        }

        let toon = HealthToon {
            status: status_name(health.status),
            timestamp: health.timestamp.clone(),
            version: health.version.clone(),
            components: health
                .components
                .iter()
                .map(|c| ComponentToon {
                    name: c.name.clone(),
                    status: status_name(c.status),
                    replicas: c
                        .replicas
                        .as_ref()
                        .map(|r| format!("{}/{}", r.ready, r.desired)),
                    metrics: c.metrics.clone(),
                })
                .collect(),
        };

        let options = EncodeOptions::default();
        encode(&toon, &options).unwrap_or_else(|_| format!("status:{}", toon.status))
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
    async fn test_stubbed_health_returns_healthy_structured() {
        let checker = ClusterHealthChecker::new(None);
        let response = checker.get_cluster_health(ResponseFormat::Raw as i32).await;

        match response.content {
            Some(get_cluster_health_response::Content::Structured(s)) => {
                assert_eq!(s.status, ClusterStatus::Healthy as i32);
                assert!(!s.version.is_empty());
                assert!(!s.timestamp.is_empty());
            }
            _ => panic!("Expected structured response"),
        }
    }

    #[tokio::test]
    async fn test_stubbed_health_returns_formatted_text_for_llm() {
        let checker = ClusterHealthChecker::new(None);
        let response = checker.get_cluster_health(ResponseFormat::Llm as i32).await;

        match response.content {
            Some(get_cluster_health_response::Content::FormattedText(text)) => {
                assert!(text.contains("healthy"));
                assert!(text.contains("webserver"));
            }
            _ => panic!("Expected formatted text response"),
        }
    }

    #[tokio::test]
    async fn test_stubbed_includes_mode_metric() {
        let checker = ClusterHealthChecker::new(None);
        let response = checker.get_cluster_health(ResponseFormat::Raw as i32).await;

        match response.content {
            Some(get_cluster_health_response::Content::Structured(s)) => {
                for component in &s.components {
                    assert_eq!(
                        component.metrics.get("mode"),
                        Some(&"stubbed".to_string()),
                        "Component {} should have mode=stubbed",
                        component.name
                    );
                }
            }
            _ => panic!("Expected structured response"),
        }
    }

    #[tokio::test]
    async fn test_stubbed_health_structured_has_components() {
        let checker = ClusterHealthChecker::new(None);
        let response = checker.get_cluster_health(ResponseFormat::Raw as i32).await;

        match response.content {
            Some(get_cluster_health_response::Content::Structured(s)) => {
                assert!(!s.components.is_empty(), "Should have components");
                let names: Vec<&str> = s.components.iter().map(|c| c.name.as_str()).collect();
                assert!(names.contains(&"webserver"), "Should include webserver");
                assert!(names.contains(&"clickhouse"), "Should include clickhouse");
            }
            _ => panic!("Expected structured response"),
        }
    }

    #[tokio::test]
    async fn test_llm_format_contains_all_components() {
        let checker = ClusterHealthChecker::new(None);
        let response = checker.get_cluster_health(ResponseFormat::Llm as i32).await;

        match response.content {
            Some(get_cluster_health_response::Content::FormattedText(text)) => {
                assert!(
                    text.contains("clickhouse"),
                    "TOON should mention clickhouse"
                );
                assert!(text.contains("indexer"), "TOON should mention indexer");
            }
            _ => panic!("Expected formatted text response"),
        }
    }

    #[test]
    fn test_format_health_as_toon_status_mapping() {
        let health = StructuredClusterHealth {
            status: ClusterStatus::Degraded.into(),
            timestamp: "2026-03-03T00:00:00Z".to_string(),
            version: "0.6.0".to_string(),
            components: vec![],
        };

        let text = ClusterHealthChecker::format_health_as_toon(&health);
        assert!(text.contains("degraded"), "Should map degraded status");
    }

    #[test]
    fn test_format_health_as_toon_replicas() {
        let health = StructuredClusterHealth {
            status: ClusterStatus::Healthy.into(),
            timestamp: "2026-03-03T00:00:00Z".to_string(),
            version: "0.6.0".to_string(),
            components: vec![ComponentHealth {
                name: "webserver".to_string(),
                status: ClusterStatus::Healthy.into(),
                replicas: Some(ReplicaStatus {
                    ready: 2,
                    desired: 3,
                }),
                metrics: HashMap::new(),
            }],
        };

        let text = ClusterHealthChecker::format_health_as_toon(&health);
        assert!(
            text.contains("2/3"),
            "Should format replicas as ready/desired"
        );
    }

    #[test]
    fn test_default_has_no_health_client() {
        let checker = ClusterHealthChecker::default();
        assert!(checker.health_client.is_none());
    }
}
