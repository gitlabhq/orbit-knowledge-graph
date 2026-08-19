use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use clickhouse_client::ArrowClickHouseClient;
use gitlab_client::{GitlabClient, GitlabClientError};
use health_check::HealthStatus;
use indexer::schema::version::read_migrating_version;
use tokio::time::timeout;
use toon_format::{EncodeOptions, encode};
use tracing::warn;

use crate::proto::{
    ClusterStatus, ComponentHealth, GetClusterHealthResponse, ReplicaStatus, ResponseFormat,
    StructuredClusterHealth, get_cluster_health_response,
};
use crate::webserver::InfrastructureHealthClient;

const GITLAB_HEALTH_CHECK_PROJECT_ID: i64 = 1;
const GITLAB_HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(2);

pub struct ClusterHealthChecker {
    version: String,
    health_client: Option<InfrastructureHealthClient>,
    graph_client: Option<ArrowClickHouseClient>,
    gitlab_client: Option<Arc<GitlabClient>>,
}

impl ClusterHealthChecker {
    pub fn new(
        health_check_url: Option<String>,
        graph_client: Option<ArrowClickHouseClient>,
        gitlab_client: Option<Arc<GitlabClient>>,
    ) -> Self {
        let health_client = health_check_url.map(InfrastructureHealthClient::new);

        Self {
            version: orbit_utils::version::get().to_string(),
            health_client,
            graph_client,
            gitlab_client,
        }
    }

    pub fn into_arc(self) -> Arc<Self> {
        Arc::new(self)
    }

    pub async fn get_cluster_health(&self, format: i32) -> GetClusterHealthResponse {
        let mut structured = match &self.health_client {
            Some(client) => self.fetch_real_health(client).await,
            None => {
                warn!("No health-check service configured, returning stubbed data");
                self.stubbed_cluster_health()
            }
        };
        self.overlay_gitlab_health(&mut structured).await;

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
        let services_only_failure = only_services_unhealthy(&health_status);
        let mut structured = self.convert_health_status(health_status);

        if services_only_failure {
            self.overlay_active_migration(&mut structured).await;
        }

        structured
    }

    async fn overlay_active_migration(&self, structured: &mut StructuredClusterHealth) {
        let Some(graph) = &self.graph_client else {
            return;
        };

        match read_migrating_version(graph).await {
            Ok(Some(version)) => apply_migration_status(structured, version),
            Ok(None) => {}
            Err(error) => warn!(
                %error,
                "failed to read migrating schema version; leaving cluster health unhealthy"
            ),
        }
    }

    async fn overlay_gitlab_health(&self, structured: &mut StructuredClusterHealth) {
        let Some(client) = &self.gitlab_client else {
            return;
        };

        let result = timeout(
            GITLAB_HEALTH_CHECK_TIMEOUT,
            client.project_info(GITLAB_HEALTH_CHECK_PROJECT_ID),
        )
        .await;

        match result {
            Ok(Ok(_) | Err(GitlabClientError::NotFound(_))) => {
                apply_gitlab_status(structured, ClusterStatus::Healthy, None);
            }
            Ok(Err(error)) => {
                warn!(%error, "GitLab health check failed");
                apply_gitlab_status(
                    structured,
                    ClusterStatus::Unhealthy,
                    Some(error.to_string()),
                );
            }
            Err(_) => {
                warn!(
                    timeout_seconds = GITLAB_HEALTH_CHECK_TIMEOUT.as_secs(),
                    "GitLab health check timed out"
                );
                apply_gitlab_status(
                    structured,
                    ClusterStatus::Unhealthy,
                    Some(format!(
                        "GitLab health check timed out after {} seconds",
                        GITLAB_HEALTH_CHECK_TIMEOUT.as_secs()
                    )),
                );
            }
        }
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

                let kind = match s.kind {
                    health_check::ResourceKind::Deployment => "Deployment",
                    health_check::ResourceKind::StatefulSet => "StatefulSet",
                };

                ComponentHealth {
                    name: s.name,
                    status: component_status.into(),
                    replicas: Some(ReplicaStatus {
                        ready: s.ready_replicas,
                        desired: s.desired_replicas,
                    }),
                    metrics: HashMap::from([
                        ("namespace".to_string(), s.namespace),
                        ("kind".to_string(), kind.to_string()),
                    ]),
                }
            })
            .collect();

        for ch in status.clickhouse {
            let ch_status = match ch.status {
                health_check::Status::Healthy => ClusterStatus::Healthy,
                health_check::Status::Unhealthy => ClusterStatus::Unhealthy,
            };

            let mut metrics = HashMap::new();
            if let Some(error) = ch.error {
                metrics.insert("error".to_string(), error);
            }

            components.push(ComponentHealth {
                name: ch.name,
                status: ch_status.into(),
                replicas: None,
                metrics,
            });
        }

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
                Ok(ClusterStatus::Migrating) => "migrating".to_string(),
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
        encode(&toon, &options).unwrap_or_else(|e| {
            tracing::warn!(error = %e, "Failed to encode health as TOON, falling back");
            format!("status:{}", toon.status)
        })
    }
}

fn only_services_unhealthy(status: &HealthStatus) -> bool {
    matches!(status.status, health_check::Status::Unhealthy)
        && status
            .services
            .iter()
            .any(|s| matches!(s.status, health_check::Status::Unhealthy))
        && !status.clickhouse.is_empty()
        && status
            .clickhouse
            .iter()
            .all(|c| matches!(c.status, health_check::Status::Healthy))
}

fn apply_migration_status(status: &mut StructuredClusterHealth, migrating_version: u32) {
    status.status = ClusterStatus::Migrating.into();
    status.components.push(ComponentHealth {
        name: "schema_migration".to_string(),
        status: ClusterStatus::Migrating.into(),
        replicas: None,
        metrics: HashMap::from([(
            "migrating_version".to_string(),
            migrating_version.to_string(),
        )]),
    });
}

fn apply_gitlab_status(
    status: &mut StructuredClusterHealth,
    component_status: ClusterStatus,
    error: Option<String>,
) {
    let metrics = error
        .map(|error| HashMap::from([("error".to_string(), error)]))
        .unwrap_or_default();

    status.components.push(ComponentHealth {
        name: "gitlab".to_string(),
        status: component_status.into(),
        replicas: None,
        metrics,
    });
}

impl Default for ClusterHealthChecker {
    fn default() -> Self {
        Self::new(None, None, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode as HttpStatus;
    use axum::response::IntoResponse as _;
    use axum::{Json, Router, routing::get};
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use health_check::{
        ComponentHealth as HcComponentHealth, HealthStatus, ResourceKind, ServiceHealth, Status,
    };
    use orbit_server_config::GitlabClientConfiguration;
    use tokio::net::TcpListener;

    fn install_crypto_provider() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    }

    async fn start_mock_sidecar(health: HealthStatus) -> String {
        install_crypto_provider();
        let app = Router::new().route(
            "/health",
            get(move || {
                let h = health.clone();
                async move { Json(h) }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        format!("http://{addr}")
    }

    async fn start_mock_gitlab(status: HttpStatus) -> Arc<GitlabClient> {
        install_crypto_provider();
        let app = Router::new().route(
            "/api/v4/internal/orbit/project/{id}/info",
            get(move || async move {
                if status == HttpStatus::OK {
                    Json(serde_json::json!({
                        "project_id": GITLAB_HEALTH_CHECK_PROJECT_ID,
                        "default_branch": "main"
                    }))
                    .into_response()
                } else {
                    status.into_response()
                }
            }),
        );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        Arc::new(
            GitlabClient::new(GitlabClientConfiguration {
                base_url: format!("http://{addr}"),
                signing_key: BASE64.encode(b"test-secret-that-is-long-enough!"),
                resolve_host: None,
            })
            .unwrap(),
        )
    }

    fn healthy_sidecar_response() -> HealthStatus {
        HealthStatus {
            status: Status::Healthy,
            services: vec![
                ServiceHealth {
                    name: "webserver".to_string(),
                    namespace: "gkg".to_string(),
                    kind: ResourceKind::Deployment,
                    status: Status::Healthy,
                    ready_replicas: 2,
                    desired_replicas: 2,
                },
                ServiceHealth {
                    name: "indexer".to_string(),
                    namespace: "gkg".to_string(),
                    kind: ResourceKind::Deployment,
                    status: Status::Healthy,
                    ready_replicas: 1,
                    desired_replicas: 1,
                },
            ],
            clickhouse: vec![HcComponentHealth {
                name: "clickhouse".to_string(),
                status: Status::Healthy,
                error: None,
            }],
        }
    }

    fn degraded_sidecar_response() -> HealthStatus {
        HealthStatus {
            status: Status::Unhealthy,
            services: vec![ServiceHealth {
                name: "indexer".to_string(),
                namespace: "gkg".to_string(),
                kind: ResourceKind::Deployment,
                status: Status::Unhealthy,
                ready_replicas: 0,
                desired_replicas: 2,
            }],
            clickhouse: vec![HcComponentHealth {
                name: "clickhouse".to_string(),
                status: Status::Healthy,
                error: None,
            }],
        }
    }

    fn extract_structured(response: GetClusterHealthResponse) -> StructuredClusterHealth {
        match response.content {
            Some(get_cluster_health_response::Content::Structured(s)) => s,
            _ => panic!("Expected structured response"),
        }
    }

    #[tokio::test]
    async fn test_stubbed_health_returns_healthy_structured() {
        let checker = ClusterHealthChecker::new(None, None, None);
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
        let checker = ClusterHealthChecker::new(None, None, None);
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
        let checker = ClusterHealthChecker::new(None, None, None);
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
        let checker = ClusterHealthChecker::new(None, None, None);
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
        let checker = ClusterHealthChecker::new(None, None, None);
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

    #[tokio::test]
    async fn gitlab_component_is_omitted_without_a_client() {
        let checker = ClusterHealthChecker::new(None, None, None);

        let health =
            extract_structured(checker.get_cluster_health(ResponseFormat::Raw as i32).await);

        assert!(!health.components.iter().any(|c| c.name == "gitlab"));
    }

    #[tokio::test]
    async fn successful_gitlab_response_is_healthy() {
        let gitlab = start_mock_gitlab(HttpStatus::OK).await;
        let checker = ClusterHealthChecker::new(None, None, Some(gitlab));

        let health =
            extract_structured(checker.get_cluster_health(ResponseFormat::Raw as i32).await);
        let component = health
            .components
            .iter()
            .find(|c| c.name == "gitlab")
            .unwrap();

        assert_eq!(component.status, ClusterStatus::Healthy as i32);
        assert!(component.metrics.is_empty());
    }

    #[tokio::test]
    async fn gitlab_not_found_is_healthy_in_raw_and_llm_formats() {
        let gitlab = start_mock_gitlab(HttpStatus::NOT_FOUND).await;
        let checker = ClusterHealthChecker::new(None, None, Some(gitlab));

        let health =
            extract_structured(checker.get_cluster_health(ResponseFormat::Raw as i32).await);
        let component = health
            .components
            .iter()
            .find(|c| c.name == "gitlab")
            .unwrap();
        assert_eq!(component.status, ClusterStatus::Healthy as i32);
        assert!(component.metrics.is_empty());

        let response = checker.get_cluster_health(ResponseFormat::Llm as i32).await;
        let Some(get_cluster_health_response::Content::FormattedText(text)) = response.content
        else {
            panic!("Expected formatted text response");
        };
        assert!(text.contains("gitlab"));
        assert!(text.contains("healthy"));
    }

    #[tokio::test]
    async fn gitlab_unauthorized_is_diagnostic_without_changing_cluster_status() {
        let gitlab = start_mock_gitlab(HttpStatus::UNAUTHORIZED).await;
        let checker = ClusterHealthChecker::new(None, None, Some(gitlab));

        let health =
            extract_structured(checker.get_cluster_health(ResponseFormat::Raw as i32).await);

        assert_eq!(health.status, ClusterStatus::Healthy as i32);
        let component = health
            .components
            .iter()
            .find(|c| c.name == "gitlab")
            .unwrap();
        assert_eq!(component.status, ClusterStatus::Unhealthy as i32);
        assert_eq!(
            component.metrics.get("error"),
            Some(&"unauthorized (401) — check JWT secret".to_string())
        );
    }

    #[test]
    fn gitlab_status_does_not_override_migration_status() {
        let mut health = StructuredClusterHealth {
            status: ClusterStatus::Unhealthy.into(),
            timestamp: "2026-03-03T00:00:00Z".to_string(),
            version: "0.6.0".to_string(),
            components: vec![],
        };

        apply_migration_status(&mut health, 2);
        apply_gitlab_status(
            &mut health,
            ClusterStatus::Unhealthy,
            Some("GitLab unavailable".to_string()),
        );

        assert_eq!(health.status, ClusterStatus::Migrating as i32);
        assert_eq!(health.components[0].name, "schema_migration");
        assert_eq!(health.components[1].name, "gitlab");
    }

    #[tokio::test]
    async fn real_mode_healthy_sidecar() {
        let url = start_mock_sidecar(healthy_sidecar_response()).await;
        let checker = ClusterHealthChecker::new(Some(url), None, None);

        let s = extract_structured(checker.get_cluster_health(ResponseFormat::Raw as i32).await);

        assert_eq!(s.status, ClusterStatus::Healthy as i32);
        let names: Vec<&str> = s.components.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"webserver"));
        assert!(names.contains(&"indexer"));
        assert!(names.contains(&"clickhouse"));

        let webserver = s.components.iter().find(|c| c.name == "webserver").unwrap();
        let replicas = webserver.replicas.as_ref().unwrap();
        assert_eq!(replicas.ready, 2);
        assert_eq!(replicas.desired, 2);
    }

    #[tokio::test]
    async fn real_mode_unhealthy_component_propagates() {
        let url = start_mock_sidecar(degraded_sidecar_response()).await;
        let checker = ClusterHealthChecker::new(Some(url), None, None);

        let s = extract_structured(checker.get_cluster_health(ResponseFormat::Raw as i32).await);

        assert_eq!(s.status, ClusterStatus::Unhealthy as i32);
        let indexer = s.components.iter().find(|c| c.name == "indexer").unwrap();
        assert_eq!(indexer.status, ClusterStatus::Unhealthy as i32);
        let replicas = indexer.replicas.as_ref().unwrap();
        assert_eq!(replicas.ready, 0);
        assert_eq!(replicas.desired, 2);
    }

    #[tokio::test]
    async fn real_mode_unreachable_sidecar_returns_unhealthy() {
        install_crypto_provider();
        let checker = ClusterHealthChecker::new(Some("http://127.0.0.1:1".to_string()), None, None);

        let s = extract_structured(checker.get_cluster_health(ResponseFormat::Raw as i32).await);

        assert_eq!(s.status, ClusterStatus::Unhealthy as i32);
        let clickhouse = s
            .components
            .iter()
            .find(|c| c.name == "clickhouse")
            .unwrap();
        assert!(
            clickhouse
                .metrics
                .get("error")
                .unwrap()
                .contains("unreachable")
        );
    }

    #[test]
    fn apply_migration_status_sets_migrating_and_adds_component() {
        let mut status = StructuredClusterHealth {
            status: ClusterStatus::Unhealthy.into(),
            timestamp: "2026-03-03T00:00:00Z".to_string(),
            version: "0.6.0".to_string(),
            components: vec![ComponentHealth {
                name: "indexer".to_string(),
                status: ClusterStatus::Unhealthy.into(),
                replicas: Some(ReplicaStatus {
                    ready: 0,
                    desired: 2,
                }),
                metrics: HashMap::new(),
            }],
        };

        apply_migration_status(&mut status, 2);

        assert_eq!(status.status, ClusterStatus::Migrating as i32);
        let indexer = status
            .components
            .iter()
            .find(|c| c.name == "indexer")
            .unwrap();
        assert_eq!(indexer.status, ClusterStatus::Unhealthy as i32);
        assert_eq!(indexer.replicas.as_ref().unwrap().ready, 0);
        let migration = status
            .components
            .iter()
            .find(|c| c.name == "schema_migration")
            .unwrap();
        assert_eq!(migration.status, ClusterStatus::Migrating as i32);
        assert_eq!(
            migration.metrics.get("migrating_version"),
            Some(&"2".to_string())
        );
    }

    #[test]
    fn service_only_failure_is_only_services_unhealthy() {
        assert!(only_services_unhealthy(&degraded_sidecar_response()));
    }

    #[test]
    fn unhealthy_clickhouse_is_not_only_services() {
        let mut broken_clickhouse = degraded_sidecar_response();
        broken_clickhouse.clickhouse[0].status = Status::Unhealthy;
        assert!(!only_services_unhealthy(&broken_clickhouse));

        assert!(!only_services_unhealthy(&healthy_sidecar_response()));
    }

    #[test]
    fn payload_without_unhealthy_service_is_not_only_services() {
        let mut no_failing_service = degraded_sidecar_response();
        no_failing_service.services.clear();
        assert!(!only_services_unhealthy(&no_failing_service));
    }
}
