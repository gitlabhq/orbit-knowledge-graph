use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Status {
    Healthy,
    Degraded,
    Unhealthy,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum ResourceKind {
    Deployment,
    StatefulSet,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: Status,
    pub services: Vec<ServiceHealth>,
    pub clickhouse: Vec<ComponentHealth>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceHealth {
    pub name: String,
    pub namespace: String,
    pub kind: ResourceKind,
    pub status: Status,
    pub ready_replicas: i32,
    pub desired_replicas: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub name: String,
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl HealthStatus {
    pub fn aggregate_status(
        services: Vec<ServiceHealth>,
        clickhouse: Vec<ComponentHealth>,
    ) -> Self {
        let service_worst = services.iter().map(|s| s.status).max_by_key(severity);
        let ch_worst = clickhouse.iter().map(|c| c.status).max_by_key(severity);

        let status = [service_worst, ch_worst]
            .into_iter()
            .flatten()
            .max_by_key(severity)
            .unwrap_or(Status::Healthy);

        Self {
            status,
            services,
            clickhouse,
        }
    }
}

fn severity(status: &Status) -> u8 {
    match status {
        Status::Healthy => 0,
        Status::Degraded => 1,
        Status::Unhealthy => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn healthy_service(name: &str, ns: &str, kind: ResourceKind) -> ServiceHealth {
        ServiceHealth {
            name: name.to_string(),
            namespace: ns.to_string(),
            kind,
            status: Status::Healthy,
            ready_replicas: 1,
            desired_replicas: 1,
            reason: None,
        }
    }

    fn healthy_ch(name: &str) -> ComponentHealth {
        ComponentHealth {
            name: name.to_string(),
            status: Status::Healthy,
            error: None,
            reason: None,
        }
    }

    #[test]
    fn aggregate_all_healthy() {
        let status = HealthStatus::aggregate_status(
            vec![healthy_service("web", "gkg", ResourceKind::Deployment)],
            vec![healthy_ch("clickhouse")],
        );
        assert_eq!(status.status, Status::Healthy);
    }

    #[test]
    fn aggregate_unhealthy_service() {
        let mut svc = healthy_service("web", "gkg", ResourceKind::Deployment);
        svc.status = Status::Unhealthy;
        let status = HealthStatus::aggregate_status(vec![svc], vec![healthy_ch("clickhouse")]);
        assert_eq!(status.status, Status::Unhealthy);
    }

    #[test]
    fn aggregate_unhealthy_clickhouse() {
        let status = HealthStatus::aggregate_status(
            vec![healthy_service("web", "gkg", ResourceKind::Deployment)],
            vec![ComponentHealth {
                name: "clickhouse".to_string(),
                status: Status::Unhealthy,
                error: Some("connection refused".to_string()),
                reason: None,
            }],
        );
        assert_eq!(status.status, Status::Unhealthy);
    }

    #[test]
    fn aggregate_degraded_service_yields_degraded() {
        let mut svc = healthy_service("web", "gkg", ResourceKind::Deployment);
        svc.status = Status::Degraded;
        svc.reason = Some("rolling_update".to_string());
        let status = HealthStatus::aggregate_status(vec![svc], vec![healthy_ch("clickhouse")]);
        assert_eq!(status.status, Status::Degraded);
    }

    #[test]
    fn aggregate_unhealthy_beats_degraded() {
        let mut degraded = healthy_service("web", "gkg", ResourceKind::Deployment);
        degraded.status = Status::Degraded;
        degraded.reason = Some("rolling_update".to_string());
        let mut unhealthy = healthy_service("indexer", "gkg", ResourceKind::Deployment);
        unhealthy.status = Status::Unhealthy;
        unhealthy.reason = Some("no_replicas_available".to_string());

        let status = HealthStatus::aggregate_status(
            vec![degraded, unhealthy],
            vec![healthy_ch("clickhouse")],
        );
        assert_eq!(status.status, Status::Unhealthy);
    }

    #[test]
    fn aggregate_empty_services_and_clickhouse_is_healthy() {
        let status = HealthStatus::aggregate_status(vec![], vec![]);
        assert_eq!(status.status, Status::Healthy);
    }

    #[test]
    fn aggregate_multiple_clickhouse_instances() {
        let status = HealthStatus::aggregate_status(
            vec![],
            vec![
                healthy_ch("clickhouse-graph"),
                healthy_ch("clickhouse-datalake"),
            ],
        );
        assert_eq!(status.status, Status::Healthy);
        assert_eq!(status.clickhouse.len(), 2);
    }

    #[test]
    fn json_round_trip() {
        let status = HealthStatus::aggregate_status(
            vec![
                healthy_service("indexer", "gkg", ResourceKind::Deployment),
                healthy_service("nats", "nats", ResourceKind::StatefulSet),
            ],
            vec![healthy_ch("clickhouse")],
        );
        let json = serde_json::to_string(&status).unwrap();
        let parsed: HealthStatus = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.services.len(), 2);
        assert_eq!(parsed.services[0].namespace, "gkg");
        assert_eq!(parsed.services[0].kind, ResourceKind::Deployment);
        assert_eq!(parsed.services[1].kind, ResourceKind::StatefulSet);
        assert_eq!(parsed.clickhouse.len(), 1);
        assert_eq!(parsed.clickhouse[0].name, "clickhouse");
    }

    #[test]
    fn component_health_skips_none_error_in_json() {
        let ch = healthy_ch("clickhouse");
        let json = serde_json::to_string(&ch).unwrap();
        assert!(!json.contains("error"));
        assert!(!json.contains("reason"));
    }

    #[test]
    fn service_health_json_omits_none_reason() {
        let svc = healthy_service("web", "gkg", ResourceKind::Deployment);
        let json = serde_json::to_string(&svc).unwrap();
        assert!(!json.contains("reason"));
    }

    #[test]
    fn service_health_json_round_trips_old_payload_without_reason() {
        let json = r#"{
            "name": "web",
            "namespace": "gkg",
            "kind": "Deployment",
            "status": "Healthy",
            "ready_replicas": 1,
            "desired_replicas": 1
        }"#;
        let svc: ServiceHealth = serde_json::from_str(json).unwrap();
        assert!(svc.reason.is_none());
    }
}
