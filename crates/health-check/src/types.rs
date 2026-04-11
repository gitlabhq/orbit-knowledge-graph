use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Status {
    Healthy,
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub name: String,
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl HealthStatus {
    pub fn aggregate_status(
        services: Vec<ServiceHealth>,
        clickhouse: Vec<ComponentHealth>,
    ) -> Self {
        let all_healthy = services.iter().all(|s| s.status == Status::Healthy)
            && clickhouse.iter().all(|c| c.status == Status::Healthy);

        Self {
            status: if all_healthy {
                Status::Healthy
            } else {
                Status::Unhealthy
            },
            services,
            clickhouse,
        }
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
        }
    }

    fn healthy_ch(name: &str) -> ComponentHealth {
        ComponentHealth {
            name: name.to_string(),
            status: Status::Healthy,
            error: None,
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
            }],
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
    }
}
