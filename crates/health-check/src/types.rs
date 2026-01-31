use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub enum Status {
    Healthy,
    Unhealthy,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HealthStatus {
    pub status: Status,
    pub services: Vec<ServiceHealth>,
    pub clickhouse: ComponentHealth,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ServiceHealth {
    pub name: String,
    pub status: Status,
    pub ready_replicas: i32,
    pub desired_replicas: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComponentHealth {
    pub status: Status,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl HealthStatus {
    pub fn aggregate_status(services: Vec<ServiceHealth>, clickhouse: ComponentHealth) -> Self {
        let all_healthy = services.iter().all(|s| s.status == Status::Healthy)
            && clickhouse.status == Status::Healthy;

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
