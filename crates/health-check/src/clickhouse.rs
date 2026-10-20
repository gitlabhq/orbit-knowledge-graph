use clickhouse_client::ArrowClickHouseClient;
use orbit_log_fields::warn;

use crate::types::{ComponentHealth, Status};

pub struct ClickHouseInstance {
    pub name: String,
    pub client: ArrowClickHouseClient,
}

pub struct ClickHouseChecker {
    instances: Vec<ClickHouseInstance>,
}

impl ClickHouseChecker {
    pub fn new(instances: Vec<ClickHouseInstance>) -> Self {
        Self { instances }
    }

    pub async fn check(&self) -> Vec<ComponentHealth> {
        let mut results = Vec::with_capacity(self.instances.len());
        for instance in &self.instances {
            results.push(Self::check_instance(instance).await);
        }
        results
    }

    async fn check_instance(instance: &ClickHouseInstance) -> ComponentHealth {
        match instance.client.execute("SELECT 1").await {
            Ok(()) => ComponentHealth {
                name: instance.name.clone(),
                status: Status::Healthy,
                error: None,
            },
            Err(e) => {
                warn!(
                    safe(COMPONENT) = instance.name.as_str(),
                    safe(ERROR_TYPE) = <&str>::from(&e),
                    expanded(ERROR_MESSAGE) = %e,
                    "ClickHouse health check failed"
                );
                ComponentHealth {
                    name: instance.name.clone(),
                    status: Status::Unhealthy,
                    error: Some(e.to_string()),
                }
            }
        }
    }
}
