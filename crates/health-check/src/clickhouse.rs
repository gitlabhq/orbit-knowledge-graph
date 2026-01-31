use clickhouse_client::ArrowClickHouseClient;
use tracing::warn;

use crate::types::{ComponentHealth, Status};

pub struct ClickHouseChecker {
    client: ArrowClickHouseClient,
}

impl ClickHouseChecker {
    pub fn new(client: ArrowClickHouseClient) -> Self {
        Self { client }
    }

    pub async fn check(&self) -> ComponentHealth {
        match self.client.execute("SELECT 1").await {
            Ok(()) => ComponentHealth {
                status: Status::Healthy,
                error: None,
            },
            Err(e) => {
                warn!(error = %e, "ClickHouse health check failed");
                ComponentHealth {
                    status: Status::Unhealthy,
                    error: Some(e.to_string()),
                }
            }
        }
    }
}
