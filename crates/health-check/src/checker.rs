use clickhouse_client::ArrowClickHouseClient;

use crate::clickhouse::ClickHouseChecker;
use crate::error::Error;
use crate::k8s::K8sChecker;
use crate::types::HealthStatus;
use gkg_server_config::HealthCheckConfig;

pub struct HealthChecker {
    k8s: K8sChecker,
    clickhouse: ClickHouseChecker,
    services: Vec<String>,
}

impl HealthChecker {
    pub async fn new(
        config: &HealthCheckConfig,
        clickhouse_client: ArrowClickHouseClient,
    ) -> Result<Self, Error> {
        let k8s = K8sChecker::new(config.namespace.clone()).await?;
        let clickhouse = ClickHouseChecker::new(clickhouse_client);

        Ok(Self {
            k8s,
            clickhouse,
            services: config.services.clone(),
        })
    }

    pub async fn check(&self) -> HealthStatus {
        let services = self.k8s.check_deployments(&self.services).await;
        let clickhouse = self.clickhouse.check().await;

        HealthStatus::aggregate_status(services, clickhouse)
    }
}
