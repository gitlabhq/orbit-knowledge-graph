use crate::clickhouse::{ClickHouseChecker, ClickHouseInstance};
use crate::error::Error;
use crate::k8s::K8sChecker;
use crate::types::HealthStatus;
use gkg_server_config::{HealthCheckConfig, NamespaceTarget};

pub struct HealthChecker {
    k8s: K8sChecker,
    clickhouse: ClickHouseChecker,
    targets: Vec<NamespaceTarget>,
}

impl HealthChecker {
    pub async fn new(
        config: &HealthCheckConfig,
        clickhouse_instances: Vec<ClickHouseInstance>,
    ) -> Result<Self, Error> {
        let k8s = K8sChecker::new().await?;
        let clickhouse = ClickHouseChecker::new(clickhouse_instances);

        Ok(Self {
            k8s,
            clickhouse,
            targets: config.targets.clone(),
        })
    }

    pub async fn check(&self) -> HealthStatus {
        let services = self.k8s.check_targets(&self.targets).await;
        let clickhouse = self.clickhouse.check().await;

        HealthStatus::aggregate_status(services, clickhouse)
    }
}
