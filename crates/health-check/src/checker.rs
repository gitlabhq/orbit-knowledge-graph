use crate::clickhouse::{ClickHouseChecker, ClickHouseInstance};
use crate::error::Error;
use crate::k8s::K8sChecker;
use crate::nats::{CodeQueueConfig, NatsDepthChecker};
use crate::types::{HealthStatus, QueueDepth};
use gkg_server_config::{HealthCheckConfig, NamespaceTarget};

pub struct HealthChecker {
    k8s: K8sChecker,
    clickhouse: ClickHouseChecker,
    nats_depth: NatsDepthChecker,
    targets: Vec<NamespaceTarget>,
}

impl HealthChecker {
    pub async fn new(
        config: &HealthCheckConfig,
        clickhouse_instances: Vec<ClickHouseInstance>,
        code_queue: CodeQueueConfig,
    ) -> Result<Self, Error> {
        let k8s = K8sChecker::new().await?;
        let clickhouse = ClickHouseChecker::new(clickhouse_instances);
        let nats_depth = NatsDepthChecker::new(code_queue);

        Ok(Self {
            k8s,
            clickhouse,
            nats_depth,
            targets: config.targets.clone(),
        })
    }

    pub async fn check(&self) -> HealthStatus {
        let services = self.k8s.check_targets(&self.targets).await;
        let clickhouse = self.clickhouse.check().await;

        HealthStatus::aggregate_status(services, clickhouse)
    }

    pub async fn queue_depth(&self) -> Result<QueueDepth, String> {
        self.nats_depth.check().await
    }
}
