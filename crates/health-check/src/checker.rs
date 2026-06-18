use crate::clickhouse::{ClickHouseChecker, ClickHouseInstance};
use crate::error::Error;
use crate::k8s::K8sChecker;
use crate::nats::NatsDepthChecker;
use crate::types::{HealthStatus, QueueDepth};
use gkg_server_config::{HealthCheckConfig, NamespaceTarget, NatsConfiguration};

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
        nats_config: &NatsConfiguration,
        code_stream_name: String,
        code_consumer_name: String,
    ) -> Result<Self, Error> {
        let k8s = K8sChecker::new().await?;
        let clickhouse = ClickHouseChecker::new(clickhouse_instances);
        let nats_depth =
            NatsDepthChecker::new(nats_config, code_stream_name, code_consumer_name).await?;

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
