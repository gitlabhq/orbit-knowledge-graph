use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use kube::{Api, Client};
use tracing::warn;

use crate::error::Error;
use crate::types::{ResourceKind, ServiceHealth, Status};
use gkg_server_config::NamespaceTarget;

pub struct K8sChecker {
    client: Client,
}

impl K8sChecker {
    pub async fn new() -> Result<Self, Error> {
        let client = Client::try_default().await?;
        Ok(Self { client })
    }

    pub async fn check_targets(&self, targets: &[NamespaceTarget]) -> Vec<ServiceHealth> {
        let mut results = Vec::new();
        for target in targets {
            for name in &target.deployments {
                results.push(self.check_deployment(&target.namespace, name).await);
            }
            for name in &target.statefulsets {
                results.push(self.check_statefulset(&target.namespace, name).await);
            }
        }
        results
    }

    async fn check_deployment(&self, namespace: &str, name: &str) -> ServiceHealth {
        let api: Api<Deployment> = Api::namespaced(self.client.clone(), namespace);

        match api.get(name).await {
            Ok(deployment) => {
                let status = deployment.status.unwrap_or_default();
                let ready = status.ready_replicas.unwrap_or(0);
                let desired = status.replicas.unwrap_or(0);
                let healthy = ready == desired && desired > 0;

                ServiceHealth {
                    name: name.to_string(),
                    namespace: namespace.to_string(),
                    kind: ResourceKind::Deployment,
                    status: if healthy {
                        Status::Healthy
                    } else {
                        Status::Unhealthy
                    },
                    ready_replicas: ready,
                    desired_replicas: desired,
                }
            }
            Err(e) => {
                warn!(namespace, deployment = name, error = %e, "Failed to get deployment status");
                ServiceHealth {
                    name: name.to_string(),
                    namespace: namespace.to_string(),
                    kind: ResourceKind::Deployment,
                    status: Status::Unhealthy,
                    ready_replicas: 0,
                    desired_replicas: 0,
                }
            }
        }
    }

    async fn check_statefulset(&self, namespace: &str, name: &str) -> ServiceHealth {
        let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), namespace);

        match api.get(name).await {
            Ok(sts) => {
                let status = sts.status.unwrap_or_default();
                let ready = status.ready_replicas.unwrap_or(0);
                let desired = status.replicas;
                let healthy = ready == desired && desired > 0;

                ServiceHealth {
                    name: name.to_string(),
                    namespace: namespace.to_string(),
                    kind: ResourceKind::StatefulSet,
                    status: if healthy {
                        Status::Healthy
                    } else {
                        Status::Unhealthy
                    },
                    ready_replicas: ready,
                    desired_replicas: desired,
                }
            }
            Err(e) => {
                warn!(namespace, statefulset = name, error = %e, "Failed to get statefulset status");
                ServiceHealth {
                    name: name.to_string(),
                    namespace: namespace.to_string(),
                    kind: ResourceKind::StatefulSet,
                    status: Status::Unhealthy,
                    ready_replicas: 0,
                    desired_replicas: 0,
                }
            }
        }
    }
}
