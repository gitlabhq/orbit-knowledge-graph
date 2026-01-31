use k8s_openapi::api::apps::v1::Deployment;
use kube::{Api, Client};
use tracing::warn;

use crate::error::Error;
use crate::types::{ServiceHealth, Status};

pub struct K8sChecker {
    client: Client,
    namespace: String,
}

impl K8sChecker {
    pub async fn new(namespace: String) -> Result<Self, Error> {
        let client = Client::try_default().await?;
        Ok(Self { client, namespace })
    }

    pub async fn check_deployment(&self, name: &str) -> ServiceHealth {
        let deployments: Api<Deployment> = Api::namespaced(self.client.clone(), &self.namespace);

        match deployments.get(name).await {
            Ok(deployment) => {
                let status = deployment.status.unwrap_or_default();
                let ready = status.ready_replicas.unwrap_or(0);
                let desired = status.replicas.unwrap_or(0);

                let healthy = ready == desired && desired > 0;

                ServiceHealth {
                    name: name.to_string(),
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
                warn!(deployment = name, error = %e, "Failed to get deployment status");
                ServiceHealth {
                    name: name.to_string(),
                    status: Status::Unhealthy,
                    ready_replicas: 0,
                    desired_replicas: 0,
                }
            }
        }
    }

    pub async fn check_deployments(&self, names: &[String]) -> Vec<ServiceHealth> {
        let mut results = Vec::with_capacity(names.len());
        for name in names {
            results.push(self.check_deployment(name).await);
        }
        results
    }
}
