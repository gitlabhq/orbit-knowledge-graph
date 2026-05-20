use k8s_openapi::api::apps::v1::{Deployment, StatefulSet};
use kube::{Api, Client};
use tracing::warn;

use crate::error::Error;
use crate::types::{ResourceKind, ServiceHealth, Status};
use gkg_server_config::NamespaceTarget;

pub struct K8sChecker {
    client: Client,
}

/// Inputs the rollout evaluator needs from a Deployment or StatefulSet status.
///
/// All counts are derived from the resource's `.status` subobject except
/// `desired`, which comes from `.spec.replicas` so we observe the operator's
/// intent rather than the current churn.
#[derive(Debug, Clone, Copy)]
struct RolloutInputs {
    desired: i32,
    total: i32,
    ready: i32,
    available: i32,
    updated: i32,
    /// `metadata.generation` — bumps every time `.spec` changes.
    generation: i64,
    /// `status.observed_generation` — last generation the controller acted on.
    observed_generation: i64,
    progress_deadline_exceeded: bool,
}

fn evaluate_rollout(inputs: RolloutInputs) -> (Status, Option<&'static str>) {
    let RolloutInputs {
        desired,
        total,
        ready,
        available,
        updated,
        generation,
        observed_generation,
        progress_deadline_exceeded,
    } = inputs;

    if progress_deadline_exceeded {
        return (Status::Unhealthy, Some("progress_deadline_exceeded"));
    }

    // Intentional scale-to-zero: not broken, not deploying.
    if desired == 0 && total == 0 {
        return (Status::Healthy, None);
    }

    if desired > 0 && available == 0 {
        return (Status::Unhealthy, Some("no_replicas_available"));
    }

    // Controller hasn't acted on the latest spec yet — rollout is queued.
    if observed_generation < generation {
        return (Status::Degraded, Some("rolling_update"));
    }

    // Steady state: every desired pod is ready, and the rollout has fully
    // converged on the latest template.
    let fully_converged = ready >= desired && updated >= desired && total <= updated;
    if fully_converged {
        return (Status::Healthy, None);
    }

    // HPA-style scale-up: existing pods all match the current template, but
    // the spec asks for more pods than currently exist. No old replicas to
    // drain — we're just adding to the pool.
    let scaling_up = desired > total && updated == total && total > 0;
    if scaling_up {
        return (Status::Degraded, Some("scaling_up"));
    }

    // Rollout is mid-flight if new pods are still being created, old pods are
    // still terminating, or new pods exist but haven't passed minReadySeconds.
    let rolling = updated < desired || total > updated || available < updated;
    if rolling {
        return (Status::Degraded, Some("rolling_update"));
    }

    // Updated template is fully in place but some pod is missing (node drain,
    // single-pod restart, etc.). Traffic is still being served by the rest.
    (Status::Degraded, Some("recovering"))
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
            for name in &target.stateful_sets {
                results.push(self.check_statefulset(&target.namespace, name).await);
            }
        }
        results
    }

    async fn check_deployment(&self, namespace: &str, name: &str) -> ServiceHealth {
        let api: Api<Deployment> = Api::namespaced(self.client.clone(), namespace);

        match api.get(name).await {
            Ok(deployment) => {
                let desired = deployment
                    .spec
                    .as_ref()
                    .and_then(|s| s.replicas)
                    .unwrap_or(0);
                let generation = deployment.metadata.generation.unwrap_or(0);
                let status = deployment.status.unwrap_or_default();
                let progress_deadline_exceeded = status
                    .conditions
                    .as_ref()
                    .map(|conds| {
                        conds.iter().any(|c| {
                            c.type_ == "Progressing"
                                && c.reason.as_deref() == Some("ProgressDeadlineExceeded")
                        })
                    })
                    .unwrap_or(false);

                let inputs = RolloutInputs {
                    desired,
                    total: status.replicas.unwrap_or(0),
                    ready: status.ready_replicas.unwrap_or(0),
                    available: status.available_replicas.unwrap_or(0),
                    updated: status.updated_replicas.unwrap_or(0),
                    generation,
                    observed_generation: status.observed_generation.unwrap_or(0),
                    progress_deadline_exceeded,
                };

                let (component_status, reason) = evaluate_rollout(inputs);

                ServiceHealth {
                    name: name.to_string(),
                    namespace: namespace.to_string(),
                    kind: ResourceKind::Deployment,
                    status: component_status,
                    ready_replicas: inputs.ready,
                    desired_replicas: inputs.desired,
                    reason: reason.map(str::to_string),
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
                    reason: Some("controller_unreachable".to_string()),
                }
            }
        }
    }

    async fn check_statefulset(&self, namespace: &str, name: &str) -> ServiceHealth {
        let api: Api<StatefulSet> = Api::namespaced(self.client.clone(), namespace);

        match api.get(name).await {
            Ok(sts) => {
                let desired = sts.spec.as_ref().and_then(|s| s.replicas).unwrap_or(0);
                let generation = sts.metadata.generation.unwrap_or(0);
                let status = sts.status.unwrap_or_default();

                let inputs = RolloutInputs {
                    desired,
                    total: status.replicas,
                    ready: status.ready_replicas.unwrap_or(0),
                    available: status.available_replicas.unwrap_or(0),
                    updated: status.updated_replicas.unwrap_or(0),
                    generation,
                    observed_generation: status.observed_generation.unwrap_or(0),
                    progress_deadline_exceeded: false,
                };

                let (component_status, reason) = evaluate_rollout(inputs);

                ServiceHealth {
                    name: name.to_string(),
                    namespace: namespace.to_string(),
                    kind: ResourceKind::StatefulSet,
                    status: component_status,
                    ready_replicas: inputs.ready,
                    desired_replicas: inputs.desired,
                    reason: reason.map(str::to_string),
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
                    reason: Some("controller_unreachable".to_string()),
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inputs(desired: i32, total: i32, ready: i32, available: i32, updated: i32) -> RolloutInputs {
        RolloutInputs {
            desired,
            total,
            ready,
            available,
            updated,
            generation: 1,
            observed_generation: 1,
            progress_deadline_exceeded: false,
        }
    }

    #[test]
    fn steady_state_is_healthy() {
        let (status, reason) = evaluate_rollout(inputs(3, 3, 3, 3, 3));
        assert_eq!(status, Status::Healthy);
        assert!(reason.is_none());
    }

    #[test]
    fn scale_to_zero_is_healthy() {
        let (status, reason) = evaluate_rollout(inputs(0, 0, 0, 0, 0));
        assert_eq!(status, Status::Healthy);
        assert!(reason.is_none());
    }

    #[test]
    fn rolling_update_new_replicas_pending_is_degraded() {
        // 4 desired, 3 of new template ready, 1 old pod still terminating.
        let (status, reason) = evaluate_rollout(inputs(4, 4, 3, 3, 3));
        assert_eq!(status, Status::Degraded);
        assert_eq!(reason, Some("rolling_update"));
    }

    #[test]
    fn rolling_update_with_surge_is_degraded() {
        // maxSurge=1: total briefly exceeds desired, new pods catching up.
        let (status, reason) = evaluate_rollout(inputs(4, 5, 3, 3, 4));
        assert_eq!(status, Status::Degraded);
        assert_eq!(reason, Some("rolling_update"));
    }

    #[test]
    fn single_pod_restart_with_full_updated_is_recovering() {
        // No rollout in flight (updated == desired, total == desired),
        // but one ready replica missing.
        let (status, reason) = evaluate_rollout(inputs(4, 4, 3, 4, 4));
        assert_eq!(status, Status::Degraded);
        assert_eq!(reason, Some("recovering"));
    }

    #[test]
    fn no_replicas_available_is_unhealthy() {
        let (status, reason) = evaluate_rollout(inputs(2, 2, 0, 0, 2));
        assert_eq!(status, Status::Unhealthy);
        assert_eq!(reason, Some("no_replicas_available"));
    }

    #[test]
    fn progress_deadline_exceeded_is_unhealthy() {
        let mut i = inputs(3, 3, 3, 3, 3);
        i.progress_deadline_exceeded = true;
        let (status, reason) = evaluate_rollout(i);
        assert_eq!(status, Status::Unhealthy);
        assert_eq!(reason, Some("progress_deadline_exceeded"));
    }

    #[test]
    fn progress_deadline_takes_precedence_over_no_replicas() {
        let mut i = inputs(3, 0, 0, 0, 0);
        i.progress_deadline_exceeded = true;
        let (status, reason) = evaluate_rollout(i);
        assert_eq!(status, Status::Unhealthy);
        assert_eq!(reason, Some("progress_deadline_exceeded"));
    }

    #[test]
    fn observed_generation_behind_is_rolling_update() {
        // Spec changed, controller hasn't reacted yet.
        let mut i = inputs(3, 3, 3, 3, 3);
        i.generation = 5;
        i.observed_generation = 4;
        let (status, reason) = evaluate_rollout(i);
        assert_eq!(status, Status::Degraded);
        assert_eq!(reason, Some("rolling_update"));
    }

    #[test]
    fn hpa_scale_up_is_distinguished_from_rollout() {
        // Spec wants 4 replicas, current 2 all match template and are ready.
        let (status, reason) = evaluate_rollout(inputs(4, 2, 2, 2, 2));
        assert_eq!(status, Status::Degraded);
        assert_eq!(reason, Some("scaling_up"));
    }
}
