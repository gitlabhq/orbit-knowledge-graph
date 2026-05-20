# Health check architecture

## Overview

GKG exposes health information through two layers: a lightweight liveness
probe for Kubernetes and a detailed cluster health endpoint that aggregates
component status from an external health-check sidecar.

## Endpoints

### `/health` (HTTP)

Liveness probe. Returns `{"status":"ok","version":"..."}` immediately.
No dependencies, no external calls. Used by Kubernetes `livenessProbe`.

### `/api/v1/cluster_health` (HTTP)

Detailed component breakdown as JSON. Shared `ClusterHealthChecker`
instance backs both this and the gRPC endpoint. Used by monitoring
dashboards and the Orbit frontend status tab.

### `GetClusterHealth` (gRPC)

Same data, exposed over gRPC for the Rails `GrpcClient`. Supports
`ResponseFormat::Raw` (structured proto) and `ResponseFormat::Llm`
(TOON text). Rails proxies this to `GET /api/v4/orbit/status`.

## Data flow

```plaintext
K8s Pods (indexer, webserver)
    │ pod status
    ▼
health-check sidecar ◄── HTTP GET /health
    │ JSON
    ▼
InfrastructureHealthClient ──► ClusterHealthChecker (shared Arc)
                                   │              │
                              HTTP /api/v1/   gRPC GetCluster
                              cluster_health  Health(format)
```

## Modes

**Real (production):** When `GKG_HEALTH_CHECK__SERVICES` is set, the
checker fetches live status from the health-check sidecar. The sidecar
watches K8s pod status and ClickHouse connectivity, returning per-service
replica counts and error details. If the sidecar is unreachable, the
checker returns Unhealthy with the connection error rather than failing
the request.

**Stubbed (development):** When no health check URL is configured, the
checker returns hardcoded healthy responses with `mode: "stubbed"` in
each component's metrics. Local development works without a K8s cluster.

## Configuration

| Env var | Effect |
|---------|--------|
| `GKG_HEALTH_CHECK__SERVICES` | Base URL for the health-check sidecar (e.g. `http://localhost:9090`). When unset, stubbed mode is used. |

## Status taxonomy

Both `ServiceHealth` (per-component) and `HealthStatus` (cluster aggregate)
use a three-state status enum. The aggregate is the worst-of all components:
`Unhealthy` > `Degraded` > `Healthy`.

| Status | Meaning | Consumer action |
|---|---|---|
| `Healthy` | Component is fully serving traffic with all desired replicas ready. | Proceed normally. |
| `Degraded` | Component is serving traffic but not at full capacity (mid-rollout, HPA scale-up, single pod restarting). Self-resolves. | Keep sending traffic. Do not page. |
| `Unhealthy` | Component is unable to serve traffic, or a rollout has failed. | Page. Check `reason` for triage. |

Each non-healthy component carries a `reason` field with a short
machine-readable code. Consumers should treat unknown values as opaque.
The cluster aggregate carries its own `reason` copied from the worst
component whose status matches the aggregate, so an agent can read the
top-level field without iterating components.

### Reason codes

| Reason | When emitted |
|---|---|
| `rolling_update` | New replicas being created, old replicas terminating, or the controller has not yet observed the latest spec. |
| `scaling_up` | Spec increased and all current replicas already match the template (HPA scale-up, no rollout in flight). |
| `recovering` | `ready < desired` with no rollout in flight (single-pod restart, node drain). |
| `progress_deadline_exceeded` | k8s gave up on the rollout (Deployment only; see `progressDeadlineSeconds`). |
| `no_replicas_available` | `available_replicas == 0` while `desired > 0`. |
| `controller_unreachable` | kube-apiserver call failed. |
| `dependency_unhealthy` | A ClickHouse instance failed its `SELECT 1` health check. |

### Rollout disambiguation

The sidecar reads `.spec.replicas`, `metadata.generation`, and the standard
`.status` replica counters from each Deployment and StatefulSet. The same
algorithm `kubectl rollout status` uses powers the taxonomy: a
`ProgressDeadlineExceeded` condition flips the component to `Unhealthy`,
`observed_generation < generation` or any rollout-in-flight signal flips
it to `Degraded(rolling_update)`, and `ready == desired` with no rollout
in flight is `Healthy`. `desired == 0` is intentional scale-to-zero and
reports `Healthy`.

## REST API

Rails `GrpcClient.get_cluster_health` calls the gRPC endpoint and serves
the result at `GET /api/v4/orbit/status`. See
[ADR 003](decisions/003_api_design.md) for request/response examples.
