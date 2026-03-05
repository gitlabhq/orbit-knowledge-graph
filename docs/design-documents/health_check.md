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

## REST API

Rails `GrpcClient.get_cluster_health` calls the gRPC endpoint and serves
the result at `GET /api/v4/orbit/status`. See
[ADR 003](decisions/003_api_design.md) for request/response examples.
