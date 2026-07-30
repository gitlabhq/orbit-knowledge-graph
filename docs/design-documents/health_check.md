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
                                   ▲   │              │
              gkg_schema_version ──┘   │              │
              (migrating row)     HTTP /api/v1/   gRPC GetCluster
                                  cluster_health  Health(format)
```

## Migration awareness

During a schema migration the newly deployed webserver pods are `Pending`
(embedded version > active version) and drop out of the K8s rotation, so
the sidecar reports the webserver deployment Unhealthy for the whole
migration window. Without extra context that is indistinguishable from a
genuinely broken deployment.

To disambiguate, `ClusterHealthChecker` reads the shared
`gkg_schema_version` table when the sidecar's aggregated status is
Unhealthy. If a `migrating` row exists (`read_migrating_version`) it applies
the `Migrating` status:

- top-level status becomes `Migrating` instead of `Unhealthy`;
- a synthetic `schema_migration` component (`Migrating`) is appended with a
  `migrating_version` metric so consumers can see the cause;
- the factual service components keep their real Unhealthy replica counts.

**Guard — the `Migrating` status only fires when every ClickHouse component
reports Healthy.** A migration can never excuse an unreachable database, and
if the sidecar itself is unreachable it synthesizes an Unhealthy ClickHouse
component, so the guard also blocks it there. Any error reading
`gkg_schema_version` leaves the status Unhealthy (fail toward the stricter
state).

**Rails caveat:** the monolith's gRPC client currently maps only Healthy,
Degraded, and Unhealthy, so `GET /api/v4/orbit/status` reports `"unknown"`
for `Migrating` until the `gitlab-gkg-proto` gem is bumped and the mapping
added. The TOON/LLM format returns `"migrating"` immediately.

**Accepted false negative:** an unrelated pod failure that happens *during*
a migration window reads `Migrating` rather than `Unhealthy`. This is
time-bounded (only while a `migrating` row exists) and gated on ClickHouse
health, so a hard infrastructure failure is never masked. See
[`schema_management.md`](schema_management.md) for the migration lifecycle
that writes the `migrating` row.

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
