# Health check architecture

## Overview

Orbit separates local pod probes from dependency health. Every mode runs the labkit probe server
with local-only liveness and readiness endpoints for Kubernetes. The Webserver, Indexer, and
Dispatcher also keep their own `/live` and `/ready` endpoints. The HealthCheck runtime performs
networked infrastructure checks, while the Webserver presents those results together with migration
and GitLab diagnostics through the `GetClusterHealth` gRPC method.

## Pod probes

Every mode runs the labkit probe server on `probe_server.bind_address` (default `0.0.0.0:9394`). It serves
`/-/liveness`, `/-/readiness`, and, when Prometheus metrics are enabled, `/-/metrics`. The probe
server binds whether or not metrics are enabled. `/-/liveness` returns `200` while the process
runs. `/-/readiness` returns `200` when every registered check passes and `503` otherwise, with a
JSON body that names each check and its result. The Webserver registers a `schema` check that reads
the active schema snapshot and a `startup` check that reads its in-memory serving flag. The
Webserver sets that flag when startup is complete and its HTTP and gRPC listeners are bound. Thus
Kubernetes does not send traffic to a port that is not open. The Indexer and Dispatcher a `schema_gate` check that reads their in-memory serving flag. The HealthCheck runtime registers no
check, so its readiness is always `200`.

The `/live` and `/ready` endpoints below read the same state. The Helm chart probes them today.
Once the chart can probe `/-/liveness` and `/-/readiness` on the probe server port, they are
deprecated and a later release removes them.

### `/live`

The Webserver, Indexer, and Dispatcher return an immediate local response. The handler makes no
network calls and does not test ClickHouse, NATS, GitLab, or another pod. Kubernetes uses this
endpoint to determine whether the process can answer HTTP.

### `/ready`

Readiness is also local-only. The Webserver is ready (`200`) while its `ActiveSchema` holds a
serving snapshot and pending (`503`, `schema_pending`) otherwise; see
[schema management](schema_management.md#webserver-readiness-gate). The Indexer and Dispatcher
read an in-memory serving flag that is set after their startup gates have cleared. These handlers
make no network calls, so a dependency outage cannot restart otherwise healthy pods or prevent a
rollout from converging.

## HealthCheck runtime endpoints

### `/health`

The HealthCheck runtime's `/health` handler calls `HealthChecker::check()`. It reads the configured
Kubernetes Deployment and StatefulSet targets and checks ClickHouse connectivity, then returns their
aggregate and component status. This endpoint is network-dependent; it is not a pod liveness probe.

### `/queue-depth`

The HealthCheck runtime's `/queue-depth` handler reads the NATS JetStream consumer state for the code
and SDLC work queues. It returns pending and in-flight counts used by KEDA to scale code and SDLC
indexers. NATS is not included in the `/health` aggregate.

## Cluster-health endpoint

### `GetClusterHealth` (gRPC)

The Webserver returns the detailed component breakdown through the `GetClusterHealth` gRPC method.
It supports `ResponseFormat::Raw` (structured proto) and `ResponseFormat::Llm` (TOON text). The
gkg-server HTTP router does not expose a cluster-health route; it registers only `/live` and
`/ready`. Rails calls the gRPC method and exposes the result to HTTP consumers at
`GET /api/v4/orbit/status`.

## Data flow

```plaintext
Kubernetes API ─┐
                ├─► HealthChecker.check() ─► HealthCheck /health
ClickHouse ─────┘                              │
                                              ▼
                                      InfrastructureHealthClient
                                              │
Graph ClickHouse ── migrating version ────────┼─► ClusterHealthChecker
GitLab ─────────── connectivity/JWT check ────┘          │
                                                        ▼
                                               gRPC GetClusterHealth
                                                        │
                                                        ▼
                                          Rails GET /api/v4/orbit/status

NATS JetStream ─► HealthChecker.queue_depth() ─► HealthCheck /queue-depth ─► KEDA

Local ActiveSchema/serving flag ─► pod /-/readiness (and legacy /ready)
Local process response ────────────► pod /-/liveness (and legacy /live)
```

## GitLab diagnostic

When the Webserver has a GitLab client, `ClusterHealthChecker` calls the internal project-info API
with a two-second timeout and appends a `gitlab` component. A successful response or `404 Not
Found` proves connectivity and JWT authentication, so the component is Healthy. Authentication,
transport, server, response-decoding, and timeout failures make only that component Unhealthy; the
error text is included in its `error` metric.

The component is reporting-only. It is appended after the infrastructure aggregate and migration
status have been computed and never changes the top-level cluster status. It is not part of pod
readiness or the HealthCheck runtime's `/health` aggregate. When no GitLab client is configured, the
component is omitted.

## Migration awareness

Webservers stay ready through a schema migration, since they keep serving the active snapshot
while the target version is built. The overlay below only matters when Kubernetes reports the
Deployments unhealthy for another reason.

When only Kubernetes services are unhealthy and every ClickHouse component is Healthy,
`ClusterHealthChecker` reads the shared `gkg_schema_version` table. If a `migrating` row exists, it:

- changes the top-level status from `Unhealthy` to `Migrating`;
- appends a `schema_migration` component with the `migrating_version` metric; and
- preserves the actual Unhealthy service components and replica counts.

A migration cannot excuse an unreachable database. If the HealthCheck runtime is unreachable, the
client synthesizes an Unhealthy ClickHouse component, which also prevents the migration overlay.
An error reading `gkg_schema_version` leaves the status Unhealthy.

The monolith's gRPC client currently maps only Healthy, Degraded, and Unhealthy, so
`GET /api/v4/orbit/status` reports `"unknown"` for `Migrating` until the `gitlab-orbit-proto` gem and
mapping are updated. The TOON response reports `"migrating"` immediately.

An unrelated pod failure during a migration is an accepted false negative: the aggregate reports
`Migrating` rather than `Unhealthy`. This is limited to the lifetime of a `migrating` row and is
gated on ClickHouse health. See [`schema_management.md`](schema_management.md) for the migration
lifecycle.

## Modes and configuration

**Real mode:** When `health_check_url` is set, the Webserver fetches the HealthCheck
runtime's live `/health` response. If that service is unreachable, cluster health is Unhealthy and
includes the connection error rather than failing the request.

**Stubbed mode:** When no HealthCheck URL is configured, the Webserver starts from hardcoded Healthy
components with `mode: "stubbed"`. If a GitLab client is configured, the real reporting-only GitLab
diagnostic is still appended to this stubbed infrastructure data.

| Config path | Effect |
|---|---|
| `health_check_url` | Base URL for the HealthCheck runtime, for example `http://localhost:4201`. When unset, cluster health uses stubbed infrastructure data. |
| `tls.internal.enabled` | Serves the HealthCheck listener over TLS. The URL above must then be `https://`, and the certificate needs a SAN for the HealthCheck service name because the Webserver verifies it against the OS trust store. |

See [ADR 003](decisions/003_api_design.md) for cluster-health request and response examples.
