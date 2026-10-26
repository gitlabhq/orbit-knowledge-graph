# Orbit NATS server SLO alerts

Covers `OrbitServiceNatsServerErrorSLOViolation`, `OrbitServiceNatsServerTrafficCessation`
and `OrbitServiceNatsServerTrafficAbsent`.

## Overview

NATS is the message broker between Siphon, the dispatcher and the indexers. This SLI
watches core NATS message flow.

These alerts cover only the NATS that runs in the Orbit clusters (orbit-prd, orbit-stg) for
Orbit and Siphon. Other NATS deployments belong to the `nats` service. These alerts do
not page and post to `#f_orbit_alerts`. Disk saturation has its own alert
(`nats_disk_space`), with its runbook at
[docs/nats/operations.md](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/nats/operations.md).

## Services

- [Orbit service overview](../README.md)
- [NATS runbook](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/nats/README.md)
- Owner team: `context_systems`, for the NATS in the Orbit clusters and the Orbit
  consumers. Siphon, the other producer, is owned by `platform_insights`.

## Alerts

### OrbitServiceNatsServerErrorSLOViolation

Slow consumers are rising. A slow consumer is a client that cannot keep up with the
messages NATS is sending it; NATS drops it after a buffer limit.

### OrbitServiceNatsServerTrafficCessation / TrafficAbsent

No messages are flowing at all. Either the producers stopped (dispatcher, Siphon) or
the cluster itself is unhealthy. `TrafficCessation` means traffic recently dropped to
zero, `TrafficAbsent` means it has been missing for longer.

## Metrics

The `nats_server` SLI in
[`metrics-catalog/services/orbit.jsonnet`](https://gitlab.com/gitlab-com/runbooks/-/blob/master/metrics-catalog/services/orbit.jsonnet)
uses the NATS varz metrics (message in-rate, slow consumer count) from the Prometheus
exporter sidecar.

Slow consumers should be zero in normal operation. Any sustained non-zero rate means a
consumer is undersized or stuck.

## Alert Behavior

- Silence via [Alertmanager](https://alerts.gitlab.net/#/alerts?filter=%7Btype%3D%22orbit%22%7D)
  filtered on the alertname.
- Slow consumers usually track indexer health: an underscaled or crashing indexer pool
  shows up here before anywhere else.

## Severities

- No paging. User impact is indexing delay (stale data), queries are unaffected.

## Verification

The alerts fire on these recording rules in the `mimir-analytics-eventsdot` datasource
([Grafana Explore](https://dashboards.gitlab.net/explore)):

```promql
# Error ratio
gitlab_component_errors:ratio_1h{type="orbit", component="nats_server", monitor="global"}

# Request rate (traffic cessation fires when this drops to zero)
gitlab_component_ops:rate_1h{type="orbit", component="nats_server", monitor="global"}
```

- [NATS dashboard](https://dashboards.gitlab.net/d/orbit-orbit-nats) for message rates, slow
  consumers, JetStream state.
- Cluster state: `kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n nats get statefulset,pod,pvc`.
- Stream and consumer detail from a nats-box pod:
  [Troubleshoot NATS with nats-box](../server_configuration.md#troubleshoot-nats-with-nats-box).

## Recent changes

- [argocd-apps MRs](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests?scope=all&state=merged)
  for services `nats` and `gkg`.
- [Production issues labeled Service::Orbit](https://gitlab.com/gitlab-com/gl-infra/production/-/issues/?sort=created_date&state=all&label_name%5B%5D=Service%3A%3AOrbit).

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| Slow consumers rising, indexer pods restarting | Indexer pool unhealthy or undersized | Fix or scale the indexers, not NATS. See [indexer SDLC](gkg-indexer-sdlc.md) / [indexer code](gkg-indexer-code.md) pages |
| Traffic ceased, NATS pods healthy | Producers stopped: dispatcher down or Siphon CDC dry | See the [dispatcher page](gkg-dispatcher.md) and the [Siphon dashboard](https://dashboards.gitlab.net/d/orbit-orbit-siphon) |
| NATS pod down, cluster degraded | Node or PVC problem | Standard kube triage. Orbit streams have one replica, so the streams on that node are unavailable until it comes back ([If X fails](../README.md#if-x-fails)) |
| Disk usage growing | Lagging consumers accumulate persisted stream data | Scale the indexers. For emergency disk mitigation see [docs/nats/operations.md](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/nats/operations.md) |

## Possible Resolutions

- Scale or fix the consuming indexer pools; NATS itself rarely needs scaling
  (see [Capacity](../maintenance.md#capacity)).
- mTLS certificate problems surface as connection errors on Orbit pods; certificates are
  cert-manager issued, check their age and events.

## Dependencies

- Producers: Siphon CDC, the dispatcher. Consumers: the indexer pools. If NATS is fully
  down, all indexing stops, Siphon stops and new webserver pods do not become ready. See
  [If X fails](../README.md#if-x-fails).

## Escalation

- Ask in `#f_orbit_dev` (team channel for `context_systems`). For Siphon producer
  problems, ask in `#g_analytics_platform_insights`.
- Generic SLO playbooks: [ErrorSLOViolation](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/alerts/ErrorSLOViolation.md),
  [TrafficAbsent](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/alerts/TrafficAbsent.md).

## Definitions

- [Alert rules](https://gitlab.com/gitlab-com/runbooks/-/blob/master/mimir-rules/analytics-eventsdot/orbit/autogenerated-analytics-eventsdot-orbit-service-level-alerts.yml)
  (generated, do not edit).
- [Tune the SLI](https://gitlab.com/gitlab-com/runbooks/-/blob/master/metrics-catalog/services/orbit.jsonnet), then run `make generate`.

## Related Links

- [Related alerts](../monitoring.md#alerts)
- [Orbit runbook](../README.md)
