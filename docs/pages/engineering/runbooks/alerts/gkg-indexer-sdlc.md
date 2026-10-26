# Orbit SDLC indexer SLO alerts

Covers `OrbitServiceGkgIndexerSdlcErrorSLOViolation`,
`OrbitServiceGkgIndexerSdlcTrafficCessation` and `OrbitServiceGkgIndexerSdlcTrafficAbsent`.

## Overview

The SDLC indexer pool consumes entity events (users, projects, MRs, pipelines, ...) from
NATS and writes graph data to the graph ClickHouse.

Objective: 99% of handler runs finish without an error.

No user-facing errors: queries keep working, data goes stale. These alerts do not page.
They post to `#f_orbit_alerts`. Data freshness has its own alerts, see
[sdlc-freshness](sdlc-freshness.md).

## Services

- [Orbit service overview](../README.md)
- Owner team: `context_systems`

## Alerts

### OrbitServiceGkgIndexerSdlcErrorSLOViolation

Handlers are failing. NATS does not redeliver a failed SDLC request. The dispatcher sends
the work again on a later cycle.

### OrbitServiceGkgIndexerSdlcTrafficCessation / TrafficAbsent

The pool stopped consuming messages. Either the pods are down, the NATS consumer is
stuck, or nothing upstream (Siphon, dispatcher) is producing. `TrafficCessation` means
traffic recently dropped to zero, `TrafficAbsent` means it has been missing for longer.

## Metrics

The `gkg_indexer_sdlc` SLI in
[`metrics-catalog/services/orbit.jsonnet`](https://gitlab.com/gitlab-com/runbooks/-/blob/master/metrics-catalog/services/orbit.jsonnet)
counts `gkg_etl_handler_duration_seconds_count` for the `entity.*` handlers as runs and
`gkg_etl_handler_errors_total` as errors. Every failed attempt counts, also when a retry
succeeds later. The `backpressure` and `deferred` error kinds are flow control and do not
count. The handler duration is still recorded, but it has no objective.

## Alert Behavior

- Silence via [Alertmanager](https://alerts.gitlab.net/#/alerts?filter=%7Btype%3D%22orbit%22%7D)
  filtered on the alertname.
- A short error burst during a deploy or a ClickHouse hiccup self-heals: the dispatcher
  sends the work again. Sustained violation means something is actually stuck.

## Severities

- No paging. User impact is stale SDLC data in Orbit query results, not errors.

## Verification

The alerts fire on these recording rules in the `mimir-analytics-eventsdot` datasource
([Grafana Explore](https://dashboards.gitlab.net/explore)):

```promql
# Error ratio
gitlab_component_errors:ratio_1h{type="orbit", component="gkg_indexer_sdlc", monitor="global"}

# Request rate (traffic cessation fires when this drops to zero)
gitlab_component_ops:rate_1h{type="orbit", component="gkg_indexer_sdlc", monitor="global"}
```

- [Orbit Overview dashboard](https://dashboards.gitlab.net/d/orbit-main/orbit3a-overview?orgId=1&var-environment=orbit-prd),
  `gkg_indexer_sdlc` row.
- [Indexer dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer) for per-handler
  duration and error split.
- Indexing progress for a namespace: `glab orbit graph-status --full-path gitlab-org`.
- Logs: [Kibana, orbit index](https://log.gprd.gitlab.net/app/r/s/DhOL5), filter
  `json.mode: indexer`.

## Recent changes

- [argocd-apps MRs](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests?scope=all&state=merged)
  touching `services/gkg`.
- [Production issues labeled Service::Orbit](https://gitlab.com/gitlab-com/gl-infra/production/-/issues/?sort=created_date&state=all&label_name%5B%5D=Service%3A%3AOrbit).

## Troubleshooting

Details: [SDLC indexing](../sdlc_indexing.md) (checkpoints) and
[rebuild and reindex](../manual_rebuild.md).

| Symptom | Cause | Action |
| --- | --- | --- |
| Errors on the ClickHouse write | Graph ClickHouse slow or unreachable | Check ClickHouse Cloud status and load first |
| Traffic ceased, pods healthy | Upstream dry: Siphon CDC or the dispatcher stopped producing | Check the [Siphon dashboard](https://dashboards.gitlab.net/d/orbit-orbit-siphon) and the [dispatcher alerts page](gkg-dispatcher.md) |
| One namespace never finishes | Checkpoint stuck or corrupted cursor | [Inspect checkpoints](../sdlc_indexing.md#inspect-checkpoints), then [reindex the namespace](../manual_rebuild.md#reindex-one-namespace-project-or-entity) |
| Same namespace fails on every cycle | A request that fails on every attempt | NATS does not redeliver it. Find the error in the logs. Do not purge NATS. Escalate to Orbit tier-2 ([Escalation](../README.md#escalation)) |
| Consumer lag grows, no errors | Throughput bound | Raise `indexer.pools.sdlc.keda.maxReplicaCount` in the ArgoCD values, up to the `max_ack_pending` limit ([Capacity](../maintenance.md#capacity)) |

NATS state can be inspected from a nats-box pod, see
[Troubleshoot NATS with nats-box](../server_configuration.md#troubleshoot-nats-with-nats-box).

## Possible Resolutions

- Raise the KEDA maximum of the SDLC pool, up to the `max_ack_pending` limit
  ([Capacity](../maintenance.md#capacity)).
- Reindex the affected namespace
  ([Reindex one namespace or one project](../manual_rebuild.md#reindex-one-namespace-project-or-entity)).
  After a pause, the dispatcher sends the missing work again, so data is late but not lost.

## Dependencies

- Graph ClickHouse (writes), NATS JetStream (queue), datalake ClickHouse via the
  dispatcher (job source). See [If X fails](../README.md#if-x-fails).

## Escalation

- Ask in `#f_orbit_dev` (team channel for `context_systems`).
- Generic SLO playbooks: [ErrorSLOViolation](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/alerts/ErrorSLOViolation.md),
  [TrafficAbsent](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/alerts/TrafficAbsent.md).

## Definitions

- [Alert rules](https://gitlab.com/gitlab-com/runbooks/-/blob/master/mimir-rules/analytics-eventsdot/orbit/autogenerated-analytics-eventsdot-orbit-service-level-alerts.yml)
  (generated, do not edit).
- [Tune the SLI](https://gitlab.com/gitlab-com/runbooks/-/blob/master/metrics-catalog/services/orbit.jsonnet), then run `make generate`.

## Related Links

- [Related alerts](../monitoring.md#alerts)
- [Orbit runbook](../README.md)
