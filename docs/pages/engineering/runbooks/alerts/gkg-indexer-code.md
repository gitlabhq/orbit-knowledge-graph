# Orbit code indexer SLO alerts

Covers `OrbitServiceGkgIndexerCodeErrorSLOViolation`,
`OrbitServiceGkgIndexerCodeTrafficCessation` and `OrbitServiceGkgIndexerCodeTrafficAbsent`.

## Overview

The code indexer pool downloads repository archives through the GitLab internal API,
parses code and writes definitions, references and imports to the graph ClickHouse.

Objective: 99% of code indexing tasks finish without an error.

No user-facing errors: queries keep working, code data goes stale. These alerts do not
page. They post to `#f_orbit_alerts`.

## Services

- [Orbit service overview](../README.md)
- Owner team: `context_systems`

## Alerts

### OrbitServiceGkgIndexerCodeErrorSLOViolation

Indexing tasks are failing. After the retry budget, failed tasks go to the dead letter
queue.

### OrbitServiceGkgIndexerCodeTrafficCessation / TrafficAbsent

The pool stopped consuming tasks. `TrafficCessation` means traffic recently dropped to
zero, `TrafficAbsent` means it has been missing for longer.

## Metrics

The `gkg_indexer_code` SLI in
[`metrics-catalog/services/orbit.jsonnet`](https://gitlab.com/gitlab-com/runbooks/-/blob/master/metrics-catalog/services/orbit.jsonnet)
counts `gkg_etl_handler_duration_seconds_count` for the `code_indexing_task` handler as
tasks and `gkg_etl_handler_errors_total` as errors. Every failed attempt counts, also when
a retry succeeds later. The `backpressure` (no free lane) and `deferred` (lock held
elsewhere) error kinds are flow control and do not count. Empty, missing and too large
repositories are not errors. Code indexing is CPU bound (parsing) and needs memory plus ephemeral disk for archives, so
resource saturation shows up here first.

## Alert Behavior

- Silence via [Alertmanager](https://alerts.gitlab.net/#/alerts?filter=%7Btype%3D%22orbit%22%7D)
  filtered on the alertname.
- Check whether failures concentrate on one project before you treat it as a service
  problem.

## Severities

- No paging. User impact is stale code data (definitions, references) in Orbit
  results and code tools.

## Verification

The alerts fire on these recording rules in the `mimir-analytics-eventsdot` datasource
([Grafana Explore](https://dashboards.gitlab.net/explore)):

```promql
# Error ratio
gitlab_component_errors:ratio_1h{type="orbit", component="gkg_indexer_code", monitor="global"}

# Request rate (traffic cessation fires when this drops to zero)
gitlab_component_ops:rate_1h{type="orbit", component="gkg_indexer_code", monitor="global"}
```

- [Orbit Overview dashboard](https://dashboards.gitlab.net/d/orbit-main/orbit3a-overview?orgId=1&var-environment=orbit-prd),
  `gkg_indexer_code` row.
- [Indexer dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer).
- Pod-level checks (OOM kills, evictions, restarts): see
  [Order of checks](../troubleshooting.md#order-of-checks).
- Logs: [Kibana, orbit index](https://log.gprd.gitlab.net/app/r/s/DhOL5), filter on the
  project ID to follow one repository.

## Recent changes

- [argocd-apps MRs](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests?scope=all&state=merged)
  touching `services/gkg`.
- [Production issues labeled Service::Orbit](https://gitlab.com/gitlab-com/gl-infra/production/-/issues/?sort=created_date&state=all&label_name%5B%5D=Service%3A%3AOrbit).

## Troubleshooting

Details: [code indexing](../code_indexing.md) (dead letter queue, stuck tasks) and
[rebuild and reindex](../manual_rebuild.md).

| Symptom | Cause | Action |
| --- | --- | --- |
| OOM kills on the code pool | A repository does not fit the memory limit | Raise the pool memory limit, see [Capacity](../maintenance.md#capacity) |
| Pod evictions for ephemeral storage | Archive larger than `tmpSizeLimit` | Raise `tmpSizeLimit` and the ephemeral-storage limit together ([Capacity](../maintenance.md#capacity)) |
| Archive downloads fail | Gitaly, Workhorse or the GitLab internal API is unhealthy | Check those services, see [code indexing](../../architecture/indexing/code-indexing.md) |
| ClickHouse write fails with `Column '_deleted' is not presented ... (THERE_IS_NO_COLUMN)` | Known code bug, not config | No infra fix, report to `context_systems` |
| Same task fails repeatedly | Poison task | Find the error in the logs. After the last attempt the task goes to the [dead letter queue](../code_indexing.md#dead-letter-queue). Do not purge NATS. Escalate to Orbit tier-2 if it continues |
| Tasks vanish without completing | Dead-lettered after retries | [Inspect the DLQ](../code_indexing.md#inspect-the-dlq), replay after fixing the cause |

## Possible Resolutions

- Raise the KEDA maximum of the code pool (`indexer.pools.code.keda.maxReplicaCount`). The
  `gkg-pool` node pool limits it. See [Capacity](../maintenance.md#capacity).
- Raise memory / ephemeral limits for large repositories.
- Replay dead-lettered tasks once the underlying cause is fixed
  ([replay a dead-lettered message](../code_indexing.md#replay-a-dead-lettered-message)).

## Dependencies

- Gitaly and Workhorse (archives), GitLab webservice (internal Orbit API), NATS
  JetStream (queue), graph ClickHouse (writes).

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
