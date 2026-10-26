# Orbit query SLO alerts

Covers `OrbitServiceOrbitQueryApdexSLOViolation`, `OrbitServiceOrbitQueryErrorSLOViolation`,
`OrbitServiceOrbitQueryTrafficCessation` and `OrbitServiceOrbitQueryTrafficAbsent`.

## Overview

The webserver serves the graph over gRPC (`KnowledgeGraphService`). Every Orbit query a
user runs through `/api/v4/orbit/*` ends up here.

Objectives:

- 99% of valid queries finish without a server fault.
- 95% of valid queries finish within 5 seconds.

These alerts do not page. They post to `#f_orbit_alerts`.

## Services

- [Orbit service overview](../README.md)
- Owner team: `context_systems`

## Alerts

### OrbitServiceOrbitQueryErrorSLOViolation

Server faults use the error budget too fast. Users see failed queries. Server faults are
ClickHouse errors (including the time and memory limits), timeouts, failed permission
checks with Rails, content errors and compiler faults.

### OrbitServiceOrbitQueryApdexSLOViolation

Too many queries take more than 5 seconds. Users see slow Orbit API responses. Timeouts
record about 60 seconds, so they count as slow.

### OrbitServiceOrbitQueryTrafficCessation / TrafficAbsent

`TrafficAbsent` means the query metrics stopped arriving, for example after a rename or
when the webserver pods stop exporting. `TrafficCessation` cannot fire at today's traffic,
because its guard needs more than 300 queries in 30 minutes.

## Metrics

The `orbit_query` SLI in
[`metrics-catalog/services/orbit.jsonnet`](https://gitlab.com/gitlab-com/runbooks/-/blob/master/metrics-catalog/services/orbit.jsonnet)
reads two metrics from the webserver pods:

- `gkg_query_pipeline_queries_total`: valid queries have `outcome!="caller_error"`, errors
  have `outcome="server_error"`.
- `gkg_query_pipeline_duration_seconds`: the `le="5.0"` bucket for valid queries. Schema
  calls (`query_type="unknown"`) are left out, because they take about 10 ms.

The timer starts when the query pipeline starts and stops after the last result is sent.
Caller errors (bad queries, cancels, disconnects, unreadable filter values, content over
the row limit) are not valid events.

Traffic is low. The alerts use a 98% confidence bound and
need at least 120 queries in a window. A 3-day window catches slow burns.

## Alert Behavior

- Silence via [Alertmanager](https://alerts.gitlab.net/#/alerts?filter=%7Btype%3D%22orbit%22%7D)
  filtered on the alertname.
- One caller that sends many slow queries can move the apdex. Check the slowest query
  types before you treat it as a service problem.

## Severities

- No paging. Impact is slow or failed Orbit API queries for GitLab.com users.
- If all queries fail, consider raising an incident at higher severity per the
  [incident severity guide](https://handbook.gitlab.com/handbook/engineering/infrastructure-platforms/incident-management/#incident-severity).

## Verification

The alerts fire on these recording rules in the `mimir-analytics-eventsdot` datasource
([Grafana Explore](https://dashboards.gitlab.net/explore)):

```promql
# Share of valid queries within 5 s
gitlab_component_apdex:ratio_1h{type="orbit", component="orbit_query", monitor="global"}

# Share of valid queries with a server fault
gitlab_component_errors:ratio_1h{type="orbit", component="orbit_query", monitor="global"}

# Server faults by status
sum by (status) (increase(gkg_query_pipeline_queries_total{env="orbit-prd", outcome="server_error"}[1h]))
```

- [Orbit Overview dashboard](https://dashboards.gitlab.net/d/orbit-main/orbit3a-overview?orgId=1&var-environment=orbit-prd),
  `orbit_query` row.
- [Webserver dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-webserver): at a
  glance tiles, latency by query type and server errors by type.
- Test from outside: `glab orbit status`, then a real query with `glab orbit query`
  (see [First 5 minutes](../README.md#first-5-minutes)). A broken webserver can still show
  `Running` pods, so always test a query.
- Logs: [Kibana, orbit index](https://log.gprd.gitlab.net/app/r/s/DhOL5), filter
  `json.mode: webserver`.

## Recent changes

- [argocd-apps MRs](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests?scope=all&state=merged)
  touching `services/gkg` (a merge is a deploy). Check whether a fresh ReplicaSet lines up
  with the alert, see [Rollback](../maintenance.md#rollback).
- [Production issues labeled Service::Orbit](https://gitlab.com/gitlab-com/gl-infra/production/-/issues/?sort=created_date&state=all&label_name%5B%5D=Service%3A%3AOrbit).

## Troubleshooting

Work through [Order of checks](../troubleshooting.md#order-of-checks). Webserver-specific
signatures from [Common signatures](../troubleshooting.md#common-signatures):

| Symptom | Cause | Action |
| --- | --- | --- |
| All `query` calls fail, `status` and `schema` still work, logs show a panic such as `LazyLock instance has previously been poisoned` | Bad release, one panic breaks all queries | Roll back the image tag ([Rollback](../maintenance.md#rollback)) |
| Server faults with `status="execution_error"` and `Code: 159 ... TIMEOUT_EXCEEDED` | Heavy query hits the ClickHouse execution timeout | Check ClickHouse load and query weight, not the pods |
| Server faults with `status="timeout"` | The 60 s stream timeout, often a slow Rails permission check | Check Rails internal API latency |
| First queries after a quiet period are slow, then recover | ClickHouse Cloud `idle_scaling` waking up | No action, expected |
| Latency rises together with indexer write volume | Shared graph ClickHouse, indexing burst degrades reads | Pause indexing pools ([Emergency actions](../README.md#emergency-actions)) |

## Possible Resolutions

- Bad deploy: revert `image.tag` in argocd-apps (see
  [Rollback](../maintenance.md#rollback)).
- ClickHouse write pressure: pause the indexer pools, queries keep working while data
  goes stale.
- Webserver OOM: scale memory as a stopgap, then report the query to `context_systems`.

## Dependencies

- Graph ClickHouse (ClickHouse Cloud) dominates query performance.
- GitLab Rails sits in front: it calls the webserver over gRPC and runs the permission
  checks. If Rails is down, traffic stops here even though Orbit is healthy. See
  [If X fails](../README.md#if-x-fails).

## Escalation

- Ask in `#f_orbit_dev` (team channel for `context_systems`).
- Generic SLO playbooks: [ApdexSLOViolation](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/alerts/ApdexSLOViolation.md),
  [ErrorSLOViolation](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/alerts/ErrorSLOViolation.md),
  [TrafficAbsent](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/alerts/TrafficAbsent.md).

## Definitions

- [Alert rules](https://gitlab.com/gitlab-com/runbooks/-/blob/master/mimir-rules/analytics-eventsdot/orbit/autogenerated-analytics-eventsdot-orbit-service-level-alerts.yml)
  (generated, do not edit).
- [Tune the SLI](https://gitlab.com/gitlab-com/runbooks/-/blob/master/metrics-catalog/services/orbit.jsonnet), then run `make generate`.

## Related Links

- [Related alerts](../monitoring.md#alerts)
- [Orbit runbook](../README.md)
- [knowledge-graph#1395](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1395):
  how the objectives were chosen.
