# Orbit SDLC freshness alerts

Covers `OrbitSdlcFreshnessSLOViolation`, `OrbitServiceSdlcFreshnessTrafficCessation` and
`OrbitServiceSdlcFreshnessTrafficAbsent`. The 4 hour hard limit has its own playbook:
[OrbitSdlcDataOlderThan4h](OrbitSdlcDataOlderThan4h.md).

## Overview

Orbit copies SDLC data (issues, merge requests, pipelines and more) from the datalake into
the graph. Freshness tells how far the served graph data trails the datalake. Siphon lag
(GitLab database to datalake) is not part of it.

Objective: 99% of enabled namespace and SDLC entity pairs are at most 15 minutes behind.

These alerts do not page. They post to `#f_orbit_alerts`.

## Services

- [Orbit service overview](../README.md)
- Owner team: `context_systems`

## Alerts

### OrbitSdlcFreshnessSLOViolation

Too many pairs are more than 15 minutes behind over 1 hour (fast burn) or 6 hours (slow
burn). Users read old issues, merge requests or pipelines.

The alert stays quiet while a schema migration runs and is under 4 hours, and for one
alert window after the migration ends. A full-rebuild migration freezes the served data
until promotion, so all namespaces age with it. That time still counts against the error
budget. A migration that passes 4 hours does not get the quiet period, and
[OrbitSdlcDataOlderThan4h](OrbitSdlcDataOlderThan4h.md) also fires.

### OrbitServiceSdlcFreshnessTrafficCessation / TrafficAbsent

Freshness samples stopped. The dispatcher task `namespace_freshness` no longer runs or no
longer exports. Without samples, the freshness alerts cannot fire, so treat this as
"freshness unknown".

## Metrics

The `sdlc_freshness` SLI in
[`metrics-catalog/services/orbit.jsonnet`](https://gitlab.com/gitlab-com/runbooks/-/blob/master/metrics-catalog/services/orbit.jsonnet)
reads `gkg_indexer_sdlc_freshness_lag_seconds` from the dispatcher. Every 5 minutes the
dispatcher records one sample per enabled namespace and SDLC entity:

- 0 when the pair is in sync with the datalake.
- The time since the last full sync when the datalake has newer changes.
- The time since the namespace was enabled when it never finished its first sync.

The value is an upper bound. An idle entity that gets one change can report up to about
65 minutes for one sample, until the change is indexed. This keeps about 0.1 to 0.3% of
samples above 15 minutes on a normal day.

The burn-rate alerts are hand-written in
[`libsonnet/alerts/orbit-alerts.libsonnet`](https://gitlab.com/gitlab-com/runbooks/-/blob/master/libsonnet/alerts/orbit-alerts.libsonnet),
because the generated SLO alerts cannot skip migrations.

## Alert Behavior

- Silence via [Alertmanager](https://alerts.gitlab.net/#/alerts?filter=%7Btype%3D%22orbit%22%7D)
  filtered on the alertname.
- A full indexer outage raises only the pairs that change, because idle pairs stay at 0.
  The indexer traffic cessation alerts usually fire first.

## Severities

- No paging. Impact is stale SDLC data in Orbit results. Queries still work.

## Verification

```promql
# Share of pairs within 15 minutes (the SLI)
gitlab_component_apdex:ratio_1h{type="orbit", component="sdlc_freshness", monitor="global"}

# Pairs more than 15 minutes behind, by entity, in the last run
sum by (entity) (increase(gkg_indexer_sdlc_freshness_lag_seconds_count{env="orbit-prd"}[5m]))
  - sum by (entity) (increase(gkg_indexer_sdlc_freshness_lag_seconds_bucket{env="orbit-prd", le="900.0"}[5m]))

# Is a migration running? (seconds, 0 when idle)
max(gkg_schema_migrating_age_seconds{env="orbit-prd"})

# Did the freshness task run?
sum(increase(gkg_scheduler_task_runs_total{env="orbit-prd", task="namespace_freshness"}[30m])) by (outcome)
```

- [Indexer dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer), at a
  glance and "Freshness and saturation" rows.
- `glab orbit graph-status --full-path <path>` shows the indexing state of one namespace.

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| All entities of all namespaces climb together | Indexer pods down, NATS consumer stalled or graph ClickHouse writes fail | Check the indexer cessation alerts, pods and NATS consumer lag |
| All namespaces climb, a migration age above 0 | Full-rebuild schema migration | Expected up to 4 hours. Past 4 hours see [OrbitSdlcDataOlderThan4h](OrbitSdlcDataOlderThan4h.md) |
| One entity climbs for many namespaces | One pipeline fails or the datalake table for it is behind | Check `gkg_etl_handler_errors_total` for that `handler` |
| Few namespaces climb, others stay at 0 | Large namespaces or a failing namespace | Check the indexer logs for the namespace and `glab orbit graph-status --full-path <path>` |

## Dependencies

- Dispatcher (sends indexing requests, records the samples), NATS JetStream, SDLC indexer
  pods, graph ClickHouse and the datalake.

## Escalation

- Ask in `#f_orbit_dev` (team channel for `context_systems`).

## Definitions

- [SLI](https://gitlab.com/gitlab-com/runbooks/-/blob/master/metrics-catalog/services/orbit.jsonnet) and
  [alert rules](https://gitlab.com/gitlab-com/runbooks/-/blob/master/libsonnet/alerts/orbit-alerts.libsonnet). Run `make generate`
  after a change.

## Related Links

- [Related alerts](../monitoring.md#alerts)
- [Orbit runbook](../README.md)
- [knowledge-graph#1395](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1395)
  and [knowledge-graph#1331](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1331)
  (stale data during full-rebuild migrations).
