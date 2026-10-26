# Orbit dispatcher SLO alerts

Covers `OrbitServiceGkgDispatcherErrorSLOViolation`, `OrbitServiceGkgDispatcherTrafficCessation`
and `OrbitServiceGkgDispatcherTrafficAbsent`.

## Overview

The dispatcher is a singleton. It runs graph schema migrations and periodic tasks, reads
the datalake ClickHouse and Siphon CDC events, and publishes indexing jobs to NATS.

No user-facing errors: queries keep working, data goes stale. These alerts do not page.
They post to `#f_orbit_alerts`.

## Services

- [Orbit service overview](../README.md)
- Owner team: `context_systems`

## Alerts

### OrbitServiceGkgDispatcherErrorSLOViolation

Scheduler task runs are ending with `outcome="error"`.

### OrbitServiceGkgDispatcherTrafficCessation / TrafficAbsent

No scheduler tasks are running. Since the dispatcher is the only source of indexing
jobs, all indexing dries up downstream. `TrafficCessation` means task runs recently
dropped to zero, `TrafficAbsent` means they have been missing for longer.

## Metrics

The `gkg_dispatcher` SLI in
[`metrics-catalog/services/orbit.jsonnet`](https://gitlab.com/gitlab-com/runbooks/-/blob/master/metrics-catalog/services/orbit.jsonnet)
counts `gkg_scheduler_task_runs_total`, errors are runs with `outcome="error"`.

## Alert Behavior

- Silence via [Alertmanager](https://alerts.gitlab.net/#/alerts?filter=%7Btype%3D%22orbit%22%7D)
  filtered on the alertname.
- Every deploy causes a short traffic gap: the dispatcher uses the `Recreate` strategy,
  so there is no overlap between old and new pod. A cessation alert right after a merge
  to argocd-apps is usually just that.

## Severities

- No paging. User impact is data freshness lag, growing with time. In-flight jobs on
  NATS keep draining, so the effect is delayed.

## Verification

The alerts fire on these recording rules in the `mimir-analytics-eventsdot` datasource
([Grafana Explore](https://dashboards.gitlab.net/explore)):

```promql
# Error ratio
gitlab_component_errors:ratio_1h{type="orbit", component="gkg_dispatcher", monitor="global"}

# Request rate (traffic cessation fires when this drops to zero)
gitlab_component_ops:rate_1h{type="orbit", component="gkg_dispatcher", monitor="global"}
```

- [Orbit Overview dashboard](https://dashboards.gitlab.net/d/orbit-main/orbit3a-overview?orgId=1&var-environment=orbit-prd),
  `gkg_dispatcher` row: task run rate and error rate.
- Pod state: `kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get deploy/orbit-dispatcher`.
- Logs: [Kibana, orbit index](https://log.gprd.gitlab.net/app/r/s/DhOL5), filter
  `json.mode: dispatch-indexing`. Task outcomes are logged per run.

## Recent changes

- [argocd-apps MRs](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests?scope=all&state=merged)
  touching `services/gkg`. Schema-version bumps matter most here, see
  [Does a release reindex?](../maintenance.md#does-a-release-reindex).
- [Production issues labeled Service::Orbit](https://gitlab.com/gitlab-com/gl-infra/production/-/issues/?sort=created_date&state=all&label_name%5B%5D=Service%3A%3AOrbit).

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| Readiness 503 right after a deploy | A schema migration is running, or the migration lock is held | Wait for the migration, then check dispatcher logs. Migration locks live in the NATS KV bucket `indexing_locks_v<schema version>` |
| Task errors reading the datalake | Datalake ClickHouse slow or unreachable, or Siphon CDC behind | Check the [Siphon dashboard](https://dashboards.gitlab.net/d/orbit-orbit-siphon) and datalake health |
| Fails to start with a NATS stream or KV error | NATS is unhealthy, or Orbit cannot create or update its stream | Check NATS ([nats-server](nats-server.md)). Do not delete streams or KV buckets: Siphon uses the same NATS. Escalate to Orbit tier-2 ([Escalation](../README.md#escalation)) |
| `get_graph_status` logs `KV bucket ... not registered` | KV bucket not created at startup | Cosmetic, do not roll back for this alone |
| OOM | Unoptimized query execution in a periodic task | More memory as a stopgap, report to `context_systems` |

Scheduler cadence locks (one dispatcher replica runs each task) are also in the
`indexing_locks_v<schema version>` KV bucket, inspect with
[nats-box](../server_configuration.md#troubleshoot-nats-with-nats-box).

## Possible Resolutions

- Deploy-related gap: none needed, the replacement pod picks up.
- Stuck migration or lock: check dispatcher logs, then restart the pod. If the lock stays,
  escalate to Orbit tier-2.
- Rollback across a schema version: see [Rollback](../maintenance.md#rollback).

## Dependencies

- Datalake ClickHouse and Siphon CDC (job sources), NATS JetStream (job sink and locks).
  The dispatcher cannot scale past 1 replica.

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
