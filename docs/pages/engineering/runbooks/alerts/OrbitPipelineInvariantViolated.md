# OrbitPipelineInvariantViolated

The query compiler reached a state that upstream validation should have prevented; the generated SQL could have been incorrect. Fires after `1m`.

## Overview

The compiler rejected a query in a late stage (`lowering`, `enforcement`,
`codegen` or `pipeline`) that earlier validation should have made
unreachable. Each rejection is a compiler bug surfacing, and the same broken
state could in another variant produce incorrect SQL instead of a rejection.
Any non-zero count is anomalous.

## Services

- [Orbit service overview](../README.md)
- Owner team: `context_systems`

## Metrics

The firing expression, on `gkg_query_engine_compiler_rejected_total` (emitted by the service, see the
[metrics catalog](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/crates/gkg-observability/orbit-dashboards/gkg-metrics.json)):

```promql
sum by (env, environment, stage) (rate(gkg_query_engine_compiler_rejected_total{failure_reason=~"lowering|enforcement|codegen|pipeline"}[5m])) > 0
```

## Alert Behavior

- Silence via [Alertmanager](https://alerts.gitlab.net/#/alerts?filter=%7Btype%3D%22orbit%22%7D) filtered on the alertname.
- Expected to never fire; each rejection is a compiler bug surfacing.

## Severities

- No paging during burn-in. The compiler rejected the query, so no incorrect SQL executed, but another variant of the same state might not be caught.

## Verification

Split the rejections by stage to see which invariant broke:

```promql
sum by (failure_reason) (rate(gkg_query_engine_compiler_rejected_total{failure_reason=~"lowering|enforcement|codegen|pipeline"}[5m]))
```

Find the rejected query in the webserver logs (`mode: webserver`) around the
alert window; the rejection log carries the failing query shape.

## Recent changes

- [argocd-apps MRs](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests?scope=all&state=merged) touching `services/gkg` (a merge is a deploy).
- [Production issues labeled Service::Orbit](https://gitlab.com/gitlab-com/gl-infra/production/-/issues/?sort=created_date&state=all&label_name%5B%5D=Service%3A%3AOrbit).

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| Rejections started with a deploy | New compiler bug in the release | Roll back the image tag ([Rollback](../maintenance.md#rollback)); file an issue with the failing query |
| No deploy correlation | Latent bug triggered by a new query shape | Capture the query from the logs; the fuzzers usually reproduce the class |

## Possible Resolutions

There is no operational fix: this is a code bug in the query compiler. If the
rejections started with a deploy, roll back the image tag. Capture the failing
query from the logs and file an issue with it; the fuzzers
(`mise run fuzz:compile`) can usually reproduce the class of failure.

## Dependencies

- None external: this is a query-compiler code bug by definition.

## Escalation

Escalate to `context_systems` (`#f_orbit_dev`) with the failing query
attached. Treat a sustained rate as a rollback trigger, not something to
wait out.

## Definitions

- [Alert rule](https://gitlab.com/gitlab-com/runbooks/-/blob/master/mimir-rules/analytics-eventsdot/orbit/orbit-cause-alerts.yml) (hand-maintained; keep the expr above in sync when tuning it).

## Related Links

- [Related alerts](../monitoring.md#alerts)
- [Orbit runbook](../README.md)
