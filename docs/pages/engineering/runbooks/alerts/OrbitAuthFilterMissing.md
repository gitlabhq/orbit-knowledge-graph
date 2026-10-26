# OrbitAuthFilterMissing

A query reached the compiler without a valid security context; the compiler rejected it, but this path means authorization filtering would have been bypassed. Fires after `1m`.

## Overview

The query compiler injects `traversal_path` predicates from the caller's JWT
into every query. This alert fires when a query reaches compilation without a
valid security context (`failure_reason="security"` on the compiler rejection
counter). The compiler is the last line of defense. It rejected the query, so
no data leaked. But nothing upstream should let such a request through.
Any non-zero count is anomalous and needs investigation.

## Services

- [Orbit service overview](../README.md)
- Owner team: `context_systems`

## Metrics

The firing expression, on `gkg_query_engine_compiler_rejected_total` (emitted by the service, see the
[metrics catalog](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/main/crates/gkg-observability/orbit-dashboards/gkg-metrics.json)):

```promql
sum by (env, environment, stage) (rate(gkg_query_engine_compiler_rejected_total{failure_reason="security"}[5m])) > 0
```

## Alert Behavior

- Silence via [Alertmanager](https://alerts.gitlab.net/#/alerts?filter=%7Btype%3D%22orbit%22%7D) filtered on the alertname.
- Expected to never fire; any occurrence is anomalous.

## Severities

- No paging during burn-in. No data leaked when it fires: the compiler rejected the queries. Escalate as if it paged.

## Verification

Confirm the rejections are real and find where they come from:

```promql
sum by (job) (rate(gkg_query_engine_compiler_rejected_total{failure_reason="security"}[5m]))
```

Then pull the matching webserver logs (`mode: webserver`) around the alert
window and look for the compiler rejection with its request context. A single
internal caller with a malformed JWT looks very different from broad traffic.

## Recent changes

- [argocd-apps MRs](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests?scope=all&state=merged) touching `services/gkg` (a merge is a deploy).
- [Production issues labeled Service::Orbit](https://gitlab.com/gitlab-com/gl-infra/production/-/issues/?sort=created_date&state=all&label_name%5B%5D=Service%3A%3AOrbit).

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| Rejections from one internal caller with a malformed or stale JWT | JWT key rotation landed on one side only | Finish the rotation on the stale side ([JWT shared key](../secret_rotation.md#jwt-shared-key)) |
| Rejections started right after a GitLab or GKG deploy | JWT claims or verification changed | Roll back the offending side ([Rollback](../maintenance.md#rollback)) |
| Requests do not come from Rails at all | Direct gRPC calls without a JWT | Treat as a security incident; capture the caller identity from the logs |

## Possible Resolutions

- If a recent GitLab or GKG deploy changed the JWT contents (claims, signing
  key rotation, `root_namespace_id`), roll the offending side back. A key
  rotation that only landed on one side is the most common cause: Rails and
  GKG cannot hold two key versions at once.
- If requests bypass Rails entirely (direct gRPC calls without a JWT), treat
  it as a security incident, not a bug.

## Dependencies

- GitLab Rails (JWT minting, internal Orbit API) builds the security context this alert watches for.

## Escalation

Escalate to the `context_systems` team (`#f_orbit_dev`) immediately. This
alert does not page. If the requests do not come from a known internal caller,
follow the security incident process.

## Definitions

- [Alert rule](https://gitlab.com/gitlab-com/runbooks/-/blob/master/mimir-rules/analytics-eventsdot/orbit/orbit-cause-alerts.yml) (hand-maintained; keep the expr above in sync when tuning it).

## Related Links

- [Related alerts](../monitoring.md#alerts)
- [Orbit runbook](../README.md)
