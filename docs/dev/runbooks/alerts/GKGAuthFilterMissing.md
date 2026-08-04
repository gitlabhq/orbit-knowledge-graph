---
alert: GKGAuthFilterMissing
expr: |
  sum by (env, environment, stage) (rate(gkg_query_engine_compiler_rejected_total{failure_reason="security"}[5m])) > 0
for: 1m
severity: critical
annotations:
  summary: A query reached the compiler without a valid security context; the compiler rejected it, but this path means authorization filtering would have been bypassed.
---

## Overview

The query compiler injects `traversal_path` predicates from the caller's JWT
into every query. This alert fires when a query reaches compilation without a
valid security context (`failure_reason="security"` on the compiler rejection
counter). The compiler is the last line of defense: it rejected the query, so
no data leaked, but nothing upstream should ever let such a request through.
Any non-zero count is anomalous and needs investigation.

## Verification

Confirm the rejections are real and find where they come from:

```promql
sum by (job) (rate(gkg_query_engine_compiler_rejected_total{failure_reason="security"}[5m]))
```

Then pull the matching webserver logs (`mode: webserver`) around the alert
window and look for the compiler rejection with its request context. A single
internal caller with a malformed JWT looks very different from broad traffic.

## Resolution

- If a recent GitLab or GKG deploy changed the JWT contents (claims, signing
  key rotation, `root_namespace_id`), roll the offending side back. A key
  rotation that only landed on one side is the most common cause: Rails and
  GKG cannot hold two key versions at once.
- If requests bypass Rails entirely (direct gRPC calls without a JWT), treat
  it as a security incident, not a bug.

## Escalation

Treat as a security-relevant page: escalate to the `context_systems` team
(`#f_orbit_dev`) immediately, and follow the security incident process if the
requests do not come from a known internal caller.
