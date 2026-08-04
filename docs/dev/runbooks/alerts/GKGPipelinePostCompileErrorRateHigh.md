---
alert: GKGPipelinePostCompileErrorRateHigh
expr: |
  sum by (env, environment, stage) (rate(gkg_query_pipeline_queries_total{status!~"ok|compile_error"}[5m]))
    / sum by (env, environment, stage) (rate(gkg_query_pipeline_queries_total{status!="compile_error"}[5m])) > 0.01
for: 5m
severity: warning
annotations:
  summary: Post-compile failure rate is above 1%, isolating server-side reliability from client-input quality.
---

## Overview

The failure rate of queries that passed compilation exceeds 1%. Excluding
`compile_error` filters out bad client input, so what remains (execution,
authorization, security, internal) is the server's own reliability. This
mirrors the "Pipeline success rate (1h), post-compile" gauge on the overview
dashboard. When `GKGQueryingErrorRateHigh` fires but this one does not, the
problem is client input, not the service.

## Verification

```promql
sum by (status) (rate(gkg_query_pipeline_queries_total{status!~"ok|compile_error"}[5m]))
```

- `execution`: ClickHouse failing queries, cross-check
  `GKGExecutionFailureRate`.
- `authorization`: the Rails redaction/authorization exchange failing,
  cross-check `GKGAuthorizationFailureRate`.
- `security`: cross-check `GKGSecurityRejected`.

## Resolution

Follow the dominant failure mode's own playbook (linked above). If no single
mode dominates and the rate rose with a deploy, roll back the image tag and
investigate offline.

## Escalation

Ask in `#f_orbit_dev` (`context_systems`).
