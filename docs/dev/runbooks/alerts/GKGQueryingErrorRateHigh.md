---
alert: GKGQueryingErrorRateHigh
expr: |
  sum by (env, environment, stage) (rate(gkg_query_pipeline_queries_total{status!="ok"}[5m]))
    / sum by (env, environment, stage) (rate(gkg_query_pipeline_queries_total[5m])) > 0.05
for: 5m
severity: warning
annotations:
  summary: Aggregate query error rate across all failure modes is above 5% for 5 minutes (the availability SLI).
---

## Overview

The aggregate error rate across the whole query pipeline (compilation,
security, execution) exceeds 5%. This is the availability SLI for querying:
users are getting failed Orbit API responses. Compile errors caused by bad
client input count toward this rate, so a single broken client with enough
volume can trip it without any server-side fault.

## Verification

Break the rate down by failure mode to find what is actually failing:

```promql
sum by (status) (rate(gkg_query_pipeline_queries_total{status!="ok"}[5m]))
```

- `compile_error` dominating: likely a client problem, cross-check
  `GKGValidationFailedBurst`.
- `execution` dominating: ClickHouse is failing queries, check the graph
  ClickHouse health and load.
- `security` dominating: cross-check `GKGSecurityRejected` and
  `GKGAuthFilterMissing`.

## Resolution

- Execution failures: reduce ClickHouse pressure (pause indexing pools) or
  scale the ClickHouse service; a heavy indexing burst degrading reads is the
  most common cause.
- Compile-error bursts from one caller: identify the client in the logs and
  fix or rate-limit it; the server needs no change.
- A fresh deploy lining up with the alert: roll back the image tag.

## Escalation

Ask in `#f_orbit_dev` (`context_systems`). If ClickHouse itself is unhealthy,
check the ClickHouse Cloud console before escalating further.
