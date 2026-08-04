---
alert: GKGExecutionFailureRate
expr: |
  sum by (env, environment, stage) (rate(gkg_query_pipeline_failed_total{failure_reason="execution"}[5m])) * 60 > 1
for: 5m
severity: warning
annotations:
  summary: ClickHouse query execution is failing, above 1 failure per minute.
---

## Overview

Compiled queries are failing at execution against the graph ClickHouse.
ClickHouse dominates query performance and shares its capacity between
webserver reads and indexer writes, so the usual cause is load: a heavy
indexing burst, a very heavy query, or the ClickHouse Cloud service itself
being degraded. Timeouts (`Code: 159 TIMEOUT_EXCEEDED`, the 30s execution
cap) land here too.

## Verification

```promql
sum by (env, environment, stage) (rate(gkg_query_pipeline_failed_total{failure_reason="execution"}[5m])) * 60
```

The webserver logs carry the ClickHouse error codes. Check the graph
ClickHouse service health and current load in the ClickHouse Cloud console
(Service Read Only role suffices), and whether indexer write volume rose at
the same time.

## Resolution

- Indexing burst degrading reads: pause the indexer pools
  (`indexer.enabled: false` in the ArgoCD values); queries keep working while
  data goes stale.
- One pathological query timing out repeatedly: identify it in the logs; the
  fix is application-side query optimization, not scaling.
- ClickHouse Cloud degraded or waking from idle: `idle_scaling` makes the
  first queries after a quiet period slow; sustained failure is a support
  case with ClickHouse Cloud.

## Escalation

Ask in `#f_orbit_dev` (`context_systems`). For a ClickHouse Cloud platform
problem, escalate through the ClickHouse Cloud support channel.
