---
alert: GKGPipelineInvariantViolated
expr: |
  sum by (env, environment, stage) (rate(gkg_query_engine_compiler_rejected_total{failure_reason=~"lowering|enforcement|codegen|pipeline"}[5m])) > 0
for: 1m
severity: critical
annotations:
  summary: The query compiler reached a state that upstream validation should have prevented; the generated SQL could have been incorrect.
---

## Overview

The compiler rejected a query in a late stage (`lowering`, `enforcement`,
`codegen` or `pipeline`) that earlier validation should have made
unreachable. Each rejection is a compiler bug surfacing, and the same broken
state could in another variant produce incorrect SQL instead of a rejection.
Any non-zero count is anomalous.

## Verification

Split the rejections by stage to see which invariant broke:

```promql
sum by (failure_reason) (rate(gkg_query_engine_compiler_rejected_total{failure_reason=~"lowering|enforcement|codegen|pipeline"}[5m]))
```

Find the rejected query in the webserver logs (`mode: webserver`) around the
alert window; the rejection log carries the failing query shape.

## Resolution

There is no operational fix: this is a code bug in the query compiler. If the
rejections started with a deploy, roll back the image tag. Capture the failing
query from the logs and file an issue with it; the fuzzers
(`mise run fuzz:compile`) can usually reproduce the class of failure.

## Escalation

Escalate to `context_systems` (`#f_orbit_dev`) with the failing query
attached. Treat a sustained rate as a rollback trigger, not something to
wait out.
