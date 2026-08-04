---
alert: GKGCircuitBreakerRejectRateHigh
expr: |
  sum by (env, environment, stage) (rate(gkg_circuit_breaker_calls_rejected_total[5m])) * 60 > 10
for: 5m
severity: warning
annotations:
  summary: A circuit is open and shedding significant traffic, above 10 rejected calls per minute.
---

## Overview

An open circuit breaker is rejecting more than 10 calls per minute, meaning
real traffic is being shed rather than served. Where `GKGCircuitBreakerOpen`
signals that a breaker tripped at all, this alert quantifies sustained user
impact: the dependency has been down long enough for meaningful load to pile
into the open breaker.

## Verification

```promql
sum by (service) (rate(gkg_circuit_breaker_calls_rejected_total[5m])) * 60
```

The `service` label names the dependency being shed. Correlate with the
error-rate alerts for that dependency and with
`gkg_circuit_breaker_state_transitions_total` to see whether the breaker is
stuck open or flapping.

## Resolution

Same paths as [GKGCircuitBreakerOpen](GKGCircuitBreakerOpen.md): restore the
dependency behind the named breaker. Because rejections here scale with
incoming traffic, consider whether the callers retrying into the open breaker
should back off; sustained high rejection with a recovered dependency means
the breaker's probe never succeeds, which is a bug worth filing with the
breaker name.

## Escalation

Ask in `#f_orbit_dev` (`context_systems`). If user-facing error rates are
elevated at the same time, treat the incident at the dependency's severity,
not this alert's.
