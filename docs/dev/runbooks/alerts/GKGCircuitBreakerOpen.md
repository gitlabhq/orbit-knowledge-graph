---
alert: GKGCircuitBreakerOpen
expr: |
  sum by (env, environment, stage) (rate(gkg_circuit_breaker_state_transitions_total{to="open"}[5m])) > 0
for: 1m
severity: warning
annotations:
  summary: A circuit breaker opened; an external dependency is unreachable and calls to it are being shed.
---

## Overview

A circuit breaker transitioned to open, meaning repeated failures against an
external dependency (ClickHouse, the GitLab internal API, NATS) crossed the
breaker's threshold and GKG is now failing calls fast instead of waiting on
timeouts. This protects the rest of the service, but whatever sits behind
the open breaker is effectively down for GKG.

## Verification

```promql
sum by (service) (rate(gkg_circuit_breaker_state_transitions_total{to="open"}[5m]))
```

The `service` label names the dependency. Check that dependency's own health
directly (ClickHouse Cloud console, GitLab webservice status, NATS pods) and
watch for `half_open` transitions, which mean the breaker is probing for
recovery.

## Resolution

The breaker itself needs no intervention: it closes automatically once the
dependency answers its probes. Fix or wait out the underlying dependency:

- ClickHouse breaker: see [GKGExecutionFailureRate](GKGExecutionFailureRate.md)
  resolution paths.
- GitLab internal API breaker: see
  [GKGAuthorizationFailureRate](GKGAuthorizationFailureRate.md).
- A breaker flapping open/closed repeatedly without a clear dependency
  outage suggests a threshold tuned too tight; file an issue with the
  breaker name and the window.

## Escalation

Ask in `#f_orbit_dev` (`context_systems`), naming the dependency (`service` label) from the
verification query.
