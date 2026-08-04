---
alert: GKGSecurityRejected
expr: |
  sum by (env, environment, stage) (rate(gkg_query_pipeline_failed_total{failure_reason="security"}[5m])) > 0
for: 5m
severity: warning
annotations:
  summary: The query pipeline is rejecting requests for invalid or missing security context.
---

## Overview

Requests are failing the pipeline's security check: the JWT is missing,
expired, unverifiable, or lacks the claims the pipeline needs. Unlike
`GKGAuthFilterMissing` (a compiler-level invariant breach), this is the
expected rejection path doing its job; the alert exists because a sustained
rate means some caller is systematically broken, most often after a JWT key
rotation that only landed on one side.

## Verification

```promql
sum by (env, environment, stage) (rate(gkg_query_pipeline_failed_total{failure_reason="security"}[5m]))
```

Check the webserver logs for the JWT verification errors around the window.
JWTs are HS256 with a 5 minute lifetime and a single shared key: if Rails and
GKG disagree on the key version, every request fails until both match.

## Resolution

- Key rotation in progress: finish it. Bump the pinned Vault secret version on
  whichever side still runs the old key and redeploy; the two sides cannot
  overlap keys, so the failure window closes only when both match.
- Clock skew between Rails and GKG pods can invalidate the short-lived
  tokens; compare pod clocks if rotation is ruled out.
- A single misbehaving internal caller: identify it in the logs and fix its
  token handling.

## Escalation

Ask in `#f_orbit_dev` (`context_systems`). If the requests come from an
unknown caller rather than Rails, treat it as probing and follow the security
process.
