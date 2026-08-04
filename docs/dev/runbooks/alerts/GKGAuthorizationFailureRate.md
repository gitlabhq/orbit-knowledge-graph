---
alert: GKGAuthorizationFailureRate
expr: |
  sum by (env, environment, stage) (rate(gkg_query_pipeline_failed_total{failure_reason="authorization"}[5m])) * 60 > 1
for: 5m
severity: warning
annotations:
  summary: The Rails authorization exchange is failing, above 1 failure per minute; redaction callbacks are not completing.
---

## Overview

Queries whose results need redaction are failing because the authorization
exchange with Rails is not completing. In the query flow, GKG sends resource
ids back over the Workhorse stream and Workhorse calls the Rails internal
redaction endpoint with the user's credentials; if Rails is slow, erroring,
or unreachable, the query fails rather than returning unredacted rows.

## Verification

```promql
sum by (env, environment, stage) (rate(gkg_query_pipeline_failed_total{failure_reason="authorization"}[5m])) * 60
```

Check GitLab webservice health (this is the Rails internal API, so Rails
error rates and saturation apply) and the webserver logs for the redaction
call errors. If the GitLab side is healthy, check whether the failures
correlate with one user or namespace: an oversized authorization payload can
fail deterministically.

## Resolution

- Rails degraded: this resolves when the GitLab webservice recovers; nothing
  to do on the GKG side. Queries not needing redaction keep working.
- Failures pinned to one query shape or namespace: capture it from the logs
  and file an issue; the redaction batch size may need tuning.
- Started with a GitLab deploy: the internal endpoint may have regressed;
  check recent changes to the Orbit internal API in the monolith.

## Escalation

Ask in `#f_orbit_dev` (`context_systems`). If the Rails internal API itself
is down, follow the standard GitLab production escalation instead.
