---
alert: GKGValidationFailedBurst
expr: |
  sum by (env, environment, stage) (rate(gkg_query_engine_compiler_rejected_total{failure_reason=~"parse|schema|reference|pagination"}[5m])) * 60 > 10
for: 5m
severity: warning
annotations:
  summary: Sustained burst of structural validation failures, above 10 per minute; a broken client or someone probing the API.
---

## Overview

The compiler is rejecting queries for structural problems (`parse`, `schema`,
`reference`, `pagination`) at more than 10 per minute for 5 minutes. These
rejections are the validation layer working as designed; the alert exists
because a sustained burst means either a client is systematically sending
broken queries or someone is probing the endpoint.

## Verification

```promql
sum by (failure_reason) (rate(gkg_query_engine_compiler_rejected_total{failure_reason=~"parse|schema|reference|pagination"}[5m])) * 60
```

Pull the rejection logs and group by caller: one correlation-id cluster or
user means a broken client, a spread across callers after a release means a
query-producing integration (for example the Duo agent tooling) regressed.

## Resolution

- Broken internal client: file an issue against the calling feature with a
  sample rejected query from the logs.
- After a GKG release: a schema or ontology change may have invalidated
  previously valid queries; check the release notes for ontology changes and
  roll back if unintended.
- External probing: rate-limiting happens in Rails (`orbit_query` rate
  limit); no GKG-side action.

## Escalation

Ask in `#f_orbit_dev` (`context_systems`). This alert alone is not
user-impacting; escalate only if it accompanies error-rate alerts.
