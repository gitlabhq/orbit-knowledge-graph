---
alert: GKGAllowlistRejectedBurst
expr: |
  sum by (env, environment, stage) (rate(gkg_query_engine_compiler_rejected_total{failure_reason=~"ontology|ontology_internal"}[5m])) * 60 > 5
for: 5m
severity: warning
annotations:
  summary: Sustained ontology-allowlist violations, above 5 per minute; schema drift or an enumeration attempt.
---

## Overview

Queries are referencing entities, properties or relationships outside the
ontology allowlist at more than 5 per minute. Two very different causes share
this signature: a client built against a newer or older graph schema than the
server exposes (drift), or a caller enumerating the schema surface to see
what exists (probing).

## Verification

```promql
sum by (failure_reason) (rate(gkg_query_engine_compiler_rejected_total{failure_reason=~"ontology|ontology_internal"}[5m])) * 60
```

Pull the rejected names from the compiler rejection logs. Real schema drift
rejects a small stable set of names repeatedly; enumeration walks many names
once. `ontology_internal` rejections mean the query referenced internal-only
schema elements, which no legitimate client does.

## Resolution

- Schema drift: check whether a `SCHEMA_VERSION` bump or ontology change
  shipped recently and whether the caller (`glab`, Duo tooling) needs a
  release to match; `glab orbit remote schema` shows what the server exposes.
- Enumeration: rate-limiting lives in Rails; capture the caller identity from
  the logs before it stops.

## Escalation

Ask in `#f_orbit_dev` (`context_systems`). For suspected enumeration of
internal schema, follow the security process.
