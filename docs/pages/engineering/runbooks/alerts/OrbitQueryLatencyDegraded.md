# OrbitQueryLatencyDegraded

## Overview

More than 10% of valid Orbit queries took over 10 seconds in the last 30 minutes, for at
least 10 minutes. Users see slow Orbit API responses.

The query latency SLO (95% within 5 seconds) catches a slowdown only when it burns the
budget fast. A slowdown that hits a few in ten queries for an hour stays under its burn
thresholds, so this alert covers it. On 2026-10-22 a ClickHouse overload made queries slow
from about 16:40 UTC. This alert would have fired at 17:05, 20 minutes before queries
started to fail.

The alert needs at least 100 valid queries in the window, so a few slow queries at night do
not fire it. No paging, posts to `#f_orbit_alerts`.

## Services

- [Orbit service overview](../README.md)
- Owner team: `context_systems`

## Metrics

`gkg_query_pipeline_duration_seconds` from the webserver pods, the same histogram as the
`orbit_query` apdex. Valid queries have `outcome!="caller_error"`. Schema calls
(`query_type="unknown"`) are left out. The alert compares the `le="10.0"` bucket with the
total.

## Verification

```promql
# Share of valid queries over 10 s, last 30 minutes
1 - sum(rate(gkg_query_pipeline_duration_seconds_bucket{env="orbit-prd", outcome!="caller_error", query_type!="unknown", le="10.0"}[30m]))
  / sum(rate(gkg_query_pipeline_duration_seconds_bucket{env="orbit-prd", outcome!="caller_error", query_type!="unknown", le="+Inf"}[30m]))

# Slow queries by type
sum by (query_type) (increase(gkg_query_pipeline_duration_seconds_bucket{env="orbit-prd", outcome!="caller_error", le="+Inf"}[30m]))
  - sum by (query_type) (increase(gkg_query_pipeline_duration_seconds_bucket{env="orbit-prd", outcome!="caller_error", le="10.0"}[30m]))
```

- [Webserver dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-webserver):
  latency of successful queries and p95 by query type.

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| All query types slow, ClickHouse CPU or memory high | Graph ClickHouse overloaded (heavy ad hoc queries, indexing burst) | Find the heavy queries in `system.query_log`, stop them, pause indexing if needed ([Emergency actions](../README.md#emergency-actions)) |
| One query type slow | One caller sends heavy queries | Find the caller in the webserver logs |
| Slow and server errors rise | ClickHouse unreachable or failing | See [orbit-query](orbit-query.md) |

## Escalation

- Ask in `#f_orbit_dev` (team channel for `context_systems`).

## Definitions

- [Alert rule](https://gitlab.com/gitlab-com/runbooks/-/blob/master/libsonnet/alerts/orbit-alerts.libsonnet), then run `make generate`.

## Related Links

- [Related alerts](../monitoring.md#alerts)
- [orbit-query](orbit-query.md) for the latency and error SLOs.
