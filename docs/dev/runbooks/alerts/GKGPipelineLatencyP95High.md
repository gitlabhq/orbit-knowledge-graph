---
alert: GKGPipelineLatencyP95High
expr: |
  histogram_quantile(0.95, sum by (env, environment, stage, le) (rate(gkg_query_pipeline_duration_seconds_bucket[5m]))) > 5
for: 10m
severity: warning
annotations:
  summary: p95 end-to-end query pipeline latency is above 5 seconds for 10 minutes.
---

## Overview

The 95th percentile of end-to-end pipeline latency (compile, authorize,
execute, redact) exceeds 5 seconds. Normal query latency is 2-5s, so this
threshold marks the top of the healthy range. ClickHouse execution time
dominates the distribution; compilation and redaction are normally
milliseconds.

## Verification

```promql
histogram_quantile(0.95, sum by (env, environment, stage, le) (rate(gkg_query_pipeline_duration_seconds_bucket[5m])))
```

Compare against the execution failure rate and indexer write volume for the
same window. Latency rising while indexer throughput rises means read/write
contention on the shared graph ClickHouse. Latency rising with a flat write
rate points at query mix (heavier queries) or ClickHouse Cloud replica
pressure.

## Resolution

- Read/write contention: pause or scale down the indexer pools; queries
  recover immediately, indexing resumes later without loss.
- Sustained heavier query mix: ClickHouse Cloud vertical autoscaling has a
  configured maximum; raising it is a terraform change in config-mgmt
  (`environments/orbit-prd/clickhouse-cloud.tf`).
- After an idle period: `idle_scaling` wake-up latency self-resolves in
  minutes; do not scale for it.

## Escalation

Ask in `#f_orbit_dev` (`context_systems`).
