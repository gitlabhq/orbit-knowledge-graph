# OrbitPipelineInvariantViolated

Covers `OrbitPipelineInvariantViolated`.

The query compiler reached a state that its earlier checks must stop. It rejected the query, but a similar query can produce incorrect SQL. The caller gets an error.

## Check

1. In [Grafana Explore](https://dashboards.gitlab.net/explore), data source `mimir-analytics-eventsdot`, find the failed stage. Healthy: `0`.

   ```promql
   sum by (failure_reason) (rate(gkg_query_engine_compiler_rejected_total{failure_reason=~"lowering|enforcement|codegen|pipeline"}[5m]))
   ```

1. Read the [logs](https://log.gprd.gitlab.net/app/r/s/DhOL5) with the filter `json.mode: webserver` at the alert time. Copy the rejected query from the rejection log line.
1. Look for a recent deploy. Healthy: the newest ReplicaSet is older than the alert.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get rs --sort-by=.metadata.creationTimestamp | grep orbit-webserver | tail -1
   ```

## Act

| You see | Do this | Done when |
| --- | --- | --- |
| Started with a deploy | Roll back ([rollback](../maintenance.md#rollback)). | The rejection rate is `0`. |
| No deploy before the alert | Page Orbit tier-2. Give them the rejected query and the stage. | Orbit tier-2 has the query. |
| The rate increases | Page Orbit tier-2 now. Do not wait for the alert to resolve. | Orbit tier-2 has the query. |

This alert has no operational fix. It shows a code bug in the query compiler.

## Escalate

Page Orbit tier-2 in incident.io with the rejected query ([escalation](../README.md#escalation)).
Dedicated: the EOC runs `/inc escalate` and selects Orbit.
