# OrbitAuthFilterMissing

Covers `OrbitAuthFilterMissing`.

A query reached the query compiler without a valid security context. The compiler rejected the query, so no data leaked, but a check before the compiler failed. The caller gets an error.

Treat it as a security alert. Page Orbit tier-2 now, then do the checks.

## Check

1. In [Grafana Explore](https://dashboards.gitlab.net/explore), data source `mimir-analytics-eventsdot`, find the source of the rejections. Healthy: `0`.

   ```promql
   sum by (job) (rate(gkg_query_engine_compiler_rejected_total{failure_reason="security"}[5m]))
   ```

1. Read the [logs](https://log.gprd.gitlab.net/app/r/s/DhOL5) with the filter `json.resource.labels.container_name: orbit-webserver` at the alert time. Find the compiler rejection and write down the caller.
1. Look for a recent deploy of GitLab or Orbit. Healthy: the newest Orbit ReplicaSet is older than the alert.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get rs --sort-by=.metadata.creationTimestamp | grep orbit-webserver | tail -1
   ```

1. Look for a recent JWT key rotation ([secret rotation](../secret_rotation.md)).

## Act

| You see | Do this | Done when |
| --- | --- | --- |
| The requests do not come from a known GitLab Rails caller | [Engage SIRT](https://handbook.gitlab.com/handbook/security/security-operations/sirt/engaging-security-on-call/). Give them the caller from the logs. | SIRT owns the incident. |
| One caller with a bad or old JWT, after a key rotation | Finish the rotation on the old side ([secret rotation](../secret_rotation.md)). | The rejection rate is `0`. |
| Started with a GitLab or Orbit deploy | Roll back that side ([rollback](../maintenance.md#rollback)). | The rejection rate is `0`. |

## Escalate

Page Orbit tier-2 in incident.io now ([escalation](../README.md#escalation)). Unknown caller: engage SIRT.
Dedicated: this alert goes through incident.io. The EOC runs `/inc escalate` and selects Orbit.
