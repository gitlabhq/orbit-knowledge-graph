# Orbit dispatcher alerts

Covers `OrbitServiceGkgDispatcherErrorSLOViolation`, `OrbitServiceGkgDispatcherTrafficCessation`, `OrbitServiceGkgDispatcherTrafficAbsent`, `OrbitDispatchTaskStopped`.

The dispatcher does not send indexing jobs, or its tasks fail. Users see old data in Orbit results. Queries continue to work.

## Check

On Dedicated, only the Dedicated EOC runs commands. Use `gkg-dispatcher` and no `--context`.

1. Check the pod. Healthy: READY is `1/1` and RESTARTS does not increase.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get pods | grep orbit-dispatcher
   ```

1. Check the task runs in [Grafana Explore](https://dashboards.gitlab.net/explore), data source `mimir-analytics-eventsdot`. Healthy: each task has `success` runs.

   ```promql
   sum by (task, outcome) (increase(gkg_scheduler_task_runs_total{env="orbit-prd"}[30m]))
   ```

1. Read the [logs](https://log.gprd.gitlab.net/app/r/s/DhOL5) with the filter `json.mode: dispatch-indexing`. Healthy: no `task failed` or `cadence lock check failed` lines.
1. Look for a recent deploy. Healthy: the newest ReplicaSet is older than the alert.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get rs --sort-by=.metadata.creationTimestamp | grep orbit-dispatcher | tail -1
   ```

## Act

| You see | Do this | Done when |
| --- | --- | --- |
| A deploy a few minutes ago, the pod is `1/1` | No action. The pod restarts on each deploy, so a short gap is expected. | Task runs show `success` again. |
| Started with a deploy and does not recover | [Roll back](../maintenance.md#rollback). | Task runs show `success`. |
| Pod is `1/1`, but no task runs or `cadence lock check failed` | Restart the dispatcher with the commands below. | READY is `1/1` and task runs show `success`. |
| Pod is not ready after a deploy, logs show a schema migration | Wait 15 minutes. Still not ready: page Orbit tier-2. | READY is `1/1`. |
| `task failed` names the datalake or Siphon | Check the [Siphon dashboard](https://dashboards.gitlab.net/d/orbit-orbit-siphon). Siphon problem: ask in `#g_analytics_platform_insights`. | Task runs show `success`. |
| `task failed` names NATS, a stream or a KV bucket | Follow [NATS alerts](nats-slo.md). Do not delete streams or KV buckets. | Task runs show `success`. |
| Pod restarts with `OOMKilled` | Follow [OrbitContainerOOMKilled](OrbitContainerOOMKilled.md). | RESTARTS does not increase. |

Restart the dispatcher:

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg rollout restart deploy/orbit-dispatcher
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg rollout status deploy/orbit-dispatcher
```

## Escalate

No cause after 15 minutes: page Orbit tier-2 in incident.io ([escalation](../README.md#escalation)).
Dedicated: the EOC runs `/inc escalate` and selects Orbit.
