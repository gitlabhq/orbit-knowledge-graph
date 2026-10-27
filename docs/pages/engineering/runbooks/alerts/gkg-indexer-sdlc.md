# Orbit SDLC indexer alerts

Covers `OrbitServiceGkgIndexerSdlcErrorSLOViolation`, `OrbitServiceGkgIndexerSdlcTrafficCessation`, `OrbitServiceGkgIndexerSdlcTrafficAbsent`.

The SDLC indexer fails to write data, or it stopped. Users see old issues, merge requests and pipelines in Orbit results. Queries continue to work.

## Check

On Dedicated, only the Dedicated EOC runs commands. Use `gkg-indexer-sdlc` and no `--context`. This pool has no ScaledObject on Dedicated, so `get scaledobject` returns `NotFound`. Ignore it.

1. Check the pods and the autoscaler. Healthy: pods are `Running`, RESTARTS does not increase and PAUSED is not `True`.
   No pods in the output: the pool has 0 pods. Look at PAUSED.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get pods | grep orbit-indexer-sdlc
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get scaledobject orbit-indexer-sdlc
   ```

1. Open the [indexer dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer). Healthy: SDLC handler errors are near 0 and traffic is above 0.
1. Read the [logs](https://log.gprd.gitlab.net/app/r/s/DhOL5) with the filter `json.resource.labels.container_name: orbit-indexer`. Note the error and the namespace.
1. Look for a recent deploy. Healthy: the newest ReplicaSet is older than the alert.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get rs --sort-by=.metadata.creationTimestamp | grep orbit-indexer-sdlc | tail -1
   ```

## Act

| You see | Do this | Done when |
| --- | --- | --- |
| Started with a deploy | [Roll back](../maintenance.md#rollback). | Handler errors are near 0. |
| PAUSED is `True` | Someone stopped the pool. Ask in the incident channel. When the overload is gone, [resume](../README.md#emergency-actions). | Pods are `Running` and traffic is above 0. |
| Pods restart with `OOMKilled` | Follow [OrbitContainerOOMKilled](OrbitContainerOOMKilled.md). | RESTARTS does not increase. |
| Errors on the ClickHouse write | The graph ClickHouse is slow or down. Page Orbit tier-2. | Handler errors are near 0. |
| Traffic stopped, pods are healthy | Nothing sends work. Follow [dispatcher alerts](gkg-dispatcher.md) and check the [Siphon dashboard](https://dashboards.gitlab.net/d/orbit-orbit-siphon). | Traffic is above 0. |
| The same namespace fails on each cycle | Do not purge NATS. Page Orbit tier-2 with the error from the logs. | Errors for that namespace stop. |
| One namespace never finishes | [Reindex the namespace](../manual_rebuild.md#reindex-one-namespace-project-or-entity). | `GITLAB_HOST=gitlab.com glab orbit status --full-path <group>` shows it indexed. |
| Consumer lag grows, no errors | [Add SDLC capacity](../maintenance.md#capacity). | Lag stops growing. |

## Escalate

No cause after 15 minutes: page Orbit tier-2 in incident.io ([escalation](../README.md#escalation)).
Dedicated: the EOC runs `/inc escalate` and selects Orbit.
