# Orbit code indexer alerts

Covers `OrbitServiceGkgIndexerCodeErrorSLOViolation`, `OrbitServiceGkgIndexerCodeTrafficCessation`, `OrbitServiceGkgIndexerCodeTrafficAbsent`.

The code indexer fails to index repositories, or it stopped. Users see old code data, such as definitions and references. Queries continue to work.

## Check

On Dedicated, only the Dedicated EOC runs commands. Use `gkg-indexer-code` and no `--context`.

1. Check the pods and the autoscaler. Healthy: pods are `Running`, RESTARTS does not increase and PAUSED is not `True`.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get pods | grep orbit-indexer-code
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get scaledobject orbit-indexer-code
   ```

1. Look for Gitaly apdex alerts that fire at the same time. Healthy: none.
1. Open the [indexer dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer). Healthy: code task errors are near 0. Then read the [logs](https://log.gprd.gitlab.net/app/r/s/DhOL5) and filter on the project ID.
1. Look for a recent deploy. Healthy: the newest ReplicaSet is older than the alert.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get rs --sort-by=.metadata.creationTimestamp | grep orbit-indexer-code | tail -1
   ```

## Act

| You see | Do this | Done when |
| --- | --- | --- |
| Gitaly apdex alerts at the same time | [Stop code indexing](../README.md#emergency-actions). Follow [Gitaly overload](../troubleshooting.md#gitaly-overload). | Gitaly CPU goes down. |
| Started with a deploy | [Roll back](../maintenance.md#rollback). | Code task errors are near 0. |
| PAUSED is `True` | Someone stopped the pool. Ask in the incident channel. When the overload is gone, [resume](../README.md#emergency-actions). | Pods are `Running`. |
| Pods restart with `OOMKilled` | Follow [OrbitContainerOOMKilled](OrbitContainerOOMKilled.md). | RESTARTS does not increase. |
| Pods are evicted for ephemeral storage | Raise `tmpSizeLimit` and the ephemeral-storage limit together ([capacity](../maintenance.md#capacity)). | No new evictions. |
| Archive downloads fail | Gitaly, Workhorse or the GitLab API is down. Page the on-call of that service. | Code task errors are near 0. |
| Errors concentrate on one project | Do not purge NATS. After the last attempt, the task goes to the dead letter queue ([code indexing](../code_indexing.md)). Errors continue after 1 hour: page Orbit tier-2. | Errors for that project stop. |
| `THERE_IS_NO_COLUMN` on the ClickHouse write | Code bug. Page Orbit tier-2. | Orbit tier-2 has the issue. |

## Escalate

No cause after 15 minutes: page Orbit tier-2 in incident.io ([escalation](../README.md#escalation)).
Dedicated: the EOC runs `/inc escalate` and selects Orbit.

Past incident: [incident-management#4632](https://gitlab.com/gitlab-com/gl-infra/gitlab-dedicated/incident-management/-/issues/4632), code indexing overloaded one Gitaly node.
