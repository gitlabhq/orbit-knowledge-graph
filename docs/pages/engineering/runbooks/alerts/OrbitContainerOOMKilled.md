# OrbitContainerOOMKilled

Covers `OrbitContainerOOMKilled`.

An Orbit container used more memory than its limit, and the kernel stopped it. The pod loses its running queries or indexing job, and repeated kills cause failed queries or old data.

## Check

On Dedicated, only the Dedicated EOC runs commands. Use `gkg-*` names and no `--context`.

1. Find the pod from the `container` label of the alert. Both indexer pools use the container
   `orbit-indexer`: check `orbit-indexer-sdlc` and `orbit-indexer-code`. Healthy: RESTARTS does
   not increase for 10 minutes.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get pods
   ```

1. Confirm the kill. Healthy: `Last State` is not `OOMKilled`.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg describe pod <pod> | grep -A3 'Last State'
   ```

1. Open the [Orbit overview dashboard](https://dashboards.gitlab.net/d/orbit-main/orbit3a-overview?orgId=1&var-environment=orbit-prd), `kube_container_rss_limit` panel. Healthy: memory is well below the limit.
1. Look for a recent deploy. Healthy: the newest ReplicaSet is older than the first kill.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get rs --sort-by=.metadata.creationTimestamp | tail -5
   ```

## Act

| You see | Do this | Done when |
| --- | --- | --- |
| Restarts do not increase after 10 minutes | No action. NATS sends the job of a killed pod again after a few minutes, so a repeated kill shows in that time. | The alert resolves. |
| Started with a deploy | Roll back ([rollback](../maintenance.md#rollback)). | RESTARTS does not increase. |
| The same container is killed again and again | Give it more memory: `resources` of `webserver`, `dispatcher` or `indexer.pools.<pool>` ([capacity](../maintenance.md#capacity)). | RESTARTS does not increase for 1 hour. |
| An indexer is killed on the same repository each time | Give the pool more memory ([capacity](../maintenance.md#capacity)). Page Orbit tier-2 with the project ID from the [logs](https://log.gprd.gitlab.net/app/r/s/DhOL5). | RESTARTS does not increase. |
| Dedicated | Ask the Dedicated EOC to give the container more memory. | RESTARTS does not increase. |

## Escalate

Kills continue after a memory increase: page Orbit tier-2 in incident.io ([escalation](../README.md#escalation)).
Dedicated: the EOC runs `/inc escalate` and selects Orbit.
