# Orbit NATS alerts

Covers `OrbitServiceNatsServerErrorSLOViolation`, `OrbitServiceNatsServerTrafficCessation`, `OrbitServiceNatsServerTrafficAbsent`, `OrbitServiceNatsJetstreamErrorSLOViolation`, `OrbitServiceNatsJetstreamTrafficCessation`, `OrbitServiceNatsJetstreamTrafficAbsent`.

Messages between Siphon, the dispatcher and the indexers stop, or consumers fail to process them. Users see old data in Orbit results. Queries continue to work.

> [!warning]
> Do not delete streams or KV buckets. Siphon uses the same NATS. Do not purge NATS.

## Check

On Dedicated, only the Dedicated EOC runs commands. Use no `--context`.

1. Check the NATS pods. Healthy: all pods are `Running`, READY is full and all PVCs are `Bound`.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n nats get statefulset,pod,pvc
   ```

1. Check the indexer pods. Healthy: all pods are `Running` and RESTARTS does not increase.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get pods
   ```

1. Open the [NATS dashboard](https://dashboards.gitlab.net/d/orbit-orbit-nats). Healthy: slow consumers are 0, redeliveries and ack pending are flat.
1. Check stream and consumer health from nats-box ([NATS](../nats.md)). Healthy: no consumer has a growing ack pending count.

## Act

| You see | Do this | Done when |
| --- | --- | --- |
| Started with a deploy | [Roll back](../maintenance.md#rollback). | Slow consumers are 0 and redeliveries are flat. |
| Indexer pods restart or show `OOMKilled` | Fix the indexers, not NATS: [SDLC indexer](gkg-indexer-sdlc.md), [code indexer](gkg-indexer-code.md), [OOM kills](OrbitContainerOOMKilled.md). | Slow consumers are 0. |
| Redeliveries on one subject only | One message fails on each attempt. Find the failed handler in the [logs](https://log.gprd.gitlab.net/app/r/s/DhOL5) and page Orbit tier-2. | Redeliveries are flat. |
| Redeliveries on all subjects, graph ClickHouse is slow | [Stop one indexer pool](../README.md#emergency-actions). | Redeliveries go down. |
| Traffic stopped, NATS pods are healthy | The producers stopped. Follow [dispatcher alerts](gkg-dispatcher.md) and check the [Siphon dashboard](https://dashboards.gitlab.net/d/orbit-orbit-siphon). | Message rate is above 0. |
| A NATS pod is not `Running` or a PVC is not `Bound` | Run `kubectl describe` on the pod and page Orbit tier-2. Orbit streams have one replica. | All NATS pods are `Running`. |
| NATS disk usage grows | [Add indexer capacity](../maintenance.md#capacity). Disk steps: [NATS operations](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/nats/operations.md). | Disk usage stops growing. |
| Indexer logs show a stream or consumer error after a deploy | Page Orbit tier-2. | Consumers process messages again. |

## Escalate

Page Orbit tier-2 in incident.io ([escalation](../README.md#escalation)). Siphon producer problem: ask in `#g_analytics_platform_insights`.
Dedicated: the EOC runs `/inc escalate` and selects Orbit.
