# Troubleshooting

Back to the [runbook](README.md). No cause after 15 minutes: page Orbit tier-2.

## Order of checks

1. Check the service and run one real query. Healthy: `status` says `healthy` and the query
   returns one project.

   ```shell
   glab orbit status
   glab orbit query 'MATCH (p:Project) WHERE p.id = 77960826 RETURN p.id, p.full_path LIMIT 1'
   ```

   The project ID is for GitLab.com. On Dedicated, use a project ID from the tenant.
   If `status` works but the query fails, the problem is in the webserver or the graph
   ClickHouse. A broken webserver can still show `Running`.

2. Check the pods. Healthy: all pods are `Running`, READY is full, restarts do not increase.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get pods
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get scaledobject,hpa
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n nats get pods
   ```

3. Look for a recent deploy. If one matches the start of the problem,
   [roll back](maintenance.md#rollback).

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get deploy \
     -o 'custom-columns=NAME:.metadata.name,IMAGE:.spec.template.spec.containers[0].image'
   ```

4. Read the dashboards and logs. See [monitoring](monitoring.md) for what the panels mean.
5. Find the failed part with [If X fails](README.md#if-x-fails).

For staging, use `--context gke_gl-orbit-stg_us-east1_orbit-stg`.

## Indexing is stuck or has a high error rate

| Check | If yes |
| --- | --- |
| A deploy with a schema migration runs (indexer dashboard, Schema migration row) | Expected. Watch graph ClickHouse CPU and memory. If it is overloaded, [stop one pool](README.md#emergency-actions). |
| A pool is at its KEDA maximum and the queue grows | Raise the maximum, see [capacity](maintenance.md#capacity). |
| Pods are `Pending` | The node pool is full. Raise `gkg_pool_max_nodes`, see [capacity](maintenance.md#capacity). |
| Pods restart with OOM kills | See [OrbitContainerOOMKilled](alerts/OrbitContainerOOMKilled.md). |
| The datalake data is old too | The problem is in Siphon. See [escalation](README.md#escalation). |

Details: [SDLC indexing](sdlc_indexing.md), [code indexing](code_indexing.md).

## Gitaly overload

Code indexing downloads repository archives through GitLab. A large backfill can overload
one Gitaly node, as in incident
[INC-15163](https://app.incident.io/gitlab/incidents/15163) on a Dedicated tenant.

Symptoms: Gitaly apdex alerts, one Gitaly node with high CPU.

Confirm that Orbit is the cause:

1. In the Gitaly logs, group the request time by `grpc.method`. `GetArchive` is at the top.
2. Filter `GetArchive` by `grpc.meta.client_name`: `gkg-indexer` or `orbit-indexer`.

Actions:

1. [Stop code indexing](README.md#emergency-actions). Do not stop the SDLC indexer.
2. Check that Gitaly CPU goes down.
3. Resume slowly. Pin the pool to 1 pod:

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas="1" --overwrite
   ```

   Watch Gitaly CPU for 15 minutes. If it stays normal, resume fully:

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas-
   ```

   On Dedicated, use `gkg-indexer-code` without `--context`.

4. If one Gitaly node holds most repositories, keep code indexing stopped. Ask the Gitaly
   on-call to rebalance the storage.

## Queries are slow or fail

1. A reindex runs (Schema migration row)? Indexing writes slow down queries.
2. Check graph ClickHouse CPU and memory per replica (ClickHouse Cloud console,
   Monitoring > Service health).
3. ClickHouse overloaded by indexing? [Stop one pool](README.md#emergency-actions).
4. Webserver errors or OOM kills? Check the pods and logs.
5. Check GitLab API health. Each query holds a GitLab API request for its full duration.

## Common signatures

| Signature | Likely cause | Action |
| --- | --- | --- |
| `query` fails, `status` works, webserver logs show `panic` | A bad release | [Roll back](maintenance.md#rollback). |
| `TIMEOUT_EXCEEDED` from ClickHouse | A heavy query hits the ClickHouse execution timeout | Check ClickHouse load and the query. |
| Dispatcher not ready after a deploy | A schema migration runs or its lock is held. The dispatcher restarts after about 5 minutes. | Wait 15 minutes. If it is still not ready, read the dispatcher logs and page Orbit tier-2. |
| New webserver pods are not ready | No active schema in NATS, or NATS is down | Check NATS and the dispatcher. |
| HTTP 422 `billing_namespace_required` for one user | The user has more than one paid Orbit group and no default group | The user sets Preferences > GitLab Orbit > Default group. |
| TLS or authentication errors between Rails and Orbit | Certificate or JWT key mismatch | See [secret rotation](secret_rotation.md#if-a-rotation-breaks-connections). |
| JetStream disk grows | Consumers lag | Find the stream that grows. If it is an Orbit stream, see [capacity](maintenance.md#capacity). |
| Dedicated: indexer logs show many 401 errors | Application is off, infra is on | Stop both indexers, see [emergency actions](README.md#emergency-actions). |

`glab orbit` exit codes:

| Code | Meaning |
| --- | --- |
| 2 | Not found: Orbit is off, or the project or group does not exist |
| 3 | Not authenticated |
| 4 | Access denied |
| 5 | Rate limited |
