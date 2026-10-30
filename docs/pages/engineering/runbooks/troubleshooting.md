# Troubleshooting

Commands use production. For staging, use `--context gke_gl-orbit-stg_us-east1_orbit-stg`.

## Order of checks

1. Run the [health check](README.md#first-5-minutes). If `status` works but the query fails,
   the problem is in the webserver or the graph ClickHouse. A broken webserver can still show
   `Running`. On Dedicated, use a project ID from the tenant.
2. Check the autoscalers and NATS. Healthy: each ScaledObject shows READY `True`, and all NATS
   pods are `Running`.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get scaledobject,hpa
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n nats get pods
   ```

3. Check the running images. A new tag that matches the start of the problem: [roll back](maintenance.md#rollback).

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get deploy \
     -o 'custom-columns=NAME:.metadata.name,IMAGE:.spec.template.spec.containers[0].image'
   ```

4. Check changes outside the Orbit image:
   [feature flags](https://gitlab.com/gitlab-com/gl-infra/feature-flag-log/-/issues/?sort=created_date&state=all&label_name%5B%5D=host%3A%3Agitlab.com&search=orbit),
   [production changes](https://gitlab.com/gitlab-com/gl-infra/production/-/issues/?sort=created_date&state=all&label_name%5B%5D=change&search=orbit),
   [k8s-workloads MRs](https://gitlab.com/gitlab-com/gl-infra/k8s-workloads/gitlab-com/-/merge_requests?scope=all&state=merged&search=knowledge%20graph).
   Also search for `gkg` and `knowledge graph`.
5. Read the dashboards and logs. [Monitoring](monitoring.md) says what the panels mean. The
   running configuration is in ConfigMap `orbit-<component>-config`. Keys that are not in it
   use `config/default.yaml`.
6. Find the failed part with [If X fails](README.md#if-x-fails).

## Indexing is stuck or has a high error rate

| Check | If yes |
| --- | --- |
| A schema migration runs (indexer dashboard, Schema migration row) | Expected. Watch graph ClickHouse CPU. Overloaded: [stop one pool](README.md#emergency-actions). |
| `get hpa` shows REPLICAS equal to MAXPODS and the queue grows | Raise the KEDA maximum, see [capacity](maintenance.md#capacity). |
| Pods are `Pending` | The node pool is full. Raise `gkg_pool_max_nodes`, see [capacity](maintenance.md#capacity). |
| Pods restart with OOM kills | See [OrbitContainerOOMKilled](alerts/OrbitContainerOOMKilled.md). |
| The datalake data is old too | The problem is in Siphon. See [escalation](README.md#escalation). |

Details: [SDLC indexing](sdlc_indexing.md), [code indexing](code_indexing.md).

## Gitaly overload

Symptoms: Gitaly apdex alerts and one Gitaly node with high CPU.

Confirm that Orbit is the cause: in the Gitaly logs, `GetArchive` requests from client
`orbit-indexer` (or `gkg-indexer`) use the most time.

1. [Stop code indexing](README.md#emergency-actions). Do not stop the SDLC indexer.
2. Check that Gitaly CPU goes down.
3. Pin the pool to 1 pod:

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas="1" --overwrite
   ```

4. Watch Gitaly CPU for 15 minutes. If it stays normal, resume fully:

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas-
   ```

5. One Gitaly node holds most repositories? Keep code indexing stopped and ask the Gitaly
   on-call to rebalance the storage.

## Queries are slow or fail

1. A reindex runs (Schema migration row)? Indexing writes slow down queries.
2. Check graph ClickHouse CPU and memory per replica (see [monitoring](monitoring.md)).
3. ClickHouse overloaded by indexing? [Stop one pool](README.md#emergency-actions).
4. Webserver errors or OOM kills? Check the pods and logs.
5. Check GitLab API health.

## Common signatures

| Signature | Likely cause | Action |
| --- | --- | --- |
| `query` fails, `status` works, webserver logs show `panic` | A bad release | [Roll back](maintenance.md#rollback). |
| `TIMEOUT_EXCEEDED` in the webserver logs | A heavy query hits the ClickHouse timeout, `query.default.max_execution_time` (15 seconds by default) | One user repeats it: page Orbit tier-2. |
| `MEMORY_LIMIT_EXCEEDED` in the webserver logs | A heavy query hits `query.default.max_memory_usage` (4 GiB by default, 16 GiB on orbit-prd) | One user repeats it: page Orbit tier-2. |
| Dispatcher not ready after a deploy | A schema migration runs or its lock is held | Wait until the Schema migration row shows no migration. Still not ready: page Orbit tier-2. |
| New webserver pods are not ready | No active schema in NATS, or NATS is down | Check NATS and the dispatcher. |
| TLS or authentication errors between Rails and Orbit | Certificate or JWT key mismatch | See [secret rotation](secret_rotation.md#if-a-rotation-breaks-connections). |
| JetStream disk grows | Consumers lag | See [NATS](nats.md) and [NATS operations](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/nats/operations.md). |
| Dedicated: indexer logs show many 401 errors | Application is off, infra is on | Stop both indexers, see [emergency actions](README.md#emergency-actions). |
| HTTP 422 `billing_namespace_required` for one user | The user has two or more paid Orbit groups and no default group | The user selects a default group in Preferences. No Orbit action. |

`glab orbit` exit codes: `2` Orbit is off, or the group or project does not exist. `3` not
authenticated. `4` access denied. `5` GitLab rate limit per user, no Orbit action.
