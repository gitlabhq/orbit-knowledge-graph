# Orbit runbook

Orbit builds a graph of GitLab data and code. Users reach it only through GitLab: the Orbit
API, MCP, agents and Code Intelligence. If Orbit fails, Orbit features fail. GitLab
continues to work.

| | |
| --- | --- |
| Owner | Context Systems team, Slack `#f_orbit_dev` |
| On-call | Orbit tier-2 in incident.io: [schedule](https://app.incident.io/gitlab/on-call/schedules/01KZY9R2WH2G8GDNYMVCCFGKMP) |
| Alerts | GitLab.com: `#f_orbit_alerts`, no pages. Dedicated: pages through incident.io. |
| Dashboards | [Webserver](https://dashboards.gitlab.net/d/orbit-orbit-gkg-webserver), [Indexer](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer), [all](https://dashboards.gitlab.net/dashboards/f/orbit/orbit). Logs: [Kibana](https://log.gprd.gitlab.net/app/r/s/DhOL5). |

| | GitLab.com | Dedicated |
| --- | --- | --- |
| Cluster | GKE `orbit-prd` in project `gl-orbit-prd`. Staging: `orbit-stg` | Each tenant's EKS cluster. Only the Dedicated EOC runs commands. |
| Namespaces | Orbit `gkg`, NATS `nats`, Siphon `siphon` | Same |
| Deployments | `orbit-webserver`, `orbit-indexer-sdlc`, `orbit-indexer-code`, `orbit-dispatcher`, `orbit-health-check` | Same, named `gkg-*` |

Get the GitLab.com cluster credentials once:

```shell
gcloud container clusters get-credentials orbit-prd --region us-east1 --project gl-orbit-prd
```

## First 5 minutes

1. Check health, one real query (project `gitlab-org/orbit/knowledge-graph`) and the pods.
   Healthy: `status` says `healthy`, the query returns one project, all pods are `Running`.

   ```shell
   GITLAB_HOST=gitlab.com glab orbit status
   GITLAB_HOST=gitlab.com glab orbit query 'MATCH (p:Project) WHERE p.id = 77960826 RETURN p.id, p.full_path LIMIT 1'
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get pods
   ```

2. Image changed recently? Check it in [troubleshooting](troubleshooting.md#order-of-checks).
   If yes, [roll back](maintenance.md#rollback).
3. Gitaly overloaded? [Stop code indexing](#emergency-actions). ClickHouse overloaded? Stop
   code indexing first. Still overloaded: stop SDLC indexing.
4. Users affected? In Slack, run `/inc declare`. Add the label `Service::Orbit`.
5. No cause after 15 minutes? In the incident channel, run `/inc escalate` and select Orbit.
6. Other checks: [troubleshooting](troubleshooting.md).

## Emergency actions

<details open>
<summary>GitLab.com</summary>

Do not use `kubectl scale` on an indexer. KEDA changes the count back, or keeps a wrong count
while the pool is paused.

**Stop code indexing:**

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas="0" --overwrite
```

**Stop SDLC indexing:**

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg annotate scaledobject orbit-indexer-sdlc autoscaling.keda.sh/paused-replicas="0" --overwrite
```

Check: PAUSED shows `True` and the pool shows `0/0`. Ignore the two KEDA warnings.

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get scaledobject,deploy
```

Effect: that data goes stale. Queries continue.

**Resume** when the overload is gone. Done when the pool shows its minimum READY count.

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas-
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg annotate scaledobject orbit-indexer-sdlc autoscaling.keda.sh/paused-replicas-
```

</details>

<details>
<summary>Dedicated (the Dedicated EOC runs the commands)</summary>

**Stop code indexing:**

```shell
kubectl -n gkg annotate scaledobject gkg-indexer-code autoscaling.keda.sh/paused-replicas="0" --overwrite
```

**Stop SDLC indexing.** This pool has no KEDA. Write down the second number in READY (for
example 2 in `1/2`). The next maintenance run sets the count back.

```shell
kubectl -n gkg get deploy gkg-indexer-sdlc
kubectl -n gkg scale deploy/gkg-indexer-sdlc --replicas=0
```

**Resume:**

```shell
kubectl -n gkg annotate scaledobject gkg-indexer-code autoscaling.keda.sh/paused-replicas-
kubectl -n gkg scale deploy/gkg-indexer-sdlc --replicas=<count you wrote down>
```

**Turn Orbit off for users:** ask the Dedicated EOC to set `orbit.application_enabled: false`
and run an emergency maintenance. Then stop both indexers.

> [!warning]
> Do not set `orbit.infra_enabled` to `false` in an incident. It deletes the Orbit graph.

</details>

## If X fails

| Part | If it fails |
| --- | --- |
| Webserver | Orbit API returns errors. |
| SDLC or code indexer | That data goes stale. |
| Dispatcher | No new indexing jobs. After a deploy with a new schema version, indexers wait. |
| NATS (owned by Orbit) | All indexing and Siphon stop. New webserver pods are not ready. |
| Graph ClickHouse `orbit-production-us-east1` (owned by Orbit) | Queries and indexing fail. |
| Siphon or datalake ClickHouse | No new SDLC data. |
| GitLab API or Workhorse | Orbit API is down even when Orbit is healthy. Code indexing stops. |
| Gitaly | Code indexing stops. Code tools return no file content. |

## Escalation

- Orbit: in the incident channel, run `/inc escalate` and select Orbit.
- Siphon or the datalake ClickHouse: Slack `#g_analytics_platform_insights`
  ([Siphon runbook](https://runbooks.gitlab.com/siphon/)).
- Gitaly, PostgreSQL or the GitLab API: the on-call of that GitLab.com service.
- Dedicated: the Dedicated EOC owns the tenant and runs `/inc escalate` for Orbit.

<details>
<summary>Get this access before your shift</summary>

| | GitLab.com | Dedicated |
| --- | --- | --- |
| Cluster | Issue in `gitlab-com/gl-security/corp/issue-tracker`, template `gcp_project_iam_update` | Dedicated SREs only, through break-glass |
| Deploys | ArgoCD through Lumos, role `knowledge-graph-members` | Switchboard (Dedicated team) |
| ClickHouse console | SAML, Service Reader with SQL console read only | Break-glass only |
| Grafana and logs | Standard GitLab.com access | Lumos tile "GitLab Dedicated Observability" |

</details>

More: [troubleshooting](troubleshooting.md), [maintenance](maintenance.md),
[monitoring](monitoring.md), [alerts](alerts/), [NATS](nats.md),
[rebuild and reindex](manual_rebuild.md), [disaster recovery](disaster-recovery/README.md),
[secret rotation](secret_rotation.md), [SDLC indexing](sdlc_indexing.md),
[code indexing](code_indexing.md), [architecture](../architecture/README.md).
