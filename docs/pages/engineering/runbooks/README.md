# Orbit runbook

Start here during an incident. The linked pages have the details.

## At a glance

Orbit (also GitLab Knowledge Graph, GKG) builds a graph from GitLab SDLC data and code
repositories. Users reach it only through GitLab (`/api/v4/orbit/*`): the Orbit REST API,
Orbit MCP, Orbit agents and Code Intelligence. If Orbit fails, these features fail or show
old data. GitLab itself continues to work.

| | |
| --- | --- |
| Owner | Context Systems team, Slack `#f_orbit_dev` |
| On-call | Orbit tier-2 in incident.io: [schedule](https://app.incident.io/gitlab/on-call/schedules/01KZY9R2WH2G8GDNYMVCCFGKMP) |
| Alerts | GitLab.com: Slack `#f_orbit_alerts` and `#alerts`, they do not page. Dedicated: `#dedicated-prod-alerts` and incident.io. |
| Incident label | `gitlab-com/gl-infra/production~"Service::Orbit"` |

| | GitLab.com | Dedicated |
| --- | --- | --- |
| Cluster | GKE `orbit-prd` (`gl-orbit-prd`), staging `orbit-stg` | Each tenant's EKS cluster, primary region only |
| kubectl | `--context gke_gl-orbit-prd_us-east1_orbit-prd` | Only the Dedicated EOC |
| Namespaces | Orbit `gkg`, NATS `nats`, Siphon `siphon` | Same |
| Deployments | `orbit-webserver`, `orbit-indexer-sdlc`, `orbit-indexer-code`, `orbit-dispatcher`, `orbit-health-check` | Same, named `gkg-*` |
| Dashboards | [Webserver](https://dashboards.gitlab.net/d/orbit-orbit-gkg-webserver), [Indexer](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer), [all](https://dashboards.gitlab.net/dashboards/f/orbit/orbit) | Tenant Grafana, General folder |
| Logs | [Kibana](https://log.gprd.gitlab.net/app/r/s/DhOL5) | Tenant OpenSearch |

## First 5 minutes

1. Check health, one real query and the pods. Healthy: `status` says `healthy`, the query
   returns one project, all pods are `Running` and restarts do not increase.

   ```shell
   glab orbit status
   glab orbit query 'MATCH (p:Project) WHERE p.id = 77960826 RETURN p.id, p.full_path LIMIT 1'
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get pods
   ```

2. A deploy in the last few hours? [Roll back](maintenance.md#rollback).
3. Gitaly overloaded? [Stop code indexing](#emergency-actions). ClickHouse overloaded? Stop one
   pool.
4. Users affected? Declare an incident with the label above.
5. No cause after 15 minutes? Page Orbit tier-2.
6. Other checks: [troubleshooting](troubleshooting.md).

## Emergency actions

<details open>
<summary>GitLab.com</summary>

Do not use `kubectl scale` on an indexer. KEDA changes the count back, or keeps a wrong count
while the pool is paused.

**Stop code indexing** (Gitaly or ClickHouse overload):

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas="0" --overwrite
```

**Stop SDLC indexing:**

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg annotate scaledobject orbit-indexer-sdlc autoscaling.keda.sh/paused-replicas="0" --overwrite
```

kubectl prints two KEDA warnings. Ignore them. Check that the PAUSED column shows `True`
and the pool shows `0/0`:

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get scaledobject,deploy
```

Effect: that data goes stale. Queries continue.

**Resume** when the overload is gone. KEDA can need 2 minutes to start the pods again:

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas-
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg annotate scaledobject orbit-indexer-sdlc autoscaling.keda.sh/paused-replicas-
```

**Roll back a release:** revert the last deploy MR in
[argocd-apps](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/tree/main/services/gkg).
Steps and checks: [rollback](maintenance.md#rollback).

</details>

<details>
<summary>Dedicated (the Dedicated EOC runs the commands)</summary>

**Stop code indexing:**

```shell
kubectl -n gkg annotate scaledobject gkg-indexer-code autoscaling.keda.sh/paused-replicas="0" --overwrite
```

**Stop SDLC indexing.** This pool has no KEDA. Write down the desired count first (the
second number in READY, for example 2 in `1/2`). The next maintenance run sets it back:

```shell
kubectl -n gkg get deploy gkg-indexer-sdlc
kubectl -n gkg scale deploy/gkg-indexer-sdlc --replicas=0
```

**Resume:**

```shell
kubectl -n gkg annotate scaledobject gkg-indexer-code autoscaling.keda.sh/paused-replicas-
kubectl -n gkg scale deploy/gkg-indexer-sdlc --replicas=<count you wrote down>
```

**Turn Orbit off for users:** set `orbit.application_enabled` to `false` in the tenant model
and run maintenance. Then stop both indexers, or they loop on authentication errors.

> [!warning]
> Do not set `orbit.infra_enabled` to `false` in an incident. It deletes the Orbit graph.

There is no Orbit-only rollback. The Orbit version comes with the Instrumentor version.

</details>

## If X fails

| Part | If it fails |
| --- | --- |
| Webserver | Orbit API returns errors. |
| SDLC or code indexer | That data goes stale. |
| Dispatcher (1 replica) | No new indexing jobs. After a deploy with a new schema version, the indexers wait for it. |
| Health-check | `/api/v4/orbit/status` reports unhealthy. Indexers go to their fallback replica count. Queries continue. |
| NATS (`nats`, owned by Orbit) | All indexing stops. Siphon stops. New webserver pods do not become ready. Orbit streams have one replica, so one failed NATS node can stop them. |
| Graph ClickHouse (`orbit-production-us-east1`, owned by Orbit) | Queries and indexing fail. |
| Siphon or datalake ClickHouse (`gitlab-com-production-us-east1`) | No new SDLC data. Code push events stop. |
| GitLab API or Workhorse | Orbit API is down even when Orbit is healthy. Code indexing stops. |
| Gitaly | Code indexing stops. Code tools return no file content. |
| GitLab PostgreSQL | No new SDLC data. |

On Dedicated, one ClickHouse service per tenant holds the datalake and the graph.

## Escalation

- Orbit: page Orbit tier-2 in incident.io.
- Siphon or the datalake ClickHouse: Analytics Platform Insights, Slack
  `#g_analytics_platform_insights` ([Siphon runbook](https://runbooks.gitlab.com/siphon/)).
- Gitaly, PostgreSQL or the GitLab API: the on-call of that GitLab.com service.
- Dedicated: the Dedicated EOC owns the tenant and runs all commands. To get Orbit help, the
  EOC runs `/inc escalate` and selects the Orbit team.

## Get this access before your shift

| | GitLab.com | Dedicated |
| --- | --- | --- |
| Cluster | GCP project access: issue in `gitlab-com/gl-security/corp/issue-tracker`, template `gcp_project_iam_update` | Dedicated SREs only, through break-glass |
| Deploys | ArgoCD through Lumos, role `knowledge-graph-members` | Switchboard (Dedicated team) |
| ClickHouse console | SAML, Service Reader with SQL console read only | Break-glass only |
| Grafana and logs | Standard GitLab.com access | Lumos tile "GitLab Dedicated Observability" |

## All pages

| Page | Use it for |
| --- | --- |
| [Troubleshooting](troubleshooting.md) | Symptoms, checks and actions |
| [Monitoring](monitoring.md) | Dashboards, SLOs, alerts and their playbooks |
| [Maintenance](maintenance.md) | Rollback, upgrade, reindex after a release, pause, capacity |
| [Rebuild and reindex](manual_rebuild.md) | Rebuild the graph, reindex a namespace or project |
| [Secret rotation](secret_rotation.md) | JWT key, ClickHouse passwords, certificates |
| [SDLC indexing](sdlc_indexing.md), [code indexing](code_indexing.md) | Pipeline details |
| [Server configuration](server_configuration.md) | Configuration reference |
| [Architecture](../architecture/README.md) | How Orbit works |
