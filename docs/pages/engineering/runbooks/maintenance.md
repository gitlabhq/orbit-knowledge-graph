# Maintenance

## Rollback

<details open>
<summary>GitLab.com</summary>

> [!warning]
> Do not edit live Deployments: ArgoCD removes the change. Do not delete the
> `orbit-webserver` Service or uninstall the release: GitLab then cannot reach Orbit. A rollback
> to an older schema version can start a full reindex. Check first:
> [does a release reindex?](#does-a-release-reindex)

1. Revert the last deploy MR in
   [argocd-apps](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests) and merge it.
2. ArgoCD syncs staging and production at the same time. Watch the `service-gkg` app in
   [ArgoCD](https://argocd.gitlab.net).
3. Done when all Deployments show the old tag:

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get deploy \
     -o 'custom-columns=NAME:.metadata.name,IMAGE:.spec.template.spec.containers[0].image,READY:.status.readyReplicas'
   ```

4. If the schema version changed, the dispatcher logs show `rolling back`.

ArgoCD rejects the old image because the chart needs a newer one? Revert the chart MR too, or
page Orbit tier-2.

</details>

<details>
<summary>Dedicated</summary>

There is no Orbit-only rollback. The Orbit version comes with the Instrumentor version. In
an incident, [turn Orbit off for users](README.md#emergency-actions): that is level 1 of the
[Dedicated rollback](https://gitlab.com/gitlab-com/gl-infra/gitlab-dedicated/team/-/blob/main/runbooks/orbit.md#rollback). Use level 1 only. Ask the Dedicated EOC about an
Instrumentor rollback.

</details>

## Upgrade

<details>
<summary>GitLab.com</summary>

1. The `release-deploy-mr` job opens a deploy MR in
   [argocd-apps](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests). Check
   if it [reindexes](#does-a-release-reindex).

   > [!warning]
   > Merge a release that reindexes during working hours, and watch the reindex.

2. An Orbit team member merges the MR. Close older deploy MRs that it replaces.
3. Done when all Deployments show the new tag (command in [rollback](#rollback)).

Chart upgrades come as a `[non-prod]` and a `[prod]` Renovate MR. Merge `[non-prod]` first.

</details>

<details>
<summary>Dedicated</summary>

Instrumentor pins the Orbit version. A tenant gets a new version in its weekly maintenance
window. A faster fix needs an emergency maintenance: ask the Dedicated EOC.

</details>

## Does a release reindex?

The deploy MR shows a schema version change. To see if it reindexes, run this in the Orbit
repository. Git tags start with `v`.

```shell
git fetch origin --tags
OLD=v<running version> NEW=v<target version>
o=$(git show "${OLD}:config/versions.yaml" | yq '.schema')
n=$(git show "${NEW}:config/versions.yaml" | yq '.schema')
echo "schema $o -> $n"
git show "${NEW}:config/schema-migrations.yaml" \
  | yq ".migrations[] | select(.version > $o and .version <= $n) | [.version, .scope] | join(\" \")"
```

Use mikefarah yq v4 (`mise` installs it). Only the `schema` line shows: the release does
not reindex. Scope `*` rebuilds everything, `sdlc` or `code` rebuilds only those tables, `none` rebuilds
nothing. A version with no entry counts as `*`.

For a rollback, the second command shows nothing. If the target `schema` is lower, check its
status in the graph ClickHouse. `retired`: the rollback uses its tables, no reindex. `dropped`
or no row: full reindex.

```sql
SELECT version, status FROM gkg.gkg_schema_version FINAL ORDER BY version DESC
```

During a reindex:

- The old version serves queries, but it gets no new data until the new version is active.
- Some indexer restarts at the start are normal.
- Watch the **Schema migration** row of the
  [indexer dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer) and the graph
  ClickHouse CPU.

## Silence alerts

Before planned work, such as a reindex or a rotation, create a
[silence for Orbit](https://alerts.gitlab.net/#/silences/new?filter=%7Btype%3D%22orbit%22%2C%20env%3D%22orbit-prd%22%7D).
Set the duration to the planned window. Put your name in **Creator** and the change issue
link in **Comment**. When the work is done, open the silence and select **Expire**.

Dedicated: the Dedicated EOC creates the silence in the tenant Grafana, Alerting > Silences
([Dedicated silences](https://gitlab.com/gitlab-com/gl-infra/gitlab-dedicated/team/-/blob/main/runbooks/alert-silences.md)).

## Pause and resume indexing

For a short pause, use [emergency actions](README.md#emergency-actions). For a longer pause,
change `services/gkg/env/orbit-prd/values.yaml` in argocd-apps:

| Value | Effect |
| --- | --- |
| `dispatcher.enabled: false` | No new jobs. Jobs already in NATS continue. |
| `indexer.pools.<pool>.enabled: false` | That pool stops. Queries continue, data goes stale. |
| `indexer.enabled: false` | All indexing stops. |

These values remove the pods and their KEDA objects, so a KEDA pause is lost. Set the value
back to `true` to resume.

## Capacity

The current numbers are in argocd-apps `services/gkg/values.yaml` and
`services/gkg/env/<env>/values.yaml`.

| To get more | Change | Where |
| --- | --- | --- |
| Indexer pods | `indexer.pools.<pool>.keda.maxReplicaCount` | `env/<env>/values.yaml` |
| SDLC throughput past the shared limit | Raise `indexer.pools.sdlc.engine.topics.namespace-handler.max_ack_pending`, or lower `indexer.pools.sdlc.nats.subscriptionBufferSize` | `values.yaml` |
| Jobs per indexer pod | `indexer.pools.<pool>.engine.max_concurrent_workers` | `values.yaml` |
| Nodes for Orbit pods | `gkg_pool_max_nodes` (per zone) | config-mgmt `environments/orbit-<env>/variables.tf` |
| Room for large repositories | `tmpSizeLimit` and the ephemeral-storage limit of the code pool | `values.yaml` |
| Container memory (OOM kills) | `resources` of `webserver`, `dispatcher` or `indexer.pools.<pool>` | `values.yaml` |
| Graph ClickHouse | Maximum replica memory | config-mgmt `environments/orbit-prd/clickhouse-cloud.tf` |

A higher `max_ack_pending` can overload the graph ClickHouse during a reindex.

More webserver pods do not make queries faster. The graph ClickHouse limits query speed.
All settings: [server configuration](../development/server-configuration.md).

Dedicated: Instrumentor sets all sizes per reference architecture. A change needs an
Instrumentor release.
