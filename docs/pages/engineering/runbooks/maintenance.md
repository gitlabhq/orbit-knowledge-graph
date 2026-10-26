# Maintenance

Back to the [runbook](README.md).

## Rollback

<details open>
<summary>GitLab.com</summary>

> [!warning]
> A rollback two or more schema versions back starts a full reindex. Check the versions
> first, see [does a release reindex?](#does-a-release-reindex).

1. Revert the last deploy MR in
   [argocd-apps](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/tree/main/services/gkg),
   or set `image.tag` back in `services/gkg/env/orbit-stg/values.yaml` and
   `services/gkg/env/orbit-prd/values.yaml`. Merge it.
2. ArgoCD syncs staging first, then production.
3. Done when all Deployments show the old tag:

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get deploy \
     -o 'custom-columns=NAME:.metadata.name,IMAGE:.spec.template.spec.containers[0].image,READY:.status.readyReplicas'
   ```

Do not edit live Deployments. ArgoCD removes the change at the next sync. If ArgoCD rejects
the old image because the chart needs a newer one, revert the chart MR too, or page Orbit
tier-2.

If the release changed the schema version:

- The new version was not active yet: nothing to do.
- The new version was active: the dispatcher makes the old tables active again by itself.
  Look for `rolling back to the embedded version` in the dispatcher logs.
- Two or more versions back: a full reindex starts. The dispatcher logs
  `rolling back via rebuild`.

</details>

<details>
<summary>Dedicated</summary>

There is no Orbit-only rollback. The Orbit version comes with the Instrumentor version. In
an incident, [turn Orbit off for users](README.md#emergency-actions) and ask the Dedicated
EOC about an Instrumentor rollback.

</details>

## Upgrade

<details>
<summary>GitLab.com</summary>

1. The `release-deploy-mr` job opens a deploy MR in
   [argocd-apps](https://gitlab.com/gitlab-com/gl-infra/argocd/apps/-/merge_requests). It sets
   `image.tag` for staging and production. The MR says if the release has a schema migration.

   > [!warning]
   > Merge a release with a schema migration during working hours, and watch the reindex.

2. An Orbit team member merges the MR. Close older deploy MRs that it replaces.
3. ArgoCD syncs `orbit-stg` first, then `orbit-prd`.
4. Done when all Deployments show the new tag (command in [rollback](#rollback)).

Chart upgrades: Renovate opens a `[non-prod]` and a `[prod]` MR. Merge `[non-prod]` first.
CI does not stop you from merging `[prod]` first.

</details>

<details>
<summary>Dedicated</summary>

- Instrumentor pins the Orbit image and chart. Renovate opens the upgrade MR and the
  Dedicated team merges it.
- A tenant gets the new version in its weekly maintenance window. Tenants can run different
  Orbit versions.
- A fix cannot reach a tenant before its next window, except in an emergency maintenance.
  Ask the Dedicated EOC.

</details>

## Does a release reindex?

Only a release that changes the schema version reindexes. The deploy MR says so. To compare
two tags, run this in the Orbit repository:

```shell
git fetch origin --tags
OLD=v0.155.0 NEW=v0.159.0
o=$(git show "${OLD}:config/versions.yaml" | yq '.schema')
n=$(git show "${NEW}:config/versions.yaml" | yq '.schema')
echo "schema $o -> $n"
git show "${NEW}:config/schema-migrations.yaml" \
  | yq ".migrations[] | select(.version > $o and .version <= $n) | [.version, .scope] | join(\" \")"
```

Scope `*` rebuilds everything, `sdlc` or `code` rebuilds only those tables, `none` rebuilds
nothing. A version with no entry counts as `*`.

During a reindex:

- The old version serves queries, but it gets no new data until the new version is active.
- A full reindex of GitLab.com takes about 4 hours.
- Some indexer restarts at the start are normal. The indexers wait for the dispatcher to
  prepare the new version.
- Watch the **Schema migration** row of the
  [indexer dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer) and the graph
  ClickHouse CPU and memory.

## Pause and resume indexing

For a short pause, use [emergency actions](README.md#emergency-actions). For a longer pause on
GitLab.com, change the ArgoCD values in `services/gkg/env/orbit-prd/values.yaml`:

| Value | Effect |
| --- | --- |
| `dispatcher.enabled: false` | No new jobs. Jobs already in NATS continue. |
| `indexer.pools.<pool>.enabled: false` | That pool stops. Queries continue, data goes stale. |
| `indexer.enabled: false` | All indexing stops. |

These values remove the pods and their KEDA objects, so a KEDA pause is lost. Jobs that were
in progress go back to the queue after the NATS acknowledgement timeout.

Set the value back to `true` to resume. The dispatcher sends the missing work again, so the
graph catches up.

## Capacity

GitLab.com values are in argocd-apps `services/gkg/values.yaml` and
`services/gkg/env/<env>/values.yaml`. Read the current numbers there.

| To get more | Change | Where |
| --- | --- | --- |
| Indexer pods | `indexer.pools.<pool>.keda.maxReplicaCount` | `env/<env>/values.yaml` |
| SDLC throughput past the shared limit | Raise `indexer.pools.sdlc.engine.topics.namespace-handler.max_ack_pending`, or lower `nats.subscriptionBufferSize` | `values.yaml` |
| Jobs per indexer pod | `indexer.pools.<pool>.engine.max_concurrent_workers` | `values.yaml` |
| Nodes for Orbit pods | `gkg_pool_max_nodes` (per zone) | config-mgmt `environments/orbit-<env>/variables.tf` |
| Room for large repositories | `tmpSizeLimit` and the ephemeral-storage limit of the code pool, together | `values.yaml` |
| Webserver pods | `webserver.replicas` | `values.yaml` |
| Container memory (OOM kills) | `resources` of `webserver`, `dispatcher` or `indexer.pools.<pool>` | `values.yaml` |
| Graph ClickHouse | Maximum replica memory | config-mgmt `environments/orbit-prd/clickhouse-cloud.tf` |

The shared SDLC limit: all SDLC pods use one NATS consumer, which holds at most
`max_ack_pending` jobs. Each pod holds its workers plus its prefetch buffer. Above
`max_ack_pending / (workers + buffer)` pods, more SDLC pods do not help. During a full
reindex, a higher limit can overload the graph ClickHouse.

More webserver pods do not make queries faster. The graph ClickHouse limits query speed.

Dedicated: Instrumentor sets all sizes per reference architecture. Only the code indexer
autoscales. A change needs an Instrumentor release.
