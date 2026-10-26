# Disaster recovery

Back to the [runbook](../README.md).

Orbit holds no primary data. Siphon copies GitLab data into the datalake, and Orbit builds
the graph from the datalake and the repositories. Recover Siphon first, then rebuild the
graph.

## 1. Recover Siphon and the datalake

The Siphon team owns Siphon and the datalake ClickHouse. Contact them first: Slack
`#g_analytics_platform_insights`.

| | GitLab.com | Dedicated |
| --- | --- | --- |
| Siphon recovery | [Siphon runbook](https://runbooks.gitlab.com/siphon/), section "Failure Modes" | [Siphon operations](https://gitlab.com/gitlab-com/gl-infra/gitlab-dedicated/instrumentor/-/blob/main/docs/data_insights_siphon_operations.md) |
| How a fresh snapshot works | [Replication and snapshot flow](https://gitlab.com/gitlab-org/analytics-section/siphon/-/blob/main/docs/replication_and_snapshot_flow.md) | Same |

Continue when the Siphon team confirms that the snapshot is complete. Until then,
[stop both indexers](../README.md#emergency-actions), so Orbit does not index incomplete data.

## 2. Check NATS

If the NATS cluster was lost, ArgoCD recreates it. Orbit creates its streams again at startup.
Check: all NATS pods are `Running`.

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n nats get pods
```

## 3. Rebuild the graph

| Situation | Action |
| --- | --- |
| The graph ClickHouse was lost, or you move to a new ClickHouse service | [Full rebuild](../manual_rebuild.md#full-rebuild) |
| The graph has wrong data for some namespaces | [Reindex those namespaces](../manual_rebuild.md#reindex-one-namespace-project-or-entity) |

On Dedicated, the Dedicated EOC runs the rebuild.

## 4. Check that Orbit recovered

- The [health check](../README.md#first-5-minutes) passes.
- The [indexer dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer) shows
  freshness back to normal.
- `GITLAB_HOST=gitlab.com glab orbit graph-status --full-path <group>` reports the projects as
  indexed.
