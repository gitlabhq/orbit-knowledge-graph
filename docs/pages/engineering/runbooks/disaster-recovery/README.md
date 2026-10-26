# Disaster recovery

Back to the [runbook](../README.md).

Orbit holds no primary data. The graph is built from GitLab data that Siphon copies into
the datalake, and from the repositories. To recover Orbit, recover Siphon first, then
rebuild the graph ([decision](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/183)).

## 1. Recover Siphon and the datalake (prerequisite)

The Siphon team owns Siphon and the datalake ClickHouse. Contact them first: Slack
`#g_analytics_platform_insights`.

| | GitLab.com | Dedicated |
| --- | --- | --- |
| Siphon recovery | [Siphon runbook](https://runbooks.gitlab.com/siphon/), section "Failure Modes" | [Siphon operations](https://gitlab.com/gitlab-com/gl-infra/gitlab-dedicated/instrumentor/-/blob/main/docs/data_insights_siphon_operations.md) |
| How a fresh snapshot works | [Replication and snapshot flow](https://gitlab.com/gitlab-org/analytics-section/siphon/-/blob/main/docs/replication_and_snapshot_flow.md) | Same |

A lost replication slot means a full re-snapshot. The Siphon runbook expects at least a week
of degraded data on GitLab.com.

Continue when the Siphon team confirms that the snapshot is complete. Until then, Orbit
indexes incomplete data.

## 2. Check NATS

If the NATS cluster was lost, ArgoCD recreates it. Orbit creates its streams again at
startup and sends the missing work again. Orbit does not need the NATS backup.

## 3. Rebuild the graph

| Situation | Action |
| --- | --- |
| The graph ClickHouse was lost, or you move to a new ClickHouse service | [Full rebuild](../manual_rebuild.md#full-rebuild) |
| The graph has wrong data for some namespaces | [Reindex those namespaces](../manual_rebuild.md#reindex-one-namespace-project-or-entity) |

On Dedicated, the Dedicated EOC runs the rebuild.

## 4. Check that Orbit recovered

- `glab orbit status` is `healthy` and a test query works (see
  [troubleshooting](../troubleshooting.md#order-of-checks)).
- The [indexer dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer) shows
  freshness back to normal.
- `glab orbit graph-status --full-path <group>` reports the projects as indexed.
