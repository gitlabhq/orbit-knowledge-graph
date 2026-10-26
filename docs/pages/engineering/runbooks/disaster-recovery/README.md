# Disaster recovery

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

A lost replication slot needs a full Siphon snapshot. On GitLab.com, expect at least one
week of incomplete data. Continue when the Siphon team confirms that the snapshot is complete. Until then,
[stop both indexers](../README.md#emergency-actions), so Orbit does not index incomplete data.

## 2. Check NATS

If the NATS cluster was lost, ArgoCD recreates it. Orbit pods restart and create the Orbit
streams again. Siphon does not: the Siphon team restarts Siphon.
A Velero backup of NATS runs every 6 hours, but Siphon streams are in it too. Do not restore
it yourself: ask the Siphon team. Check: all NATS pods are `Running`, and the stream list
shows `GKG_INDEXER_V<release>`.

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n nats get pods
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n nats exec deploy/nats-box -- nats stream ls -n
```

## 3. Rebuild the graph

| Situation | Action |
| --- | --- |
| The graph ClickHouse was lost, or you move to a new ClickHouse service | [Full rebuild](../manual_rebuild.md#full-rebuild) |
| The graph has wrong data for some namespaces | [Reindex those namespaces](../manual_rebuild.md#reindex-one-namespace-project-or-entity) |

On Dedicated, the Dedicated EOC runs the rebuild.

A rebuild does not bring back `gkg.namespace_storage_snapshot`. To keep its history, a
ClickHouse Cloud admin restores the daily backup (kept for 24 hours) to a new service
([procedure](https://gitlab.com/gitlab-com/runbooks/-/blob/master/docs/clickhouse/backup-restore.md)).
Copy only this table back
([ClickHouse docs](https://clickhouse.com/docs/products/cloud/guides/backups/review-and-restore-backups)),
then delete the restored service:

```sql
INSERT INTO gkg.namespace_storage_snapshot
SELECT * FROM remoteSecure('<restored host>:9440', 'gkg', 'namespace_storage_snapshot', '<user>', '<password>');
```

Check: `SELECT count() FROM gkg.namespace_storage_snapshot FINAL` gives the same number on
both services. A second copy adds no duplicates.

## 4. Check that Orbit recovered

- The [health check](../README.md#first-5-minutes) passes.
- The [indexer dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer) shows
  freshness back to normal.
- `GITLAB_HOST=gitlab.com glab orbit graph-status --full-path <group>` reports the projects as
  indexed.
