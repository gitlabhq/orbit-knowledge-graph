# OrbitSdlcDataOlderThan4h

## Overview

The oldest SDLC data that Orbit serves was more than 4 hours behind the datalake for 15
minutes. Users read SDLC data (issues, merge requests, pipelines) that is more than 4 hours
old for at least one namespace.

This is a hard limit with no error budget. No paging, posts to `#f_orbit_alerts`.

## Services

- [Orbit service overview](../README.md)
- Owner team: `context_systems`

## Metrics

Every 5 minutes the dispatcher sets `gkg_indexer_sdlc_freshness_oldest_sync_timestamp_seconds`
for each SDLC entity. The value is the time up to which the data matches the datalake, in
the least synced enabled namespace. Global entities, such as users and runners, use their own
sync time. The alert compares the age of the oldest value with 4 hours. See
[sdlc-freshness](sdlc-freshness.md) for when a namespace counts as in sync.

If the freshness task stops, the value stops and its age keeps growing, so this alert fires
after 4 hours. `OrbitDispatchTaskStopped` fires first in that case.

A schema migration under 4 hours cannot fire this alert. A migration that runs longer
fires it for every entity, which is also how a stuck migration shows up.

## Verification

```promql
# Age of the oldest data, by entity
time() - min by (entity) (gkg_indexer_sdlc_freshness_oldest_sync_timestamp_seconds{env="orbit-prd"})

# Migration age in seconds, 0 when idle
max(gkg_schema_migrating_age_seconds{env="orbit-prd"})
```

To find the namespaces, query the `gkg` database of the `orbit-production` ClickHouse
service. Take `<version>` from the `active` row of `gkg_schema_version`.

```sql
SELECT splitByChar('.', key)[2] AS root_namespace_id, count() AS entities, min(watermark) AS synced_until
FROM v<version>_checkpoint FINAL
WHERE NOT _deleted AND key LIKE 'ns.%' AND watermark < now() - INTERVAL 4 HOUR
GROUP BY root_namespace_id
ORDER BY synced_until
```

A disabled namespace keeps its old checkpoints until its scheduled deletion, so check
that the namespace is still enabled. A namespace that never finished its first sync has no
completed checkpoint and is not in this list.

- [Indexer dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer),
  "Oldest data" tile and the "SDLC: oldest data per plan" chart.

## Troubleshooting

| Symptom | Cause | Action |
| --- | --- | --- |
| A few entities in many namespaces, or only a global entity | These plans fail or stop everywhere | Check the SDLC errors for the entity in the indexer logs |
| Every entity, one or few namespaces in the query | The namespace cannot finish a sync, or never finished its first sync | Find the namespace in the indexer logs, check `glab orbit graph-status --full-path <path>` for it |
| Every entity, migration age above 4 hours | Slow or stuck full-rebuild migration | Check the migration completion log line and [knowledge-graph#1004](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1004) |
| Every entity, no migration | Indexing or the freshness task stopped for more than 4 hours | Check `OrbitDispatchTaskStopped`, the indexer and dispatcher pods, and NATS |

## Escalation

- Ask in `#f_orbit_dev` (team channel for `context_systems`).

## Definitions

- [Alert rule](https://gitlab.com/gitlab-com/runbooks/-/blob/master/libsonnet/alerts/orbit-alerts.libsonnet), then run `make generate`.

## Related Links

- [Related alerts](../monitoring.md#alerts)
- [Orbit runbook](../README.md)
