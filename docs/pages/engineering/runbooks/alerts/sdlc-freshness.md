# Orbit SDLC freshness alerts

Covers `OrbitSdlcFreshnessSLOViolation`, `OrbitServiceSdlcFreshnessTrafficCessation`, `OrbitServiceSdlcFreshnessTrafficAbsent`, `OrbitSdlcDataOlderThan4h`.

Orbit SDLC data is behind the datalake, or the freshness samples stopped. Users see old issues, merge requests and pipelines in Orbit results.

## Check

1. In [Grafana Explore](https://dashboards.gitlab.net/explore), data source `mimir-analytics-eventsdot`, check for a migration. Healthy: `0`.

   ```promql
   max(gkg_schema_migrating_age_seconds{env="orbit-prd"})
   ```

1. In the same place, check the freshness task. Healthy: `success` is above 0.

   ```promql
   sum by (outcome) (increase(gkg_scheduler_task_runs_total{env="orbit-prd", task="namespace_freshness"}[30m]))
   ```

1. Open the [indexer dashboard](https://dashboards.gitlab.net/d/orbit-orbit-gkg-indexer), "Oldest data" tile. Healthy: below 15 minutes. Note which entities are old.
1. In the ClickHouse console, service `orbit-production`, get the active version `<N>`. Then list the old namespaces. Healthy: no rows.

   ```sql
   SELECT version FROM gkg.gkg_schema_version FINAL WHERE status = 'active';

   SELECT splitByChar('.', key)[2] AS root_namespace_id, count() AS entities, min(watermark) AS synced_until
   FROM gkg.v<N>_checkpoint FINAL
   WHERE NOT _deleted AND key LIKE 'ns.%' AND watermark < now() - INTERVAL 4 HOUR
   GROUP BY root_namespace_id
   ORDER BY synced_until;
   ```

## Act

| You see | Do this | Done when |
| --- | --- | --- |
| Migration age above 0 and below 4 hours | No action. A full-rebuild migration freezes the data until it ends. | Migration age is `0`. |
| Migration age above 4 hours | The migration is stuck. Page Orbit tier-2. | Migration age is `0`. |
| Freshness task has no `success` runs | Follow [dispatcher alerts](gkg-dispatcher.md). | `success` is above 0. |
| All entities are old, no migration | Indexing stopped. Follow [SDLC indexer](gkg-indexer-sdlc.md) and [NATS](nats-slo.md) alerts. | "Oldest data" is below 15 minutes. |
| One entity is old in many namespaces | One handler fails. Find its error in the [logs](https://log.gprd.gitlab.net/app/r/s/DhOL5), filter `json.mode: indexer`. Page Orbit tier-2. | That entity is below 15 minutes. |
| A few namespaces in the SQL result | Get the path with the query below. Check `GITLAB_HOST=gitlab.com glab orbit graph-status --full-path <path>`. If it is enabled, [reindex it](../manual_rebuild.md#reindex-one-namespace-project-or-entity). | The namespace is not in the SQL result. |
| Started with a deploy | [Roll back](../maintenance.md#rollback). | "Oldest data" is below 15 minutes. |

Get the namespace path:

```shell
GITLAB_HOST=gitlab.com glab orbit query 'MATCH (g:Group) WHERE g.id = <root_namespace_id> RETURN g.id, g.full_path LIMIT 1'
```

## Escalate

No cause after 15 minutes: page Orbit tier-2 in incident.io ([escalation](../README.md#escalation)).
Dedicated: the EOC runs `/inc escalate` and selects Orbit.
