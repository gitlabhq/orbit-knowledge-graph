# SDLC indexing

Use this page when the SDLC data of one namespace is stale. For how SDLC indexing works, see
[SDLC indexing architecture](../architecture/indexing/sdlc-indexing.md).

## Names you need

Get `<release>` and `<N>` from [names that you need](manual_rebuild.md#names-that-you-need).
To open a nats-box shell and list the current streams, see [NATS](nats.md). The work stream
is `GKG_INDEXER_V<release>`. The checkpoint table is `gkg.v<N>_checkpoint`.

On Dedicated, the Dedicated EOC runs the commands without `--context`. Deployments are `gkg-*`.

## A namespace's data is stale

Use the top-level namespace ID. Run the SQL in the read-only ClickHouse console.

1. Is Orbit on for the namespace? Run this on the datalake service `gitlab-com-production-us-east1`.
   No row: Orbit is off, and Orbit does not index it.

   ```sql
   SELECT root_namespace_id, created_at
   FROM gitlab_clickhouse_main_production.siphon_knowledge_graph_enabled_namespaces FINAL
   WHERE root_namespace_id = <root_namespace_id> AND _siphon_deleted = false
   ```

1. Is a request queued? A subject means that a request waits or runs. `No subjects found`
   means none. A failed request is not retried at once. The dispatcher sends the namespace
   again at its next data change or at the hourly sweep.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n nats exec deploy/nats-box -- nats stream subjects GKG_INDEXER_V<release> 'v<release>.sdlc.namespace.indexing.requested.*.<root_namespace_id>' --names
   ```

1. Is the watermark old? Run this on the graph service `orbit-production-us-east1`. An
   `oldest` value older than 4 hours is stale, see [SDLC freshness](alerts/sdlc-freshness.md).

   ```sql
   SELECT min(watermark) AS oldest, max(watermark) AS newest
   FROM gkg.v<N>_checkpoint FINAL
   WHERE startsWith(key, 'ns.<root_namespace_id>.')
   ```

If the datalake data is old too, the problem is in Siphon. See [escalation](README.md#escalation).

## The indexer is stuck on a long query

The SDLC indexer reads as `gkg_siphon_reader` on the datalake service. Find its long queries:

```sql
SELECT hostName() AS replica, query_id, round(elapsed) AS seconds, left(query, 100) AS query
FROM clusterAllReplicas('default', system.processes)
WHERE user = 'gkg_siphon_reader' AND elapsed > 300
ORDER BY elapsed DESC
```

The query needs `READ ON REMOTE` and `SELECT ON system.processes`. The on-call console is
read-only: ask Analytics Platform Insights to run
`KILL QUERY ON CLUSTER default WHERE query_id = '<query_id>' SYNC`. Check: the query is not in the list. The dispatcher sends the namespace again.

## Reindex a namespace or an entity

See [reindex one namespace, project or entity](manual_rebuild.md#reindex-one-namespace-project-or-entity).
