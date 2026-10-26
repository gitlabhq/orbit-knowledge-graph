# Rebuild and reindex

Back to the [runbook](README.md). Tested on a throwaway cluster, not yet run in production.
Do staging first.

| | Staging (commands below) | Production |
| --- | --- | --- |
| kubectl context | `gke_gl-orbit-stg_us-east1_orbit-stg` | `gke_gl-orbit-prd_us-east1_orbit-prd` |
| Graph ClickHouse service | `gitlab-com-staging-us-east1` | `orbit-production-us-east1` |

Before you run SQL, check the service name in the ClickHouse console header.

## When to rebuild

- The graph has wrong data that indexing does not fix, for example after a database incident.
- Disaster recovery (start with [disaster recovery](disaster-recovery/README.md)), or a move to
  another ClickHouse service.

The graph is derived data. GitLab PostgreSQL and the repositories are the source of truth.
Orbit does not restore from ClickHouse Cloud backups.

You need a ClickHouse user that can drop and create tables in database `gkg`. Ask an Orbit
team member with Service Admin on the graph service.

## Names that you need

- `<release>`: the image tag with dashes, for example `0-159-0`.

  ```shell
  kubectl --context gke_gl-orbit-stg_us-east1_orbit-stg -n gkg get deploy orbit-dispatcher \
    -o jsonpath='{.spec.template.spec.containers[0].image}'
  ```

- `<N>`: the active schema version.

  ```sql
  SELECT version, status FROM gkg.gkg_schema_version FINAL ORDER BY version DESC
  ```

Keep `gkg.namespace_storage_snapshot`. A reindex cannot rebuild its history.

## Full rebuild

Queries fail from step 4 to step 5. After step 5, queries return empty or incomplete results
until the indexers fill the tables. On GitLab.com this takes about 4 hours.

1. Tell users in the incident channel and in `#f_orbit_dev`.
2. Pause both indexer pools:

   ```shell
   kubectl --context gke_gl-orbit-stg_us-east1_orbit-stg -n gkg annotate scaledobject orbit-indexer-sdlc autoscaling.keda.sh/paused-replicas="0" --overwrite
   kubectl --context gke_gl-orbit-stg_us-east1_orbit-stg -n gkg annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas="0" --overwrite
   ```

   Check: `kubectl --context gke_gl-orbit-stg_us-east1_orbit-stg -n gkg get deploy` shows `0/0`
   for both indexers.

3. Stop the dispatcher: set `dispatcher.enabled: false` in the ArgoCD values and merge.
   Check: `kubectl --context gke_gl-orbit-stg_us-east1_orbit-stg -n gkg get pods` shows no
   `orbit-dispatcher` pod.

   Then delete the old indexing jobs of the current release. Old jobs block new jobs for the
   same namespace. Purge only `GKG_INDEXER_V<release>`: the same NATS holds Siphon streams.

   ```shell
   kubectl --context gke_gl-orbit-stg_us-east1_orbit-stg -n nats exec deploy/nats-box -- nats stream purge GKG_INDEXER_V<release> -f
   kubectl --context gke_gl-orbit-stg_us-east1_orbit-stg -n nats exec deploy/nats-box -- nats stream info GKG_INDEXER_V<release>
   ```

   Check: `stream info` shows 0 messages.

   > [!warning]
   > The next step deletes the graph. Get approval from the incident lead first.

4. Drop the graph. This query prints the `DROP` statements in the correct order (views,
   dictionaries, tables). Run them, then drop the version table:

   ```sql
   SELECT concat(multiIf(engine IN ('View', 'MaterializedView'), 'DROP VIEW',
                         engine = 'Dictionary', 'DROP DICTIONARY', 'DROP TABLE'),
                 ' gkg.', name, ';')
   FROM system.tables
   WHERE database = 'gkg' AND match(name, '^v[0-9]+_')
   ORDER BY multiIf(engine IN ('View', 'MaterializedView'), 1, engine = 'Dictionary', 2, 3), name;

   DROP TABLE gkg.gkg_schema_version;
   ```

   If you keep `gkg_schema_version`, the dispatcher creates no tables and crash-loops. For a
   table larger than 50 GB, add `SETTINGS max_table_size_to_drop = 0` to its `DROP TABLE`.

5. Start the dispatcher: set `dispatcher.enabled: true` and merge. Check: its logs show
   `fresh install`.
6. Resume both pools:

   ```shell
   kubectl --context gke_gl-orbit-stg_us-east1_orbit-stg -n gkg annotate scaledobject orbit-indexer-sdlc autoscaling.keda.sh/paused-replicas-
   kubectl --context gke_gl-orbit-stg_us-east1_orbit-stg -n gkg annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas-
   ```

7. Done when the indexer dashboard shows indexing of all namespaces and
   `glab orbit graph-status --full-path <group>` reports the projects as indexed.

To move to another ClickHouse service, change the host and the passwords in the ArgoCD
values and Vault before step 5. The datalake does not change.

## Reindex one namespace, project or entity

Mark the checkpoint rows as deleted. Orbit then indexes that data again from the start.
Replace `<N>` with the schema version.

SDLC data of one top-level namespace. The last line starts the full sweep now; without it,
the namespace reindexes at the next hourly sweep:

```sql
INSERT INTO gkg.v<N>_checkpoint (key, watermark, cursor_values, _deleted)
SELECT key, argMax(watermark, _version), argMax(cursor_values, _version), true
FROM gkg.v<N>_checkpoint
WHERE startsWith(key, 'ns.<root_namespace_id>.')
   OR key = 'dispatch.sdlc.namespace.sweep'
GROUP BY key HAVING argMax(_deleted, _version) = false;
```

Other selections for the same query:

| Reindex | `WHERE` |
| --- | --- |
| One entity type in a namespace | `key = 'ns.<id>.<Entity>' OR startsWith(key, 'ns.<id>.<Entity>.p')` |
| One global entity, for example User | `key = 'global.<Entity>'` |

Code of one project (all branches; the backfill sends the default branch again within a
few minutes):

```sql
INSERT INTO gkg.v<N>_code_indexing_checkpoint
  (traversal_path, project_id, branch, last_task_id, last_commit, indexed_at, _deleted)
SELECT traversal_path, project_id, branch, argMax(last_task_id, _version),
       argMax(last_commit, _version), argMax(indexed_at, _version), true
FROM gkg.v<N>_code_indexing_checkpoint
WHERE project_id = <id>
GROUP BY traversal_path, project_id, branch HAVING argMax(_deleted, _version) = false;
```

For all projects in a namespace, use
`WHERE startsWith(traversal_path, '<org_id>/<root_namespace_id>/')`.

Do not disable and enable Orbit on a namespace to reindex it. That does not reindex, and
it schedules the namespace data for deletion.

## Dedicated

The Dedicated EOC runs these steps. Names are `gkg-*`, and only the code indexer has a KEDA
ScaledObject.

> [!warning]
> Setting `orbit.infra_enabled` to `false` deletes the whole Orbit graph. It is not an
> incident action.
