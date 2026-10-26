# Rebuild and reindex

Back to the [runbook](README.md).

> [!warning]
> Tested on a throwaway cluster, not yet run in production. Do staging first.

Set the cluster once. All commands below use it:

```shell
export CTX=gke_gl-orbit-stg_us-east1_orbit-stg   # production: gke_gl-orbit-prd_us-east1_orbit-prd
```

The graph ClickHouse service is `gitlab-com-staging-us-east1` in staging and
`orbit-production-us-east1` in production. Before you run SQL, check the service name in the
ClickHouse console header. You need a user that can drop and create tables in database `gkg`:
ask an Orbit team member with Service Admin.

## When to rebuild

- The graph has wrong data that indexing does not fix.
- [Disaster recovery](disaster-recovery/README.md), or a move to another ClickHouse service.

## Names that you need

- `<release>`: the current indexing stream name ends with it.

  ```shell
  kubectl --context "$CTX" -n nats exec deploy/nats-box -- nats stream ls | grep GKG_INDEXER_V
  ```

- `<N>`: the active schema version.

  ```sql
  SELECT version, status FROM gkg.gkg_schema_version FINAL ORDER BY version DESC
  ```

Keep `gkg.namespace_storage_snapshot`. A reindex cannot rebuild its history.

## Full rebuild

Steps 4 and 5: queries fail. After step 5: results are incomplete until indexing completes,
about 4 hours on GitLab.com.

1. Tell users in the incident channel and in `#f_orbit_dev`.
2. Pause both indexer pools. Check: both indexers show `0/0`.

   ```shell
   kubectl --context "$CTX" -n gkg annotate scaledobject orbit-indexer-sdlc autoscaling.keda.sh/paused-replicas="0" --overwrite
   kubectl --context "$CTX" -n gkg annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas="0" --overwrite
   kubectl --context "$CTX" -n gkg get deploy
   ```

3. Stop the dispatcher: set `dispatcher.enabled: false` in
   `services/gkg/env/orbit-<env>/values.yaml` in argocd-apps, and merge. Check: no
   `orbit-dispatcher` pod in `kubectl --context "$CTX" -n gkg get pods`.

4. Delete the old indexing jobs. Old jobs block new jobs for the same namespace.

   > [!warning]
   > Purge only the `GKG_INDEXER_V<release>` stream. Siphon streams are on the same NATS.

   ```shell
   kubectl --context "$CTX" -n nats exec deploy/nats-box -- nats stream purge GKG_INDEXER_V<release> -f
   kubectl --context "$CTX" -n nats exec deploy/nats-box -- nats stream info GKG_INDEXER_V<release>
   ```

   Check: `stream info` shows 0 messages.

5. Drop the graph.

   > [!warning]
   > This deletes the graph. Get approval from the incident lead first.

   This query prints the `DROP` statements in the correct order. Run them, then drop the
   version table:

   ```sql
   SELECT concat(multiIf(engine IN ('View', 'MaterializedView'), 'DROP VIEW',
                         engine = 'Dictionary', 'DROP DICTIONARY', 'DROP TABLE'),
                 ' gkg.', name,
                 if(engine IN ('View', 'MaterializedView', 'Dictionary'), '',
                    ' SETTINGS max_table_size_to_drop = 0'),
                 ';')
   FROM system.tables
   WHERE database = 'gkg' AND match(name, '^v[0-9]+_')
   ORDER BY multiIf(engine IN ('View', 'MaterializedView'), 1, engine = 'Dictionary', 2, 3), name;

   DROP TABLE gkg.gkg_schema_version;
   ```

   If you keep `gkg_schema_version`, the dispatcher creates no tables and crash-loops.

6. Start the dispatcher: set `dispatcher.enabled: true` and merge. Check: its logs show
   `fresh install`.
7. Resume both pools:

   ```shell
   kubectl --context "$CTX" -n gkg annotate scaledobject orbit-indexer-sdlc autoscaling.keda.sh/paused-replicas-
   kubectl --context "$CTX" -n gkg annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas-
   ```

8. Done when `GITLAB_HOST=gitlab.com glab orbit graph-status --full-path <group>` reports the
   projects as indexed.

To move to another ClickHouse service, change the host and the passwords in the ArgoCD values
and Vault before step 6. The datalake does not change.

## Reindex one namespace, project or entity

Mark the checkpoint rows as deleted. Orbit then indexes that data again from the start.

SDLC data of one top-level namespace. The sweep key starts the reindex within a minute.
Without it, the reindex waits for the hourly sweep.

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

Code of one project, all branches. The backfill sends the default branch again within a few
minutes.

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

Check: `GITLAB_HOST=gitlab.com glab orbit graph-status --full-path <group>` shows the
projects as indexed again.

> [!warning]
> Do not disable and enable Orbit on a namespace to reindex it. That does not reindex, and it
> schedules the namespace data for deletion.

## Dedicated

The Dedicated EOC runs these steps. Names are `gkg-*`. Only the code indexer has a KEDA
ScaledObject: stop the SDLC indexer as in [emergency actions](README.md#emergency-actions).
