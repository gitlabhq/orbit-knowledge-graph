# Rebuild and reindex

Set the cluster once. All commands below use it:

```shell
export CTX=gke_gl-orbit-stg_us-east1_orbit-stg   # production: gke_gl-orbit-prd_us-east1_orbit-prd
```

The graph ClickHouse service is `gitlab-com-staging-us-east1` in staging and
`orbit-production-us-east1` in production. Before you run SQL, check the service name in the
ClickHouse console header. You need a user that can drop and create tables in database `gkg`:
ask an Orbit team member with Service Admin.

## When to rebuild

A full rebuild in production is for extraordinary cases only. It was
[rehearsed in staging](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1428)
on 2026-10-26.

- The graph has wrong data that indexing does not fix.
- [Disaster recovery](disaster-recovery/README.md), or a move to another ClickHouse service.

## Names that you need

- `<release>`: the running image tag, with dashes for dots (`1.2.3` is `1-2-3`). The stream
  list also shows old releases: use only the running one.

  ```shell
  kubectl --context "$CTX" -n gkg get deploy orbit-dispatcher -o jsonpath='{.spec.template.spec.containers[0].image}'
  kubectl --context "$CTX" -n nats exec deploy/nats-box -- nats stream ls -n | grep GKG_INDEXER_V
  ```

- `<N>`: the active schema version.

  ```sql
  SELECT version, status FROM gkg.gkg_schema_version FINAL ORDER BY version DESC
  ```

## Full rebuild

Steps 5 and 6: queries fail. After step 6: results are incomplete until indexing completes,
about 4 hours on GitLab.com and 7 minutes in staging.

1. Tell users in the incident channel and in `#f_orbit_dev`.
2. Pause both indexer pools:

   ```shell
   kubectl --context "$CTX" -n gkg annotate scaledobject orbit-indexer-sdlc autoscaling.keda.sh/paused-replicas="0" --overwrite
   kubectl --context "$CTX" -n gkg annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas="0" --overwrite
   ```

   Use this command to check the replicas in each step. Here: both indexers show `0/0`.

   ```shell
   kubectl --context "$CTX" -n gkg get deploy orbit-dispatcher orbit-indexer-sdlc orbit-indexer-code
   ```

3. Stop the dispatcher. First check if ArgoCD puts changes back by itself:

   ```shell
   GITLAB_HOST=gitlab.com glab api "projects/gitlab-com%2Fgl-infra%2Fargocd%2Fapps/repository/files/applications%2Fservices.yaml/raw?ref=main" | grep selfHeal
   ```

   - `selfHeal: false`: scale it down directly. A merge to the gkg app in argocd-apps starts
     it again, so merge no gkg change until step 6.

     ```shell
     kubectl --context "$CTX" -n gkg scale deploy orbit-dispatcher --replicas=0
     ```

   - `selfHeal: true`: set `dispatcher.enabled: false` in
     `services/gkg/env/orbit-<env>/values.yaml` in argocd-apps, and merge.

   Check: `orbit-dispatcher` shows `0/0`.

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

6. Start the dispatcher. If you scaled it down, scale it to 1. Otherwise set
   `dispatcher.enabled: true` and merge.

   ```shell
   kubectl --context "$CTX" -n gkg scale deploy orbit-dispatcher --replicas=1
   kubectl --context "$CTX" -n gkg logs deploy/orbit-dispatcher | grep "fresh install"
   ```

   Check: `orbit-dispatcher` shows `1/1` and the log shows `fresh install`.
7. Resume both pools. Check: both indexers show their minimum replicas again.

   ```shell
   kubectl --context "$CTX" -n gkg annotate scaledobject orbit-indexer-sdlc autoscaling.keda.sh/paused-replicas-
   kubectl --context "$CTX" -n gkg annotate scaledobject orbit-indexer-code autoscaling.keda.sh/paused-replicas-
   ```

8. Done when `GITLAB_HOST=gitlab.com glab orbit status --full-path <group>` reports the
   projects as indexed.

To move to another ClickHouse service, change the host and the passwords in the ArgoCD values
and Vault before step 6.

## Reindex one namespace, project or entity

Mark the checkpoint rows as deleted. Orbit then indexes that data again from the start.

SDLC data of one top-level namespace. The sweep key starts the reindex within a minute,
together with a sweep of all namespaces.

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

Check: `GITLAB_HOST=gitlab.com glab orbit status --full-path <group>` shows the
projects as indexed again.

> [!warning]
> Do not disable and enable Orbit on a namespace to reindex it. That does not reindex, and it
> schedules the namespace data for deletion.

## Dedicated

The Dedicated EOC runs these steps. Names are `gkg-*`. One ClickHouse service holds the
datalake and the graph: drop tables only in database `gkg`. Only the code indexer has a KEDA
ScaledObject: stop the SDLC indexer as in [emergency actions](README.md#emergency-actions).
