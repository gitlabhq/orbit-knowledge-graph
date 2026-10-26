# Code indexing

Use this page when the code data of one project is stale or its task fails. For how code
indexing works, see [code indexing architecture](../architecture/indexing/code-indexing.md).
To stop code indexing, use [emergency actions](README.md#emergency-actions).

## Names you need

Get `<release>` and `<N>` from [names that you need](manual_rebuild.md#names-that-you-need).
To open a nats-box shell and list the current streams, see [NATS](nats.md).

| Name | Value |
| --- | --- |
| Work stream | `GKG_INDEXER_V<release>` |
| Dead letter stream | `GKG_DEAD_LETTERS_V<release>` |
| Task subject | `v<release>.code.task.indexing.requested.<project_id>.<branch_b64>` |
| Lock bucket | `indexing_locks_v<N>` |
| Checkpoint table | `gkg.v<N>_code_indexing_checkpoint` |

`<branch_b64>` is the branch name in URL-safe base64 without padding. A backfill task uses `_`.
Get it with `printf main | base64 | tr '+/' '-_' | tr -d '='`.

On Dedicated, the Dedicated EOC runs the commands without `--context`. Deployments are `gkg-*`.

## A project's code is stale

### Check

1. Check the project. Healthy: `code_indexing.state` is `indexed`. This shows one full index,
   not the newest commit. Exit code 2: no such project, no access, or Orbit is off.

   ```shell
   GITLAB_HOST=gitlab.com glab orbit graph-status --project-id <project_id>
   ```

1. Check the checkpoint in the read-only console of the graph ClickHouse service. The backfill
   indexes only the default branch. Compare `last_commit` with the last commit on that branch. An empty `last_commit` means
   that a backfill or reindex indexed the branch at `indexed_at`.

   ```sql
   SELECT branch, last_commit, indexed_at
   FROM gkg.v<N>_code_indexing_checkpoint FINAL
   WHERE project_id = <project_id>
   ```

1. Check for a queued task. A subject means that a task waits or runs. `No subjects found` means none.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n nats exec deploy/nats-box -- nats stream subjects GKG_INDEXER_V<release> 'v<release>.code.task.indexing.requested.<project_id>.*' --names
   ```

1. Check the lock. `key not found` means that no worker indexes the project. A value is the
   time when the lease ends. The worker renews it while it works.

   ```shell
   kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n nats exec deploy/nats-box -- nats kv get indexing_locks_v<N> 'project.<project_id>.<branch_b64>'
   ```

1. Read the indexer logs in [Kibana](https://log.gprd.gitlab.net/app/r/s/DhOL5). Filter
   `json.mode: indexer` and `json.span.gl_project_id: <project_id>`.

Do not delete a lock. If a lock stays for more than 15 minutes, page Orbit tier-2.

No code tasks for any project? The dispatcher ConfigMap must have
`events_stream_name: prd_siphon_event_stream` (staging: `stg_siphon_event_stream`):

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n gkg get configmap orbit-dispatcher-config -o jsonpath='{.data.*}' | grep events_stream_name
```

### Fix

If a task waits or runs, wait for it. If the task fails, see
[a task fails again and again](#a-task-fails-again-and-again). Else, reindex the project with
[reindex one namespace, project or entity](manual_rebuild.md#reindex-one-namespace-project-or-entity).
Check: `indexed_at` in the checkpoint query shows a new time.

## A task fails again and again

### Read the dead letters

These commands only read. List the dead letters of the project, then read one. The
`last_error` field shows the cause.

```shell
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n nats exec deploy/nats-box -- nats stream subjects GKG_DEAD_LETTERS_V<release> 'v<release>.dlq.GKG_INDEXER.v<release>.code.task.indexing.requested.<project_id>.*' --names
kubectl --context gke_gl-orbit-prd_us-east1_orbit-prd -n nats exec deploy/nats-box -- nats stream get GKG_DEAD_LETTERS_V<release> --last-for '<subject from the last command>'
```

Older releases keep their own `GKG_DEAD_LETTERS_V<release>` stream. Do not send dead letters
again. After the cause is fixed, [reindex the project](#fix).

## Gitaly or ClickHouse overload

See [Gitaly overload](troubleshooting.md#gitaly-overload).
