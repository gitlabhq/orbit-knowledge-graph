# Code Indexing Runbook

Operations guide for the code indexing pipeline: dispatch sources, task processing, dead letter queue, and recovery.

## Architecture overview

Code indexing is event-driven, not periodic. Two dispatch sources feed code indexing task requests into the `GKG_INDEXER` stream:

```plaintext
Push events (CDC):
  Siphon -> p_knowledge_graph_code_indexing_tasks -> NATS siphon_stream_main_db
    -> SiphonCodeIndexingTaskDispatcher (DispatchIndexing mode)
      -> GKG_INDEXER: code.task.indexing.requested.<project>.<branch>

Namespace backfill:
  Siphon -> knowledge_graph_enabled_namespaces -> NATS siphon_stream_main_db
    -> NamespaceCodeBackfillDispatcher (DispatchIndexing mode)
      -> GKG_INDEXER: code.task.indexing.requested.<project>._
```

The `CodeIndexingTaskHandler` (Indexer mode) consumes from `code.task.indexing.requested.*.*`, fetches the repository, parses code, builds a property graph, and writes to ClickHouse.

### NATS subjects

| Subject | Source | Purpose |
|---------|--------|---------|
| `code.task.indexing.requested.<project_id>.<base64_branch>` | Push dispatcher | Index a specific branch after a push |
| `code.task.indexing.requested.<project_id>._` | Backfill dispatcher | Index all branches for a project (namespace enable) |

Branch names are base64-encoded in the subject. `_` means no specific branch (backfill).

### NATS consumers

Consumer names are derived from the configured `consumer_name` (default in production: `gkg-indexer`) and the subscription subject, with dots replaced by hyphens and wildcards spelled out.

| Consumer | Stream | Role |
|----------|--------|------|
| `gkg-indexer-code-task-indexing-requested-wildcard-wildcard` | `GKG_INDEXER` | Indexer handler (processes code indexing tasks) |
| `dispatch-code-task-indexing-requested-wildcard-wildcard` | `GKG_INDEXER` | Code task dispatcher |

With ephemeral consumers (`consumer_name: None`, the local dev default), NATS assigns random names that don't survive restarts.

To inspect a specific consumer:

```shell
nats consumer info GKG_INDEXER gkg-indexer-code-task-indexing-requested-wildcard-wildcard
```

### Handler configuration

| Setting | Default |
|---------|---------|
| `max_attempts` | 5 |
| `retry_interval_secs` | 60 |
| `dead_letter_on_exhaustion` | true |
| `concurrency_group` | `code` (4 workers by default) |

Unlike SDLC handlers, code indexing handlers retry via NATS because tasks are event-driven and won't be re-dispatched automatically.

## Dispatch sources

### Push event dispatcher (`SiphonCodeIndexingTaskDispatcher`)

Consumes CDC events from `siphon_stream_main_db` for the `p_knowledge_graph_code_indexing_tasks` table. Each event represents a git push.

The dispatcher:

1. Fetches a batch of pending messages (default batch size: 100)
2. Decodes Siphon replication events, extracting `project_id`, `ref`, `commit_sha`, `traversal_path`
3. Deduplicates by `(project_id, branch)`, keeping the highest `task_id`
4. Publishes `CodeIndexingTaskRequest` to `GKG_INDEXER`
5. Acknowledges the batch

### Namespace backfill dispatcher (`NamespaceCodeBackfillDispatcher`)

Consumes CDC events for `knowledge_graph_enabled_namespaces`. When a namespace is newly enabled:

1. Looks up the namespace's traversal path
2. Queries all projects under that namespace from the graph DB
3. Publishes a backfill request per project (`task_id: 0`, no branch, no commit)

## Processing pipeline

When the `CodeIndexingTaskHandler` receives a message:

1. Compare `task_id` against stored checkpoint. Skip if `task_id <= last_task_id`.
2. Acquire a NATS KV lock on `indexing/{project_id}/{base64_branch}/lock` (TTL: 60 seconds). Skip if lock is held by another worker.
3. Download the repository archive from Rails API (or use incremental fetch / cache).
4. Run tree-sitter + swc parsers across supported languages, build an in-memory property graph.
5. Convert graph to Arrow batches and insert into graph tables.
6. Record checkpoint: `(project_id, branch, last_task_id, last_commit, indexed_at)`.

Individual file parse failures are logged but do not fail the task. The pipeline writes whatever it parsed successfully.

## Dead letter queue

Failed code indexing tasks are sent to the `GKG_DEAD_LETTERS` stream after exhausting all 5 attempts.

### DLQ subject format

```plaintext
dlq.GKG_INDEXER.code.task.indexing.requested.<project_id>.<base64_branch>
```

### DLQ message envelope

Each dead letter contains:

| Field | Description |
|-------|-------------|
| `original_subject` | The original NATS subject |
| `original_stream` | `GKG_INDEXER` |
| `original_payload` | The `CodeIndexingTaskRequest` JSON |
| `original_message_id` | NATS message ID |
| `original_timestamp` | When the message was first published |
| `failed_at` | When the message entered the DLQ |
| `attempts` | Number of delivery attempts (5) |
| `last_error` | Error message from the final attempt |

### Inspect the DLQ

```shell
# List all dead-lettered code indexing messages
nats stream info GKG_DEAD_LETTERS
nats consumer create GKG_DEAD_LETTERS dlq-inspector \
  --filter='dlq.GKG_INDEXER.code.task.indexing.requested.>' \
  --pull --deliver=all --ack=none

nats consumer next GKG_DEAD_LETTERS dlq-inspector --count=10
```

### Replay a dead-lettered message

Extract the `original_payload` and republish to the original subject:

```shell
nats pub 'code.task.indexing.requested.<project_id>.<base64_branch>' \
  '<original_payload_json>'
```

The message will be processed as a new request (attempt counter resets to 1).

### Purge the DLQ

```shell
# Purge all code indexing dead letters
nats stream purge GKG_DEAD_LETTERS \
  --subject='dlq.GKG_INDEXER.code.task.indexing.requested.>'

# Purge dead letters for a specific project
nats stream purge GKG_DEAD_LETTERS \
  --subject='dlq.GKG_INDEXER.code.task.indexing.requested.<project_id>.*'
```

## Stuck tasks

### Symptoms

- Code indexing logs show no activity for a project that should be indexing
- NATS consumer has pending messages that are not being processed
- A project's code graph is stale (missing recent commits)

### Diagnose

1. Check if a message is stuck in the stream:

   ```shell
   nats stream info GKG_INDEXER --subjects | grep 'code.task'
   ```

2. Check consumer pending count:

   ```shell
   nats consumer info GKG_INDEXER gkg-indexer-code-task-indexing-requested-wildcard-wildcard
   ```

3. Check for lock contention in NATS KV:

   ```shell
   nats kv ls indexing_locks
   ```

4. Check indexer logs for the project:

   ```shell
   kubectl logs -n gkg deployment/gkg-indexer -f | grep 'project_id=<id>'
   ```

### Lock stuck (worker crashed mid-indexing)

The NATS KV lock has a 60-second TTL and expires automatically. If a worker crashes, the lock releases after at most 60 seconds and the message is redelivered.

If the lock is not expiring (clock skew or NATS bug):

```shell
nats kv del indexing_locks 'indexing/<project_id>/<base64_branch>/lock'
```

### Message stuck in redelivery loop

If a message keeps failing and hasn't reached `max_attempts` yet:

1. Check the error in logs (repository fetch failure, ClickHouse write error, parse crash)
2. Fix the root cause
3. The next redelivery (after `retry_interval_secs`) will succeed

If you want to skip it immediately:

```shell
nats stream purge GKG_INDEXER \
  --subject='code.task.indexing.requested.<project_id>.<base64_branch>'
```

### Checkpoint blocking reprocessing

If the checkpoint's `last_task_id` is higher than the incoming task, the handler skips the message.

Check the checkpoint:

```sql
SELECT project_id, branch, last_task_id, last_commit, indexed_at
FROM `gkg-sandbox`.code_indexing_checkpoint
WHERE project_id = <id>;
```

To force reprocessing, delete the checkpoint row:

```sql
ALTER TABLE `gkg-sandbox`.code_indexing_checkpoint
DELETE WHERE project_id = <id> AND branch = '<branch>';
```

## Force reindex

### Reindex a single project branch

1. Clear the checkpoint:

   ```sql
   ALTER TABLE `<gkg-database>`.code_indexing_checkpoint
   DELETE WHERE project_id = <id> AND branch = '<branch>';
   ```

2. Publish a new indexing request:

   ```shell
   # Encode branch name: echo -n 'main' | base64
   nats pub 'code.task.indexing.requested.<project_id>.<base64_branch>' \
     '{"task_id":0,"project_id":<id>,"branch":"<branch>","commit_sha":null,"traversal_path":"<path>"}'
   ```

### Reindex default branch for a project

```sql
ALTER TABLE `<gkg-database>`.code_indexing_checkpoint
DELETE WHERE project_id = <id>;
```

Then publish a backfill request (no branch):

```shell
nats pub 'code.task.indexing.requested.<project_id>._' \
  '{"task_id":0,"project_id":<id>,"branch":null,"commit_sha":null,"traversal_path":"<path>"}'
```

### Reindex an entire namespace

Trigger a namespace backfill by re-enabling the namespace in GitLab (toggle the Knowledge Graph feature flag off and on), or simulate the Siphon event that the `NamespaceCodeBackfillDispatcher` consumes.

## Retry mechanism

Code indexing retries differently from SDLC indexing because tasks are event-driven:

| Level | Mechanism | Details |
|-------|-----------|---------|
| NATS redelivery | `max_attempts: 5`, `retry_interval_secs: 60` | Handler nacks with 60-second delay on failure |
| Dead letter | `GKG_DEAD_LETTERS` stream | After 5 attempts, message moves to DLQ with full error context |
| Ack timeout | 300 seconds | If handler doesn't ack within 5 minutes, NATS redelivers |
| Progress heartbeat | `ack_progress()` | Long-running handlers reset the ack timer to prevent premature redelivery |

The retry flow:

```plaintext
Attempt 1 fails -> nack (60s delay)
  -> Attempt 2 fails -> nack (60s delay)
    -> Attempt 3 fails -> nack (60s delay)
      -> Attempt 4 fails -> nack (60s delay)
        -> Attempt 5 fails -> publish to GKG_DEAD_LETTERS -> ack original
```

If DLQ publication itself fails, the original message is nacked for redelivery rather than being dropped.

## Other failure modes

### Stale data accumulation

The cleanup stage runs after indexing but failures are logged as warnings and do not block the pipeline. Over time, stale rows from deleted files may accumulate.

To clean up manually:

```sql
OPTIMIZE TABLE `<gkg-database>`.gl_file FINAL CLEANUP;
OPTIMIZE TABLE `<gkg-database>`.gl_directory FINAL CLEANUP;
OPTIMIZE TABLE `<gkg-database>`.gl_imported_symbol FINAL CLEANUP;
OPTIMIZE TABLE `<gkg-database>`.gl_definition FINAL CLEANUP;
```

## Monitoring

### Key metrics

| Metric | What it tells you |
|--------|-------------------|
| Handler outcome: `indexed` | Successful indexing completions |
| Handler outcome: `skipped_checkpoint` | Messages skipped (already processed) |
| Handler outcome: `skipped_lock` | Messages skipped (another worker holds the lock) |
| Handler outcome: `error` | Failed processing attempts |
| Error stage: `repository_fetch` | Repository download failures |
| Error stage: `checkpoint` | Checkpoint read/write failures |
| Error stage: `indexing` | Parse or graph build failures |
| Error stage: `arrow_conversion` | Schema conversion failures |
| Error stage: `write` | ClickHouse write failures |

### Check NATS stream health

```shell
nats stream info GKG_INDEXER
nats stream info GKG_DEAD_LETTERS
nats consumer ls GKG_INDEXER
```
