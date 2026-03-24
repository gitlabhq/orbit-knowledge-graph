# SDLC Indexing Runbook

Operations guide for the SDLC indexing pipeline: dispatching, entity extraction, checkpoint management, and recovery.

## Architecture overview

Siphon CDC replicates GitLab PostgreSQL tables into ClickHouse datalake tables.
The SDLC indexing pipeline transforms datalake rows into graph node and edge tables.

```text
GitLab PostgreSQL
  -> Siphon CDC (logical replication)
    -> NATS JetStream
      -> ClickHouse datalake tables
        -> [Dispatcher] publishes indexing requests to NATS
          -> [Indexer] extracts, transforms, writes graph tables
```

### NATS stream: `GKG_INDEXER`

| Setting | Value |
|---------|-------|
| Retention | WorkQueue (deleted on ack) |
| Max messages per subject | 1 |
| Discard policy | New (reject duplicates while in-flight) |

### Subjects

| Subject | Purpose |
|---------|---------|
| `sdlc.global.indexing.requested` | Trigger global entity indexing (User) |
| `sdlc.namespace.indexing.requested.<org>.<ns>` | Trigger namespace entity indexing |

### NATS consumers

Consumer names are derived from the configured `consumer_name` (default in production: `gkg-indexer`) and the subscription subject, with dots replaced by hyphens and wildcards spelled out.

| Consumer | Stream | Role |
|----------|--------|------|
| `gkg-indexer-sdlc-global-indexing-requested` | `GKG_INDEXER` | Global handler |
| `gkg-indexer-sdlc-namespace-indexing-requested-wildcard-wildcard` | `GKG_INDEXER` | Namespace handler |
| `gkg-indexer-sdlc-namespace-deletion-requested-wildcard` | `GKG_INDEXER` | Namespace deletion handler |

With ephemeral consumers (`consumer_name: None`, the local dev default), NATS assigns random names that don't survive restarts.

### Handlers

| Handler | Subject | Default max_attempts | DLQ |
|---------|---------|---------------------|-----|
| GlobalHandler | `sdlc.global.indexing.requested` | 1 | No (re-dispatched next cycle) |
| NamespaceHandler | `sdlc.namespace.indexing.requested.*.*` | 1 | No (re-dispatched next cycle) |

Both handlers rely on the dispatcher to re-create requests on the next cycle rather than retrying via NATS. This is the eventual consistency model.

## Dispatcher

The dispatcher runs as `gkg-server --mode DispatchIndexing` and publishes indexing requests on a schedule.

### Scheduled tasks

| Task | What it does |
|------|-------------|
| GlobalDispatcher | Publishes a single `GlobalIndexingRequest` with watermark = now |
| NamespaceDispatcher | Queries `siphon_knowledge_graph_enabled_namespaces`, publishes one request per enabled namespace |

Both run on every scheduler cycle. The NATS `max_messages_per_subject: 1` constraint ensures at-most-one in-flight request per subject. Duplicate publishes are silently rejected.

## Checkpoint system

The indexer tracks progress in a `checkpoint` table in the graph database.

### Schema

| Column | Type | Purpose |
|--------|------|---------|
| `key` | String | Position identifier (e.g., `global.User`, `ns.42.Project`) |
| `watermark` | DateTime64 | Upper bound of the extraction window |
| `cursor_values` | String (JSON) | Keyset pagination cursor for resuming mid-batch |
| `_version` | DateTime64 | ReplacingMergeTree version |
| `_deleted` | Bool | Soft delete flag |

### Checkpoint states

| State | Meaning |
|-------|---------|
| No row | First run. Extracts from epoch. |
| `cursor_values` is null/empty | Previous run completed. Next run starts a fresh window. |
| `cursor_values` has values | Interrupted mid-pagination. Resumes from the cursor. |

### Inspect checkpoints

```sql
SELECT key, watermark, cursor_values
FROM `<gkg-database>`.checkpoint
ORDER BY key;
```

### Inspect checkpoints for a specific namespace

```sql
SELECT key, watermark, cursor_values
FROM `<gkg-database>`.checkpoint
WHERE key LIKE 'ns.42.%'
ORDER BY key;
```

## Force reindex

### Reindex a single namespace

1. Delete the namespace's checkpoints so the indexer starts from epoch:

   ```sql
   ALTER TABLE `<gkg-database>`.checkpoint
   DELETE WHERE key LIKE 'ns.<namespace_id>.%';
   ```

2. Trigger the dispatcher (it will publish a new request on the next cycle):

   ```shell
   # Option A: wait for the next scheduled dispatch cycle
   # Option B: trigger manually (see "Trigger dispatcher manually" above)
   # Option C: publish directly to NATS
   nats pub sdlc.namespace.indexing.requested.<org_id>.<namespace_id> \
     '{"organization":<org_id>,"namespace":<namespace_id>,"watermark":"2026-03-24T00:00:00Z"}'
   ```

### Reindex a single entity type within a namespace

```sql
ALTER TABLE `<gkg-database>`.checkpoint
DELETE WHERE key = 'ns.<namespace_id>.<EntityType>';
```

For example, to reindex only Projects in namespace 42:

```sql
ALTER TABLE `<gkg-database>`.checkpoint
DELETE WHERE key = 'ns.42.Project';
```

### Reindex global entities (User)

```sql
ALTER TABLE `<gkg-database>`.checkpoint
DELETE WHERE key LIKE 'global.%';
```

### Full reindex (all namespaces, all entities)

1. Truncate graph tables:

   ```sql
   SELECT 'TRUNCATE TABLE `<gkg-database>`.' || name || ';'
   FROM system.tables
   WHERE database = '<gkg-database>';
   ```

   Run the generated statements.

2. Clear all checkpoints:

   ```sql
   TRUNCATE TABLE `<gkg-database>`.checkpoint;
   ```

3. Trigger the dispatcher to re-publish all requests.

### Replay from a specific point in time

Manually set a checkpoint watermark to re-extract rows after that timestamp:

```sql
INSERT INTO `<gkg-database>`.checkpoint (key, watermark, cursor_values, _version, _deleted)
VALUES ('ns.42.Project', '2026-01-01T00:00:00', '', now64(), false);
```

The handler will extract rows where `_siphon_replicated_at` falls between the old watermark and the new one.

## Remove stuck messages from NATS

### Check pending messages

```shell
nats stream info GKG_INDEXER
nats consumer ls GKG_INDEXER
nats consumer info GKG_INDEXER <consumer_name>
```

### Purge a specific subject

Remove a stuck message for a single namespace:

```shell
nats stream purge GKG_INDEXER --subject='sdlc.namespace.indexing.requested.<org>.<ns>'
```

### Purge all SDLC subjects

```shell
nats stream purge GKG_INDEXER --subject='sdlc.global.indexing.requested'
nats stream purge GKG_INDEXER --subject='sdlc.namespace.indexing.requested.*.*'
```

### Full NATS reset

Destroys all in-flight messages. The dispatcher will re-create them on the next cycle.

```shell
kubectl -n gkg delete pod gkg-nats-0
kubectl -n gkg delete pvc gkg-nats-js-gkg-nats-0
kubectl -n gkg wait --for=condition=ready pod/gkg-nats-0 --timeout=120s
```

## Retry mechanism

SDLC handlers use `max_attempts: 1` by default (no NATS-level retry). Instead, they rely on eventual consistency:

1. Handler fails and term-acks the message
2. Message is removed from the stream
3. On the next dispatcher cycle, the dispatcher publishes a new request for the same subject
4. The handler picks it up and retries from its checkpoint

A transient failure (ClickHouse timeout, network blip) resolves on its own within one dispatch interval.

### Extraction-level retries

Within a single handler invocation, the datalake batch extraction query retries up to 3 times with exponential backoff (100ms, 200ms, 400ms) before failing the handler.

## Troubleshooting

### Namespace not being indexed

1. Verify the namespace is enabled:

   ```sql
   SELECT *
   FROM gitlab_clickhouse_main_production.siphon_knowledge_graph_enabled_namespaces
   WHERE root_namespace_id = <namespace_id>
     AND _siphon_deleted = false;
   ```

2. Check if a message is in-flight (blocking new dispatches):

   ```shell
   nats stream info GKG_INDEXER --subjects
   ```

3. Check the checkpoint for errors or stale cursors:

   ```sql
   SELECT key, watermark, cursor_values
   FROM `<gkg-database>`.checkpoint
   WHERE key LIKE 'ns.<namespace_id>.%';
   ```

### Indexer stuck on a batch

If the indexer is consuming CPU but not making progress:

1. Check ClickHouse for long-running queries:

   ```sql
   SELECT query_id, elapsed, query
   FROM system.processes
   WHERE elapsed > 60
   ORDER BY elapsed DESC;
   ```

2. Kill the stuck query if needed:

   ```sql
   KILL QUERY WHERE query_id = '<query_id>';
   ```

3. The handler will fail and the dispatcher will re-create the request.

### Checkpoint has a corrupted cursor

If `cursor_values` contains invalid JSON:

```sql
ALTER TABLE `<gkg-database>`.checkpoint
DELETE WHERE key = '<position_key>';
```

The handler will restart extraction from epoch for that entity type.

## Graph table maintenance

The `TableCleanup` scheduled task runs `OPTIMIZE TABLE ... FINAL CLEANUP` on all graph tables every 24 hours. This compacts ReplacingMergeTree tables and physically removes soft-deleted rows.

To trigger manually:

```sql
OPTIMIZE TABLE `<gkg-database>`.gl_project FINAL CLEANUP;
```
