# Namespace deletion

## Overview

When a namespace loses access to Orbit, all of its indexed data needs to be removed. This includes every node and edge in the graph tables, plus the SDLC and code indexing checkpoints that track indexing progress for that namespace.

Deletion is not immediate. A 30-day grace period gives operators time to reverse the decision before any data is removed. The scheduler detects deleted namespaces and queues them for future removal. The handler does the actual deletion once the grace period passes.

## How detection works

The `knowledge_graph_enabled_namespaces` table in the datalake tracks which namespaces have access to Orbit. When a namespace is disabled, Siphon's CDC pipeline sets `_siphon_deleted = true` on the corresponding row. The deletion scheduler picks this up on its next run.

The scheduler runs every 24 hours. Each run has two phases:

**Phase 1: Record newly deleted namespaces.** The scheduler loads its last checkpoint watermark and queries the datalake for any `knowledge_graph_enabled_namespaces` rows where `_siphon_deleted = true` within the watermark window. For each match, it inserts a row into the `namespace_deletion_schedule` table with `scheduled_deletion_date` set to 30 days from now. It then advances the checkpoint.

**Phase 2: Dispatch due deletions.** The scheduler queries `namespace_deletion_schedule` for entries where `scheduled_deletion_date <= now()`. For each one, it publishes a `NamespaceDeletionRequest` to NATS. If NATS rejects the publish as a duplicate (the previous request is still in-flight), the scheduler skips it and moves on.

```sql
-- Finding newly deleted namespaces
SELECT
    enabled.root_namespace_id AS namespace_id,
    CONCAT(toString(namespaces.organization_id), '/',
           toString(enabled.root_namespace_id), '/') AS traversal_path
FROM siphon_knowledge_graph_enabled_namespaces AS enabled
INNER JOIN siphon_namespaces AS namespaces
    ON enabled.root_namespace_id = namespaces.id
WHERE enabled._siphon_deleted = true
  AND enabled._siphon_watermark > {last_watermark}
  AND enabled._siphon_watermark <= {watermark}
```

## How deletion works

The `NamespaceDeletionHandler` consumes `NamespaceDeletionRequest` messages from NATS. Each message contains a `namespace_id` and a `traversal_path` (formatted as `<org_id>/<namespace_id>/`).

The handler follows these steps:

1. **Validate the traversal path.** The path must match `<org_id>/<namespace_id>/` where both segments are numeric. An empty or malformed path would cause `startsWith(traversal_path, '')` to match every row in every table, so the handler rejects anything that does not fit the expected format.

2. **Check if the namespace is still deleted.** Between scheduling and execution, an operator may have re-enabled the namespace. The handler queries the datalake to check the current state. If the namespace was re-enabled, the handler clears the schedule entry without touching any data and returns early.

3. **Soft-delete graph data.** For every namespaced node table and all configured edge tables, the handler runs an `INSERT INTO ... SELECT` that copies matching rows with `_deleted = true` and a fresh `_version` timestamp. The list of tables comes from the ontology at startup, so adding a new entity type or edge table to the ontology automatically includes it in namespace deletion. The handler attempts every table, records each outcome, and returns an error after the pass if any table failed. Successful tombstones are not rolled back.

4. **Soft-delete checkpoints.** Once every graph-table attempt succeeds, the handler removes the SDLC checkpoints (keyed by namespace position, e.g. `ns.42.Project`) and then the code indexing checkpoints (keyed by traversal path prefix). These are two sequential ClickHouse inserts rather than one transaction. This prevents stale checkpoints from interfering if the namespace is later re-enabled and re-indexed from scratch.

5. **Mark deletion complete.** The handler soft-deletes the `namespace_deletion_schedule` entry so the scheduler does not dispatch it again.

```sql
-- Soft-delete pattern for graph tables (generated per table from the ontology)
INSERT INTO {table} ({sort_key_columns}, _deleted, _version)
SELECT {sort_key_columns}, true, now64(6)
FROM {table}
WHERE startsWith(traversal_path, {traversal_path:String})
  AND _deleted = false
```

### Relationship to row-level deletion

Namespace deletion is separate from the row-level soft-delete that flows through Siphon's CDC pipeline. When an individual row is deleted in the source PostgreSQL database, Siphon sets `_siphon_deleted = true`, and the SDLC indexer carries the `_deleted` flag through to the graph table during normal ETL. Namespace deletion removes an entire namespace and all of its data at once.

### Reconciling moved entities

When a project or subgroup is transferred or reparented, Rails rewrites its `traversal_path` in the route tables (`project_namespace_traversal_paths`, `namespace_traversal_paths`) while keeping the same `id`. Those tables are `ReplacingMergeTree(version, deleted)` keyed on `id` alone, so both the old and the new path stay readable until a merge collapses them. The SDLC ETL then re-indexes the entity at its new path, creating a new ReplacingMergeTree key. The graph tables are sorted by `traversal_path`, so the old-path row is a distinct key that never gets touched and survives FINAL with `_deleted = false`, producing a duplicate live row under the stale path.

The `Project` and `Group` extracts collapse that join with `argMax(traversal_path, version)` scoped to the root being indexed, so a move within a root stops emitting the old path; moves across roots and rows written before the collapse still reach the reconciler.

On the same cadence as the deletion scheduler, `reconcile_moved_entities` runs one pass per enabled root namespace. For each root it reuses the deletion `INSERT INTO ... SELECT` but appends one predicate that narrows the scan from "all rows under the root" to "rows whose `traversal_path` is no longer a current route under the root":

```sql
INSERT INTO {table} ({sort_key_columns}, _deleted, _version)
SELECT {sort_key_columns}, true, now64(6)
FROM {table}
WHERE startsWith(traversal_path, {traversal_path:String})
  AND _deleted = false
  AND (SELECT count() FROM (current routes under root)) > 0
  AND traversal_path NOT IN (current routes under root)
```

A row at the entity's current path is always present in the route set (the ETL joins those same tables to write it), so it is never matched. Only old-path rows match. The `count() > 0` guard skips reconcile for a freshly-enabled root whose route tables have not replicated yet, so a transient empty-route window can never tombstone a just-indexed namespace. The pass is idempotent: a tombstoned row has `_deleted = true` and is filtered out, and a still-stale row re-inserts an identical tombstone that ReplacingMergeTree collapses.

## Data model

The `namespace_deletion_schedule` table tracks pending and completed deletions:

```sql
CREATE TABLE namespace_deletion_schedule (
    namespace_id Int64,
    traversal_path String,
    scheduled_deletion_date DateTime64(6, 'UTC'),
    _deleted Boolean DEFAULT false,
    _version DateTime64(6, 'UTC') DEFAULT now64(6)
) ENGINE = ReplacingMergeTree(_version)
ORDER BY (namespace_id, traversal_path);
```

The scheduler writes to this table when it detects a deleted namespace. The handler reads from it (indirectly, via the scheduler's dispatch) and soft-deletes the entry when deletion is complete or when the namespace was re-enabled.

## Error handling

Graph-table deletion is an idempotent, best-effort pass, not an all-or-nothing transaction. Every table is attempted even after a failure, so a failed request can leave successful tombstones visible alongside live rows from failed tables. The handler then returns an error. It does not start checkpoint cleanup or clear the schedule entry while any graph-table outcome is unsuccessful, so a later scheduler publication or configured NATS redelivery can retry the request.

Checkpoint cleanup is also nontransactional: the SDLC checkpoint insert completes before the code checkpoint insert starts. If the second insert fails, the schedule entry remains and a later attempt safely retries both. If `mark_deletion_complete` fails after graph data and checkpoints have been deleted, a later attempt re-executes the idempotent soft-deletes and cleanup before trying the schedule entry again.

## Observability

The namespace deletion module emits two metrics under the `indexer_namespace_deletion` meter:

| Metric | Type | Description |
|---|---|---|
| `indexer.namespace_deletion.table.duration` | Histogram | Duration of each table's soft-delete query |
| `indexer.namespace_deletion.table.errors` | Counter | Per-table deletion failures |

The scheduler uses the shared `scheduler` meter documented in [observability](../observability.md): `scheduler.task.runs`, `scheduler.task.requests.published`, and `scheduler.task.requests.skipped`.
