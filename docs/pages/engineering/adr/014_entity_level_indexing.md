---
title: "GKG ADR 014: Entity-level SDLC indexing"
creation-date: "2026-05-08"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Accepted (revised 2026-05-22)

## Date

2026-05-08

## Context

Software development lifecycle (SDLC) indexing had two handlers.
The global handler processed all global entities for each message.
The namespace handler processed all namespaced entities of one namespace for each message.
Both ran entity pipelines concurrently up to a fixed limit.
The work stayed bound to one NATS message and one engine worker slot.

This design had three problems:

- No concurrency across workers.
  A slow entity blocked all other entities in the same message, and the other workers stayed idle.
- No parallelism inside an entity.
  The indexer could not split a large entity across workers.
- Noisy neighbors.
  All namespaced entities shared one NATS consumer.
  A backlog of messages for large namespaces delayed all entity types.

## Decision

**We replace the two handlers with one entity handler for each ontology entity.**

Each handler owns the plan for one entity.
It subscribes to the existing global or namespace NATS topic for the scope of its entity.
The dispatcher publishes one message, and NATS delivers it to every entity handler for that scope.
This gives parallelism across entities without per-entity subjects or a new message type.
The handlers read the existing request messages, which already carry the watermark and the scope.

Parallelism inside an entity comes from partitions.
The ontology can declare a partition count for an entity pipeline.
A pipeline without a partition count runs without partitions.
On the first run, the handler computes ID-range partitions and runs them in parallel in the same process.
After all partitions complete, the handler consolidates their checkpoints into one completed checkpoint.
Later runs are incremental and do not partition.

### Partitions

A partition range is one more filter on the prepared query, together with the watermark filter and the traversal path filter.
Each range is half-open.

The handler follows these steps:

1. Load the parent checkpoint for the entity and scope.
   If it exists, the run is incremental and does not partition.
1. Read the minimum and maximum of the partition column from the data lake source, inside the traversal path of the scope.
   Slice that range into equal parts.
   The handler skips this probe for scopes with few rows.
1. Run one prepared query for each range, with a checkpoint for each partition.
1. After all partitions finish, write the parent checkpoint and tombstone the partition checkpoints.

If a partition fails, the handler does not write the parent checkpoint.
The next message enters the partition path again.
The handler skips each partition that completed in an earlier attempt, and the rows of that partition stay in the graph.
The parent checkpoint gets the oldest watermark of all partitions, and not the watermark of the current request.
Partitions from an earlier attempt still hold their older watermark.
The next incremental run then covers the window between that older watermark and the current one, so no data is lost.
If all partitions are already complete, the handler consolidates at once.
This case occurs when the last attempt completed every partition but stopped before consolidation.

### Partition column

The partition column is the first column of the sort key after the traversal path.
For merge requests and for global users, that column is the ID.
An entity whose first column after the traversal path has low cardinality is a bad candidate for partitions.
An example is the type column of notes, which has about 10 values.

We use range partitions and not hash partitions.
Benchmarks ran on the CI builds source table, with 100 million rows and a primary key of traversal path, ID, and partition ID.
They ran on a ClickHouse Cloud development instance on 2026-05-08, with four partitions inside one top-level group.

| Filter | Rows read | Bytes read | Duration |
|---|---:|---:|---:|
| No partition filter | 147,456 | 50.53 MiB | 109 ms |
| Hash of the ID, one of four buckets | 147,456 | 50.62 MiB | 649 ms |
| ID range | 147,456 | 13.05 MiB | 48 ms |

A range filter on a primary key column read 3.9 times less data than a hash filter.
ClickHouse evaluates the range condition before it reads the other columns, so it skips the decompression of rows that do not match.
The query plan showed that the range filter uses the traversal path and the ID in the primary key condition.
The hash filter uses only the traversal path.

### Checkpoint keys

A checkpoint key holds the scope and the entity.
A partition key also holds the partition index and the partition total.
The scope is global, or a namespace with its ID.
Keys without a partition match the previous format, so checkpoints need no migration.
The partition total in the key invalidates old partition checkpoints when the partition count changes.
Namespace deletion matches keys by the namespace prefix, so it covers both formats.
Consolidation inserts a completed row at the parent key and marks each partition row as deleted.

### Indexing status

At the time of this decision, one NATS key-value entry for each namespace tracked indexing progress.
On 2026-09-26, we removed that bucket, and status now reads the checkpoint tables.
For more information, see [ADR 010](010_graph_status_endpoint.md).

Per-entity handlers broke the single status entry.
When entity A completes and writes "indexed" while entity B still runs, the namespace status is wrong.

Each entity handler now writes its own status entry, keyed by the traversal path and the entity type.
Each handler writes only its own entry.
NATS message deduplication serializes runs for the same entity and scope, so the handlers need no coordination.

The status service gets the expected set of namespaced entity types from the ontology and reads one entry for each type.
A missing entry counts as not indexed.
The namespace state is the worst state of any entity.
From best to worst, the order is indexed, indexing, error, backfilling, not indexed, and unknown.
About 36 reads at under one millisecond each took about 18 ms in total.

The existing global and namespace topic configurations still set engine-level policy for retries, concurrency groups, and the dead letter queue.
The entity handlers subscribe to those topics.

## Consequences

Each entity type has its own NATS consumer on the shared subscription.
A slow merge request run does not delay issue or job runs.
To partition a large entity, a developer declares a partition count in the ontology, and the message bus does not change.
Checkpoint keys stay compatible, so a deploy does not reprocess data.

The change also has costs:

- The refactor is breaking.
  The global and namespace handlers are gone, together with the pipeline path that ran many plans.
  Only the single-plan path remains.
- Failure isolation across entities now depends on the retry and concurrency layer of the engine.
  Before, each handler collected the errors of its entities.
- Indexing status needs one entry for each entity and namespace, and the status service aggregates them.
  During rollout, the status service read the old and the new formats.
  Old entries became stale after all handlers ran the new code, and a time to live removes them.

## Why not the alternatives

### Keep two handlers and add parallelism inside each handler

The old pipeline already ran entities concurrently up to a limit.
A higher limit helped inside one handler run.
The work was still bound to one NATS message and one engine worker slot.
More workers could not help with the entities of one namespace.
One slow entity delayed the NATS acknowledgment for the whole message, and redelivery then ran all entities again.

### Per-entity subjects with a new request envelope

The first proposal had the dispatcher publish one message for each entity, scope, and partition.
The messages used a new subject for each entity type, and the handler routed by entity type.
This needed a new message type, a new NATS subject family, and a dispatcher that ran partitions.
That dispatcher computed boundaries, published partitions, and consolidated them.

We chose per-entity handlers on shared topics for these reasons:

- The existing global and namespace requests already carry the watermark and the scope.
  The new envelope added cost and no information.
- The entity handler already knows its plan, its scope, and its partition strategy.
  Dispatcher partitions move the same logic across a network hop and add a partition state machine to the dispatcher.
- Local parallel partitions are simpler than dispatcher partition messages.
  Failure isolation, retry, and acknowledgment all stay in one handler run.

### Hash partitions from the start

A hash partition needs no boundary computation and is stable across retries.
In the benchmark, a hash filter read 3.9 times more data than a range filter for the same scope.
A hash filter cannot use the ClickHouse primary key index.

## References

- [SDLC indexing design document](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/indexing/sdlc_indexing.md)
- [Observability design document](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/observability.md)
