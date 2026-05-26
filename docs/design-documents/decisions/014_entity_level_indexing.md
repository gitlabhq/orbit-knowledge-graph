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

Originally, `GlobalHandler` processed global entities per message and
`NamespaceHandler` processed all namespaced entities per namespace per message.
Both ran entity pipelines concurrently behind `max_concurrent_entities`, but
the work was bound to a single NATS message and a single engine worker slot.

Problems:

1. No cross-worker concurrency. A slow entity blocks all others in the same
   message and the other workers sit idle.
2. No intra-entity parallelism. A large entity cannot be split across workers.
3. Noisy neighbours. All namespaced entities shared one NATS consumer, so a
   backlog of large-namespace messages delayed all entity types.

## Decision

Replace `GlobalHandler` and `NamespaceHandler` with one `EntityHandler` per
ontology entity. Each handler owns a single `Plan` and subscribes to the
existing global/namespace NATS topic for its scope. The dispatcher publishes
one message; every entity handler for that scope receives it (NATS pub/sub),
which gives cross-entity parallelism without per-entity subjects or a new
message type.

Intra-entity parallelism comes from `partition_overrides`: when configured for
an entity, the handler computes id-range partitions on the fly during the
first run and fans them out across a `JoinSet`. Once all partitions complete,
the partition checkpoints are consolidated into a single completed checkpoint
and subsequent runs skip partitioning.

### Handler

```rust
pub struct EntityHandler {
    plan: Plan,
    scope: EtlScope,
    pipeline: Arc<Pipeline>,
    datalake: Arc<dyn DatalakeQuery>,
    checkpoint_store: Arc<dyn CheckpointStore>,
    partition_strategy: Option<PartitionStrategy>,
    // ...
}
```

Registration loops `Ontology::nodes()`, builds a `Plan` per entity, and
registers one `EntityHandler` against either `GlobalIndexingRequest` or
`NamespaceIndexingRequest` based on the entity's `EtlScope`. The handler
deserializes the existing request types, no new message envelope is needed.

### Partitioning

Filters compose via the existing `Filter` trait. A partition range is just
another filter (half-open id-range), so it belongs on the prepared query:

```rust
plan.prepare()
    .with(WatermarkFilter { ... })
    .with(TraversalPathFilter { ... })
    .into_partitions(ranges)  // -> Vec<(PartitionAssignment, PreparedQuery)>
```

`PartitionStrategy` holds the partition count, the id column, and the
datalake table needed to probe `min/max`. `PartitionAssignment` carries the
resulting half-open bounds for one shard.

The handler:

1. Loads the parent checkpoint `{scope_key}.{entity}`. If it exists, this is
   an incremental run, no partitioning.
2. Otherwise, calls `strategy.compute_ranges(...)`, which runs
   `SELECT min/max(col) FROM source WHERE [traversal_path...]` against the
   datalake (using the ETL's `source` table) and slices the result evenly.
3. `base_query.into_partitions(ranges)` yields N prepared queries.
4. Each is spawned on a `JoinSet` against `Pipeline::run_plan` with its own
   checkpoint key `{scope_key}.{entity}.p{idx}of{total}`.
5. After all partitions finish, `CheckpointStore::consolidate(parent_key,
   watermark)` writes the parent key and tombstones the partition rows.

#### Retry behavior

If any partition fails before they all complete, the parent checkpoint stays
unwritten and the next message re-enters the partitioning path. To avoid
re-extracting work that already succeeded:

- `run_partitions` loads each partition's checkpoint before spawning. Any
  partition whose checkpoint has `cursor_values: None` (a successful
  `save_completed` from a prior attempt) is skipped: its task is never
  spawned, and the rows it indexed last time stay in the destination.
- `consolidate` writes the parent at `min(partition watermarks)` rather than
  the current `request.watermark`. Partitions that completed in an earlier
  attempt still hold their original (older) watermark; pinning the parent to
  the minimum keeps the next incremental run covering the
  `(old_watermark, request.watermark]` window for those id-ranges, so no data
  is lost.

If every partition is already complete (e.g. the previous attempt finished
all partitions but crashed before consolidation), `run_partitions` spawns
nothing and `consolidate` runs immediately.

#### Partition column: first non-scope sort key column

The range filter is applied on the first non-`traversal_path` column of the
plan's sort key.

```sql
AND {partition_column} >= '{lower_bound}' AND {partition_column} < '{upper_bound}'
```

Examples from the current ontology:

| Entity | Source `order_by` | Partition column |
|---|---|---|
| MergeRequest | `[traversal_path, id]` | `id` |
| User (global) | `[id]` | `id` |
| JobMetadata | `[traversal_path, build_id]` | `build_id` |

Entities where the first non-scope column has low cardinality (e.g., Note's
`noteable_type` with ~10 enum values) are poor partitioning candidates and
should not have `partition_overrides` set.

#### Why range over hash

Benchmarks on `siphon_p_ci_builds` (100M rows, PRIMARY KEY
`(traversal_path, id, partition_id)`, ClickHouse Cloud dev instance,
2026-05-08) show range filtering on a primary key column reads 3.9× less
data than hash. ClickHouse evaluates the range condition via PREWHERE and
skips decompressing non-matching columns:

| Filter (4 partitions, `startsWith(traversal_path, '158/')`) | read_rows | read_bytes | duration |
|---|---|---|---|
| Baseline (no partition filter) | 147,456 | 50.53 MiB | 109 ms |
| `cityHash64(id) % 4 = 0` | 147,456 | 50.62 MiB | 649 ms |
| `id >= 548 AND id < 24726584` | 147,456 | 13.05 MiB | 48 ms |

`EXPLAIN indexes = 1` confirms range uses both `traversal_path` and `id` in
the primary key condition (`generic exclusion search`), while hash uses only
`traversal_path` (`binary search`).

### Checkpoint key design

```plaintext
global.{entity_kind}                                  # global, no partition
global.{entity_kind}.p{idx}of{total}                  # global, partitioned
ns.{namespace_id}.{entity_kind}                       # namespaced, no partition
ns.{namespace_id}.{entity_kind}.p{idx}of{total}       # namespaced, partitioned
```

Non-partitioned keys match the previous format (`global.User`,
`ns.100.MergeRequest`), so no checkpoint migration is needed. The `of{total}`
suffix invalidates old partitioned checkpoints when the partition count
changes. Namespace deletion's `startsWith(key, 'ns.{id}.')` matches both
formats.

`CheckpointStore` gains `load_by_prefix` and `consolidate`. Consolidation
inserts a completed row at the parent key and tombstones every row matching
`{parent}.p%` via the `_deleted` column on the `ReplacingMergeTree`.

### Indexing status tracking

Today, one NATS KV key per namespace tracks indexing progress
(`orbit_indexing_progress` bucket, consumed by `GraphStatusService`). With
per-entity handlers, this breaks: Entity A completing and writing "Indexed"
while Entity B is still running gives a wrong answer for the namespace.

#### Per-entity status key

Each entity handler writes its own status key:

```plaintext
status.{dotted_traversal_path}.{entity_kind}
```

For example: `status.42.9970.MergeRequest`, `status.42.9970.Issue`.

```rust
fn entity_status_key(traversal_path: &str, entity_kind: &str) -> String {
    let dotted = gkg_utils::traversal_path::to_dotted(traversal_path);
    format!("status.{dotted}.{entity_kind}")
}
```

Each handler writes only its own key. NATS message deduplication serializes
runs for the same (entity, scope) pair, so no cross-handler coordination is
needed.

#### GraphStatusService aggregation

`GraphStatusService` uses the ontology to derive the expected set of
namespaced entity kinds, then reads one NATS KV key per entity. Missing keys
are treated as `NotIndexed`. The namespace-level state is the worst of any
entity's state:

```rust
// Priority: higher = worse. NotIndexed dominates (missing key = not started).
fn state_priority(state: IndexingState) -> u8 {
    match state {
        IndexingState::Indexed     => 0,
        IndexingState::Indexing    => 1,
        IndexingState::Error       => 2,
        IndexingState::Backfilling => 3,
        IndexingState::NotIndexed  => 4,
        IndexingState::Unknown     => 5,
    }
}
```

~36 KV reads at sub-millisecond each ≈ ~18ms total.

#### Migration

During rollout, `GraphStatusService` checks both old-format keys
(`status.42.9970`) and new entity-suffixed keys. Old keys become stale once
all handlers run the new code and can be purged by TTL.

### Configuration

`partition_overrides` lives on the entity handler config. Entities without an
override run un-partitioned.

```yaml
handlers:
  entity-handler:
    batch_size_overrides:
      WorkItem: 50000
    partition_overrides:
      Job: 5
```

The existing `global-handler` and `namespace-handler` topic configs continue
to govern engine-level policy (retry, concurrency group, DLQ) since per-entity
handlers subscribe to those topics.

## Why not the alternatives

### Keep two handlers, add internal entity-level parallelism

`Pipeline::run` (now removed) already ran entities concurrently behind a
semaphore. Increasing `max_concurrent_entities` helped within one handler
invocation, but the work was still bound to one NATS message and one engine
worker slot. Multiple workers could not help with a single namespace's
entities, and one slow entity delayed the NATS ack for the entire message,
triggering redelivery of all entities.

### Per-subject `sdlc.entity.indexing.requested.>` with an `EntityIndexingRequest` envelope

Originally proposed: a dispatcher publishes one message per (entity, scope,
partition) on a per-entity-kind subject, and the handler routes by
`entity_kind`. This required a new message type, a new NATS subject family,
and a dispatcher that owned partition orchestration (boundary computation,
publishing, consolidation).

Switched to per-entity handlers on shared topics because:

- The existing `GlobalIndexingRequest` and `NamespaceIndexingRequest` already
  carry everything needed (watermark + scope). The new envelope was pure
  overhead.
- The dispatcher had nothing useful to do, the entity handler already knows
  its plan, its scope, its partition strategy. Pushing that work to the
  dispatcher just moved the same logic across a network hop and added a
  partition-state-machine to the dispatcher.
- Local `JoinSet` partition execution is simpler than dispatcher-orchestrated
  partition messages: failure isolation, retry, and ack semantics all stay in
  one handler invocation.

### Hash partitioning from day 1

`cityHash64(column) % N = i` needs no boundary computation and is stable
across retries. Benchmarks above show it reads 3.9× more data than range
for the same scope. Hash cannot benefit from ClickHouse's primary key index.

## Consequences

### What improves

- Each entity kind has its own NATS consumer (per-handler registration on the
  shared subscription), so a slow MergeRequest does not delay Issue or Job
  processing.
- Large entities can be partitioned with no message-bus changes:
  `partition_overrides: { Job: 5 }` and the handler does the rest.
- Backward-compatible checkpoint keys, no re-processing on deploy.

### What gets harder

- Breaking refactor: `GlobalHandler` and `NamespaceHandler` are gone, along
  with `Pipeline::run` (multi-plan). Only `Pipeline::run_plan` remains.
  Failure isolation across entities now relies on the engine's
  retry/concurrency layer rather than per-handler error aggregation.
- Indexing status: one KV key per (entity, namespace) instead of one per
  namespace. `GraphStatusService` aggregates with worst-state-wins. Old and
  new key formats coexist during rollout.

## References

- [SDLC indexing design document](../indexing/sdlc_indexing.md)
- [Observability design document](../observability.md)
- Handler implementation: `crates/indexer/src/modules/sdlc/handler/entity.rs`
- Partitioning logic: `crates/indexer/src/modules/sdlc/partitioning.rs`
- Range vs hash partition benchmarks: `siphon_p_ci_builds` (100M rows, ClickHouse Cloud dev instance, 2026-05-08)
