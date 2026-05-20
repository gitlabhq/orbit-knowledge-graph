---
title: "GKG ADR 014: Entity-level SDLC indexing"
creation-date: "2026-05-08"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-05-08

## Context

Today, `GlobalHandler` processes global entities (currently just User)
per message, and `NamespaceHandler` processes all namespaced entities
(MergeRequest, Issue, Pipeline, Job, ...) per namespace per message. Both
run entity pipelines concurrently behind `max_concurrent_entities`, but the
work is bound to a single NATS message and a single engine worker slot.

Problems:

1. **No cross-worker concurrency.** A slow entity blocks all others in the
   same message. Other workers sit idle.
2. **No intra-entity parallelism.** A large entity cannot be split across
   workers.
3. **Noisy neighbors.** All namespaced entities share one NATS consumer. A
   backlog of large-namespace messages delays all entity types.

## Decision

Replace `GlobalHandler` and `NamespaceHandler` with a single handler type,
`EntityIndexingHandler`, where one NATS message = one entity kind, optionally
scoped to a namespace. The dispatcher owns partition orchestration: it
reads checkpoint state, computes quantile boundaries, and publishes one
message per (entity, scope, partition) combination. Each message carries
an optional `PartitionAssignment` so the handler applies the range filter
without needing checkpoint or quantile awareness.

Incremental indexing (where a completed unified checkpoint already exists)
runs as a single non-partitioned message — it processes only the delta
since the last watermark and is already fast.

### Message type

```rust
pub struct EntityIndexingRequest {
    pub entity_kind: String,
    pub watermark: DateTime<Utc>,
    pub scope: IndexingScope,
    pub partition: Option<PartitionAssignment>,
}

pub enum IndexingScope {
    Global,
    Namespace { namespace_id: i64, traversal_path: String },
}

pub struct PartitionAssignment {
    pub index: u32,
    pub total: u32,
    pub column: String,
    pub bounds: PartitionBounds,
}
```

The dispatcher attaches a `PartitionAssignment` when it decides to split
work. The handler applies the range filter from the assignment to its
extract query. `PartitionBounds` and `Partitioner` are internal
dispatcher types used for checkpoint keys and quantile computation.

### NATS subject hierarchy

```plaintext
sdlc.entity.indexing.requested.{entity_kind}.global             # global
sdlc.entity.indexing.requested.{entity_kind}.{org_id}.{ns_id}  # namespaced
```

A single `EntityIndexingHandler` subscribes to
`sdlc.entity.indexing.requested.>` and routes internally by
`entity_kind`. The `GKG_INDEXER` stream adds this wildcard subject to
accept all entity messages. Multiple engine workers pull from the same
NATS consumer concurrently, so cross-entity parallelism comes from the
worker pool rather than per-entity consumers.

The stream's `max_messages_per_subject: 1` with
`discard_new_per_subject: true` deduplicates at the (entity, scope)
level: if a handler has not acked the previous message for that exact
subject, the dispatcher's publish is silently rejected.

### Handler and pipeline

A single `EntityIndexingHandler` holds a
`HashMap<String, Arc<dyn EntityPipeline>>`, keyed by entity kind. On
each message it deserializes the `EntityIndexingRequest`, looks up the
pipeline by `entity_kind`, and delegates. Unknown entity kinds log a
warning and return `Ok(())`.

All current entities use `SimpleEntityPipeline`, which runs a single
`PipelinePlan` against the existing `Pipeline::run_plan`. If the request
carries a `PartitionAssignment`, the pipeline applies the range filter
to its extract query and uses a partition-specific checkpoint key.
Otherwise it uses the unified key. The pipeline has no partition
orchestration logic — it runs exactly what the dispatcher told it to.

Future entities (e.g., SystemNotes) can implement `EntityPipeline` with
custom logic instead of using `SimpleEntityPipeline`.

### Partitioning (initial indexing only)

Partitioning applies only during initial indexing (no unified checkpoint
for the entity+scope). Incremental indexing processes only the delta since
the last watermark and completes in seconds; partitioning it would add
overhead for no gain. The dispatcher checks the checkpoint store to
decide (see [Dispatcher](#dispatcher)).

#### Partition column: first non-scope sort key column

When a `PartitionAssignment` is present, the handler applies a range
filter on the **first non-scope column** of the entity's source
`order_by`. For namespaced entities, the scope column is
`traversal_path`. For global entities, there is no scope column.

```sql
AND {partition_column} >= '{lower_bound}' AND {partition_column} < '{upper_bound}'
```

Examples from the current ontology:

| Entity | Source `order_by` | Partition column |
|---|---|---|
| MergeRequest | `[traversal_path, id]` | `id` |
| User (global) | `[id]` | `id` |
| JobMetadata | `[traversal_path, build_id]` | `build_id` |
| Note | `[traversal_path, noteable_type, noteable_id, id]` | `noteable_type` |
| deployed_to | `[traversal_path, deployment_id, merge_request_id]` | `deployment_id` |

Entities where the first non-scope column has low cardinality (e.g.,
Note's `noteable_type` with ~10 enum values) are poor partitioning
candidates and should not have `partition-overrides` set.

Derivation and filter generation:

```rust
fn partition_column(order_by: &[String], scope: EtlScope) -> Option<&str> {
    let skip = match scope {
        EtlScope::Namespaced => 1, // skip traversal_path
        EtlScope::Global => 0,
    };
    order_by.get(skip).map(String::as_str)
}

fn partition_filter_sql(column: &str, bounds: &PartitionBounds) -> String {
    match bounds {
        PartitionBounds::Range { lower_bound, upper_bound } => format!(
            "{column} >= '{lower_bound}' AND {column} < '{upper_bound}'"
        ),
    }
}
```

`partition_column` runs once at dispatcher startup; `partition_filter_sql`
runs at handler execution time using the bounds from the
`PartitionAssignment`. The range filter composes with the existing `WHERE`
and keyset cursor as a conjunct and does not affect sort order.

#### Why range over hash

Benchmarks on `siphon_p_ci_builds` (100M rows, PRIMARY KEY
`(traversal_path, id, partition_id)`, ClickHouse Cloud dev instance,
2026-05-08) show range filtering on a primary key column reads 3.9×
less data than hash. ClickHouse evaluates the range condition via
PREWHERE and skips decompressing non-matching columns:

| Filter (4 partitions, `startsWith(traversal_path, '158/')`) | read_rows | read_bytes | duration |
|---|---|---|---|
| Baseline (no partition filter) | 147,456 | 50.53 MiB | 109 ms |
| `cityHash64(id) % 4 = 0` | 147,456 | 50.62 MiB | 649 ms |
| `id >= 548 AND id < 24726584` | 147,456 | 13.05 MiB | 48 ms |

`EXPLAIN indexes = 1` confirms range uses both `traversal_path` and
`id` in the primary key condition (`generic exclusion search`), while
hash uses only `traversal_path` (`binary search`). Both select the same
granules for this scope, but range's PREWHERE reads **3.9× fewer bytes**
and runs **13× faster**.

See [Hash partitioning from day 1](#hash-partitioning-from-day-1)
for why hash was considered and rejected.

#### Boundary computation

The dispatcher computes quantile boundaries using `quantilesTDigest`
each time it plans partition jobs. Boundaries are not persisted — they
are recomputed from the source table. This works because:

- Boundary drift between runs does not cause data gaps. Each partition
  has its own cursor-based checkpoint, so rows near a shifted boundary
  are processed by whichever partition covers them. The cursor prevents
  re-processing within a partition.
- The quantile query is cheap (single aggregate over a primary key
  column) compared to the actual ETL work.

### Checkpoint key design

```plaintext
global.{entity_kind}                                  # global, no partition
global.{entity_kind}.p{idx}of{total}                  # global, partitioned
ns.{namespace_id}.{entity_kind}                       # namespaced, no partition
ns.{namespace_id}.{entity_kind}.p{idx}of{total}       # namespaced, partitioned
```

Non-partitioned keys match the current format (`global.User`,
`ns.100.MergeRequest`), so no checkpoint migration is needed. The
`of{total}` suffix invalidates old partitioned checkpoints when the
partition count changes. Namespace deletion's
`startsWith(key, 'ns.{id}.')` matches both formats.

Key construction:

```rust
fn entity_position_key(scope: &IndexingScope) -> String {
    match scope {
        IndexingScope::Global => "global".to_string(),
        IndexingScope::Namespace { namespace_id, .. } => format!("ns.{namespace_id}"),
    }
}

fn entity_checkpoint_key(
    scope: &IndexingScope,
    entity_kind: &str,
    partition: Option<&PartitionAssignment>,
) -> String {
    let base = entity_position_key(scope);
    match partition {
        None => format!("{base}.{entity_kind}"),
        Some(p) => format!(
            "{base}.{entity_kind}.p{}of{}",
            p.partition_index, p.total_partitions
        ),
    }
}
```

These produce the same prefixes as the current `namespace_position_key`
and `"global"` constants (e.g., `ns.100.MergeRequest`,
`ns.100.MergeRequest.p2of4`). Pipeline checkpoint load/save is unchanged.

### Dispatcher

`EntityDispatcher` owns partition orchestration. On each scheduled run it:

1. Loads enabled namespaces from the datalake.
2. For each (entity, scope) pair, calls `plan_dispatch` which reads the
   checkpoint store to decide what to publish:
   - Unified checkpoint already completed → publish one non-partitioned
     message (incremental delta).
   - All partition checkpoints completed → consolidate (write unified
     checkpoint with min watermark), then publish non-partitioned.
   - Some partitions incomplete or none started → compute quantile
     boundaries via `Partitioner`, publish one message per
     pending partition with a `PartitionAssignment`.
3. `PublishDuplicate` is handled silently (NATS dedup).

Consolidation writes a unified checkpoint with `watermark = min(partition
watermarks)`. After consolidation, subsequent runs publish non-partitioned
messages (incremental).

### Configuration

All entity handlers share a single concurrency group (`"sdlc"`). The
engine's worker pool caps total concurrent SDLC handlers via the group
semaphore. Per-entity groups can be added later without code changes.

`partition-overrides` lives on the handler config. The dispatcher reads
it at startup to build partition configs per entity.

```yaml
handlers:
  entity-handler:
    concurrency-group: sdlc
    datalake-batch-size: 500000
    batch-size-overrides:
      MergeRequest: 100000
    partition-overrides:
      MergeRequest: 4
      Job: 4
```

### Indexing status tracking

Today, one NATS KV key per namespace tracks indexing progress
(`orbit_indexing_progress` bucket, consumed by `GraphStatusService`).
This breaks with entity-level messages: Entity A completing and writing
"Indexed" while Entity B is still running gives a wrong answer.

#### New key scheme: per-entity status

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

Each handler writes only its own key. NATS message deduplication
serializes runs for the same (entity, scope) pair, so no cross-handler
coordination is needed.

#### GraphStatusService aggregation

`GraphStatusService` uses the ontology to derive the expected set of
namespaced entity kinds, then reads one NATS KV key per entity. Missing
keys are treated as `NotIndexed`. The namespace-level state is the worst
of any entity's state:

```rust
// Priority: higher = worse. NotIndexed dominates (missing key = not started).
fn state_priority(state: IndexingState) -> u8 {
    match state {
        IndexingState::Indexed    => 0,
        IndexingState::Indexing   => 1,
        IndexingState::Error      => 2,
        IndexingState::Backfilling => 3,
        IndexingState::NotIndexed => 4,
        IndexingState::Unknown    => 5,
    }
}
```

~36 KV reads at sub-millisecond each ≈ ~18ms total.

#### Migration

During rollout, `GraphStatusService` checks both old-format keys
(`status.42.9970`) and new entity-suffixed keys. Old keys become stale
once all handlers run the new code and can be purged by TTL.

## Why not the alternatives

### Keep two handlers, add internal entity-level parallelism

`Pipeline::run` already runs entities concurrently behind a semaphore.
Increasing `max_concurrent_entities` helps within one handler invocation,
but the work is still bound to one NATS message and one engine worker slot.
Multiple workers cannot help with a single namespace's entities. Worse, one
slow entity delays the NATS ack for the entire message, triggering
redelivery of all entities.

### Hash partitioning from day 1

`cityHash64(column) % N = i` needs no boundary computation and is
stable across retries. Benchmarks show it reads **3.9× more
data** than range for the same scope (50.62 MiB vs 13.05 MiB on
`siphon_p_ci_builds`, 100K row scope, 4 partitions). Hash cannot
benefit from ClickHouse's primary key index — `EXPLAIN indexes = 1`
confirms only `traversal_path` is used, not `id`. For entities with
deeper sort keys (Note, MergeRequestDiffFile) neither approach provides
index benefits, but those entities are poor partitioning candidates
anyway (low-cardinality first columns). Boundary drift between runs is
not a concern because each partition resumes from its own cursor
checkpoint.

### Per-entity concurrency groups from day 1

Each entity kind could get its own concurrency group (e.g.,
`sdlc.merge_request: 3`, `sdlc.user: 1`). This gives finer-grained
isolation but requires operators to configure 38 group limits. A shared
`sdlc` group with the existing global cap is simpler and sufficient
until empirical data shows a specific entity needs throttling.

## Consequences

**What improves:**

- Entity kinds process independently. Slow MergeRequest does not delay
  Issue or Job.
- Large entities can be partitioned: 4-partition MergeRequest = 4 workers.
- `EntityPipeline` trait is an extension point for custom logic
  (e.g., SystemNotes).
- Backward-compatible checkpoint keys. No re-processing on deploy.

**What gets harder:**

- Breaking config change: `global-handler`/`namespace-handler` →
  `entity-handler`.
- Indexing status: one key per (entity, namespace) instead of per
  namespace. `GraphStatusService` aggregates with worst-state-wins.
  Old and new key formats coexist during rollout.
- Dispatcher complexity: `EntityDispatcher` owns a partition state
  machine (scan checkpoints, compute boundaries, consolidate). The
  handler pipeline is simpler in exchange.

## References

- [SDLC indexing design document](../indexing/sdlc_indexing.md)
- [Observability design document](../observability.md)
- Current handler implementations: `crates/indexer/src/modules/sdlc/handler/{global,namespace}.rs`
- NATS stream configuration: `crates/nats-client/src/client.rs:91-104`
- Range vs hash partition benchmarks: `siphon_p_ci_builds` (100M rows, ClickHouse Cloud dev instance, 2026-05-08)
