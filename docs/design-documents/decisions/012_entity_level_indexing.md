---
title: "GKG ADR 012: Entity-level SDLC indexing"
creation-date: "2026-05-08"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-05-08

## Context

Today, `GlobalHandler` processes all global entities (User, Runner) per
message, and `NamespaceHandler` processes all namespaced entities
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
scoped to a namespace. For initial indexing of large entities, the dispatcher
can split work across multiple workers via partitioning. Incremental indexing
(where a completed checkpoint already exists) always runs as a single
message — it processes only the delta since the last watermark and is already
fast.

The dispatcher computes range boundaries per partition and includes them
in the message. The pipeline applies a range filter on the **first
non-scope sort key column** (derived from the ontology at startup). The
partition column is `id` for most entities today. `PartitionStrategy` is
a tagged enum (`#[serde(tag = "type")]`) — adding a new strategy (e.g.,
hash for low-cardinality columns) is one variant and one match arm in
`partition_filter_sql`, with no changes to NATS subjects, checkpoint
keys, or message routing.

### Message type

```rust
pub struct EntityIndexingRequest {
    pub entity_kind: String,
    pub watermark: DateTime<Utc>,
    pub scope: IndexingScope,
    pub partition: Option<PartitionSpec>,
}

pub enum IndexingScope {
    Global,
    Namespace { namespace_id: i64, traversal_path: String },
}

pub struct PartitionSpec {
    pub partition_index: u32,
    pub total_partitions: u32,
    pub strategy: PartitionStrategy,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum PartitionStrategy {
    Range { lower_bound: String, upper_bound: String },
    // Future: Hash (no additional fields needed — index + total is sufficient)
}
```

### NATS subject hierarchy

```plaintext
sdlc.entity.indexing.requested.{entity_kind}.global                      # global, no partition
sdlc.entity.indexing.requested.{entity_kind}.global.p{id}               # global, partitioned
sdlc.entity.indexing.requested.{entity_kind}.{org_id}.{ns_id}           # namespaced, no partition
sdlc.entity.indexing.requested.{entity_kind}.{org_id}.{ns_id}.p{id}    # namespaced, partitioned
```

Each entity kind subscribes via `sdlc.entity.indexing.requested.{kind}.>`,
so each gets its own NATS consumer. The `GKG_INDEXER` stream adds a single
wildcard subject `sdlc.entity.indexing.requested.>` to accept all entity
messages.

The stream's `max_messages_per_subject: 1` with
`discard_new_per_subject: true` deduplicates at the (entity, scope,
partition) level: if a handler has not acked the previous message for that
exact subject, the dispatcher's publish is silently rejected.

### Handler and pipeline

One `EntityIndexingHandler` per entity kind (38 for the current ontology),
each with its own NATS consumer. Each handler holds an
`Arc<dyn EntityPipeline>`. All current entities use `BasePipeline`, which
translates the request into a `PipelineContext` and calls `Pipeline::run`:

```rust
pub struct BasePipeline {
    plan: PipelinePlan,
    partition_column: Option<String>, // from ontology, see Partition column
    pipeline: Arc<Pipeline>,
}

// In EntityPipeline::execute:
let mut plan = self.plan.clone();
if let (Some(spec), Some(column)) = (&request.partition, &self.partition_column) {
    plan.extract_query = plan.extract_query
        .with_additional_filter(&partition_filter_sql(column, spec));
}
let position_key = entity_checkpoint_key(
    &request.scope, &request.entity_kind, request.partition.as_ref(),
);
let base_conditions = match &request.scope {
    IndexingScope::Global => BTreeMap::new(),
    IndexingScope::Namespace { traversal_path, .. } => {
        BTreeMap::from([("traversal_path".to_string(), traversal_path.clone())])
    }
};
self.pipeline.run(
    &[plan],
    &PipelineContext { watermark: request.watermark, position_key, base_conditions },
    context.destination, &context.progress, 1,
).await
```

Future entities (e.g., SystemNotes) can implement `EntityPipeline` with
custom logic instead of using `BasePipeline`.

### Partitioning (initial indexing only)

Partitioning applies only during initial indexing (no completed checkpoint
for the entity+scope). Incremental indexing processes only the delta since
the last watermark and completes in seconds; partitioning it would add
overhead for no gain. The dispatcher checks the checkpoint store to decide
(see [Dispatcher](#dispatcher)).

#### Partition column: first non-scope sort key column

When `partition` is present, the pipeline applies a range filter on the
**first non-scope column** of the entity's source `order_by`. For
namespaced entities, the scope column is `traversal_path`. For global
entities, there is no scope column.

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

fn partition_filter_sql(column: &str, spec: &PartitionSpec) -> String {
    match &spec.strategy {
        PartitionStrategy::Range { lower_bound, upper_bound } => format!(
            "{column} >= '{lower_bound}' AND {column} < '{upper_bound}'"
        ),
    }
}
```

`partition_column` runs once at registration; `partition_filter_sql` runs
at execution time when the request has a `PartitionSpec`. The range
filter composes with the existing `WHERE` and keyset cursor as a
conjunct and does not affect sort order.

#### Why range over hash

Benchmarks on `siphon_p_ci_builds` (100M rows, PRIMARY KEY
`(traversal_path, id, partition_id)`, ClickHouse Cloud dev instance,
2026-05-08) confirm that range filtering on a primary key column reads
significantly less data than hash, because ClickHouse evaluates the
range condition via PREWHERE and skips decompressing non-matching
columns:

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

#### Boundary stability

The dispatcher computes quantile boundaries once per (entity, scope)
pair at the start of initial indexing and stores them in NATS KV
(`partition_boundaries` bucket, key
`boundaries.{entity_kind}.{scope_key}`). Subsequent dispatch cycles
load the stored boundaries rather than recomputing. This prevents
boundary drift from causing gaps between partitions if a partition
fails and is re-dispatched. The boundary key is deleted during
consolidation (step 3 of the [Dispatcher](#dispatcher)).

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
    partition: Option<&PartitionSpec>,
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

A single `EntityDispatcher` replaces `GlobalDispatcher` and
`NamespaceDispatcher`. It loads all checkpoints in one bulk query per
cycle, then for each (entity, scope) pair decides what to publish:

1. **Unpartitioned entity, or entity with completed unpartitioned
   checkpoint** → publish 1 incremental message.
2. **Partitioned entity, not all partitions completed** → re-publish
   partition messages for incomplete partitions only. No incremental
   message is published until every partition has succeeded.
3. **Partitioned entity, all N partition checkpoints completed** →
   consolidate: write a completed unpartitioned checkpoint
   (`ns.100.MergeRequest`) with `watermark = min(partition watermarks)`,
   then publish 1 incremental message. The partition checkpoints can be
   cleaned up in a future cycle.
4. **No checkpoint at all (first run)** → if `partition-overrides` > 1,
   compute quantile boundaries for the partition column, store them in
   NATS KV, and publish N partition messages with range bounds. Otherwise
   publish 1 unpartitioned message.

`PublishDuplicate` is handled silently (NATS dedup).

The consolidation in step 3 is the transition from partitioned initial
indexing to unpartitioned incremental indexing. Until all partitions
succeed, the entity stays in initial mode — a single slow or failed
partition does not cause the others to re-process, but incremental
indexing cannot begin until the full dataset has been covered.

### Configuration

All entity handlers share a single concurrency group (`"sdlc"`). The
engine's worker pool caps total concurrent SDLC handlers via the group
semaphore. Per-entity groups can be added later without code changes.

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

### One NATS consumer with internal routing

A single handler subscribing to `sdlc.entity.indexing.requested.>` could
route internally by entity kind. Simpler, but with one consumer one
entity's message backlog delays all others. Per-entity consumers give
physical isolation at the NATS level.

### Hash partitioning from day 1

`cityHash64(column) % N = i` needs no boundary computation and is
completely stable across retries. Benchmarks show it reads **3.9× more
data** than range for the same scope (50.62 MiB vs 13.05 MiB on
`siphon_p_ci_builds`, 100K row scope, 4 partitions). Hash cannot
benefit from ClickHouse's primary key index — `EXPLAIN indexes = 1`
confirms only `traversal_path` is used, not `id`. For entities with
deeper sort keys (Note, MergeRequestDiffFile) neither approach provides
index benefits, but those entities are poor partitioning candidates
anyway (low-cardinality first columns). The boundary stability concern
is addressed by persisting boundaries in NATS KV on first computation.

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
- Per-entity NATS consumers prevent noisy neighbors.
- `EntityPipeline` trait is an extension point for custom logic
  (e.g., SystemNotes).
- Backward-compatible checkpoint keys. No re-processing on deploy.

**What gets harder:**

- 38 NATS consumers instead of 2 (operators monitor per entity kind).
- Range partitioning requires quantile boundary computation on first
  dispatch and NATS KV storage for boundary stability.
- Breaking config change: `global-handler`/`namespace-handler` →
  `entity-handler`.
- Indexing status: one key per (entity, namespace) instead of per
  namespace. `GraphStatusService` aggregates with worst-state-wins.
  Old and new key formats coexist during rollout.
- Dispatcher needs one bulk checkpoint query per dispatch cycle to
  distinguish initial from incremental indexing.

## References

- [SDLC indexing design document](../indexing/sdlc_indexing.md)
- [Observability design document](../observability.md)
- Current handler implementations: `crates/indexer/src/modules/sdlc/handler/{global,namespace}.rs`
- NATS stream configuration: `crates/nats-client/src/client.rs:91-104`
- Range vs hash partition benchmarks: `siphon_p_ci_builds` (100M rows, ClickHouse Cloud dev instance, 2026-05-08)
