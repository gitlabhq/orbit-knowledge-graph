---
title: "GKG ADR 010: Evolving GetGraphStats into GetGraphStatus"
creation-date: "2026-04-21"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-04-21 (initial-backfill semantics updated 2026-09-09)

## Context

Entity counts answer what exists in the graph, but not whether initial indexing has
completed. `GetGraphStatus` combines current counts with an initial-backfill summary.
It does not certify that Siphon's CDC has caught up or that the graph is fresh relative
to GitLab. The response contract lives in
[`orbit.proto`](../../../crates/orbit-server/proto/orbit.proto).

## Decision

Extend the existing `IndexingStatusStore` in the unversioned `orbit_indexing_progress`
NATS KV bucket with one snapshot per root namespace, keyed `backfill.<root_namespace_id>`.
The dispatcher owns first completion, using target-schema checkpoints and the replicated
project inventory. The webserver reads the snapshot; it never reads source tables or
computes completion. See the [snapshot store](../../../crates/indexer/src/indexing_status/backfill.rs),
[dispatcher](../../../crates/indexer/src/orchestrator/dispatch/backfill_status.rs), and
[status service](../../../crates/orbit-server/src/graph_status/mod.rs).

### Response contract

The structured response contains `projects`, `domains`, and one `backfill` object:

```json
{
  "backfill": {
    "state": "running",
    "last_progress_at": "2026-09-09T10:05:00Z",
    "sdlc": { "completed": 12, "total": 20 },
    "code": { "completed": 45 }
  },
  "projects": { "indexed": 45, "total_known": 150 },
  "domains": [
    {
      "name": "core",
      "items": [{ "name": "Project", "count": 150 }]
    }
  ]
}
```

This is an illustrative structured payload; the state names are the semantic labels.
The protobuf defines the corresponding `BACKFILL_STATE_*` enum values. `last_progress_at`
and `error` are optional. SDLC has a total; code has only an informational completed
count, never a total. No generation, schema, or scope IDs are exposed. The
`backfill` object replaces `indexing`, `sdlc_indexing`, and `code_indexing`. Domain items
contain only `name` and `count`, not per-item state. The old protobuf field numbers and
names are reserved. RAW uses the structured response; LLM uses
[TOON formatting](../../../crates/orbit-server/src/graph_status/toon.rs), not a separate
status model. See [`orbit.proto`](../../../crates/orbit-server/proto/orbit.proto).

| State | Meaning |
|---|---|
| `unknown` | The snapshot is missing or unreadable, or the dispatcher recorded unavailable evidence. |
| `running` | Initial backfill has begun but the dispatcher has not recorded completion. |
| `retrying` | A worker recorded an error before completion; this does not establish that a retry is scheduled or running. |
| `completed` | The dispatcher recorded initial SDLC completion and no remaining uncheckpointed projects in the root's replicated inventory. |

`not_started` remains a protobuf enum value but is not emitted by the snapshot reader.
A missing record is unknown, not proof the namespace was never backfilled. Worker
progress clears an earlier error; completed snapshots ignore later errors. Other
response branches can still return data when backfill is unknown, so a successful RPC
alone does not establish completion. See the
[snapshot updates](../../../crates/indexer/src/indexing_status/backfill.rs) and
[response mapping](../../../crates/orbit-server/src/graph_status/mod.rs).

### Scope and counts

The entire `backfill` object, including both counts and progress, is root-namespace
scope. Parent, subgroup, and project requests read the same snapshot. See the
[root key](../../../crates/indexer/src/indexing_status/backfill.rs).

- **SDLC:** `sdlc.total` counts the target ontology's namespaced pipeline descriptors,
  including node, standalone-edge, and derived pipelines. `sdlc.completed` counts
  initial-complete parent checkpoints in the target schema at the root namespace.
  Partitions are not separate units; cross-schema KV attempt markers are not completion
  evidence. See [checkpoint aggregation](../../../crates/indexer/src/orchestrator/dispatch/backfill_status.rs).
- **Code:** `code.completed` counts distinct projects with non-deleted code checkpoints
  in the target schema under the root path. It is informational, not a percentage or
  completion predicate; there is no `code.total`. See
  [checkpoint counting](../../../crates/indexer/src/orchestrator/dispatch/code_backfill.rs).
- **Current graph contents:** `projects` remains requested-scope live project coverage
  and `domains` remains requested-scope live entity counts, even after backfill completes.
  Entity visibility is derived from ontology role requirements and the caller's security
  context. See [project coverage](../../../crates/orbit-server/src/graph_status/code.rs),
  [input selection](../../../crates/orbit-server/src/graph_status/input.rs) and
  [count queries](../../../crates/orbit-server/src/graph_status/mod.rs).

### Dispatcher-owned completion

Enable events and scheduled backfill use the same `CodeBackfill` path. The dispatcher
compares non-deleted projects in the Datalake's `project_namespace_traversal_paths` with
the target schema's code checkpoints. It records completion when initial SDLC is complete
and no replicated projects remain uncheckpointed. Source-read failures do not prove the
inventory is empty. See [shared dispatch](../../../crates/indexer/src/orchestrator/dispatch/code_backfill.rs)
and [trigger wiring](../../../crates/indexer/src/lib.rs).

Completion means **all currently replicated projects are indexed after initial SDLC**.
There is no upstream-inventory readiness dependency and no replication-freshness
guarantee. Projects arriving after completion are ongoing indexing and do not reopen
initial backfill. Polling neither creates nor completes a snapshot. See the
[completion predicate](../../../crates/indexer/src/orchestrator/dispatch/backfill_status.rs),
[snapshot updates](../../../crates/indexer/src/indexing_status/backfill.rs), and
[reader](../../../crates/orbit-server/src/graph_status/mod.rs).

### Progress and checkpoint evidence

Workers advance `last_progress_at` for meaningful work, such as processing a nonempty
SDLC page, repository work, or code checkpoint finalization. Dispatch, attempt start,
idle polling, and NATS liveness notifications alone do not advance it. No state is
inferred from timestamp age. See the
[SDLC pipeline](../../../crates/indexer/src/modules/sdlc/pipeline.rs),
[code pipeline](../../../crates/indexer/src/modules/code/pipeline.rs), and
[snapshot store](../../../crates/indexer/src/indexing_status/backfill.rs).

SDLC completion requires the durable parent checkpoint, including partition
consolidation. A parent with no cursor, or a resume floor from a completed initial pass,
counts as initial-complete. Code checkpoints are finalized after their buffered writes
succeed. Workers report progress and errors, not namespace completion. Existing
checkpoints can establish completion without inventing a progress timestamp. See
[SDLC evidence](../../../crates/indexer/src/orchestrator/dispatch/backfill_status.rs) and
the [code finalizer](../../../crates/indexer/src/modules/code/pipeline.rs).

### Storage and lifecycle

All status records and snapshots use the unversioned indexing-progress bucket:

| Key | Contents |
|---|---|
| `status.{dotted_root_path}.{pipeline_name}` | SDLC attempt metadata, not initial-completion evidence. |
| `status.{dotted_project_path}` | Code attempt metadata. |
| `backfill.<root_namespace_id>` | Root state, progress, counts, error, and internal target schema and generation. |

While incomplete, a snapshot follows the indexing target schema. Dispatcher updates
are guarded by the migration target and snapshot generation; worker progress applies
only to the matching target schema. Changing the target resets an incomplete snapshot
with a new generation. Completed snapshots retain their state, counts, and progress
across later indexing, schema migrations, and rebuilds. This is not a schema-migration
readiness gate. See the [dispatcher guard](../../../crates/indexer/src/orchestrator/dispatch/backfill_status.rs)
and [snapshot updates](../../../crates/indexer/src/indexing_status/backfill.rs).

Before migration, the dispatcher makes a best-effort adoption pass using the active
ontology and checkpoints when available. Missing historical evidence cannot establish
earlier completion. See [startup reconciliation](../../../crates/indexer/src/lib.rs)
and [schema management](../schema_management.md#stable-initial-backfill-status).

Actual root namespace data deletion clears its snapshot and descendant attempt keys
before marking deletion complete. Subgroup deletion clears its attempt keys without
resetting the root snapshot. Disabling indexing during the grace period does not clear
status. See the [cleanup implementation](../../../crates/indexer/src/indexing_status.rs)
and
[namespace deletion](../indexing/namespace_deletion.md).

## Consumers

The Rails `GET /api/v4/orbit/graph_status` proxy, UI consumers, CLI output consumers,
agent prompts, and skills must use `backfill` instead of the removed indexing fields
and per-item states. The proto is the contract; Rails serialization and display changes
must be coordinated rather than assuming the previous response mapping still works.
The CLI entry point is documented in
[remote access](../../source/remote/access/glab.md#check-indexing-progress).

Clients can poll while initial indexing is incomplete. Treat `unknown` as unavailable
evidence, not proof indexing never ran. `completed` is the root's initial backfill
milestone, not certified upstream CDC completion, continuous freshness, or current worker
health. Requested-scope graph counts continue to be queried. See the
[service implementation](../../../crates/orbit-server/src/graph_status/mod.rs).

## References

- [Code indexing design document](../indexing/code_indexing.md)
- [SDLC indexing design document](../indexing/sdlc_indexing.md)
- [Entity-level indexing](014_entity_level_indexing.md#indexing-status-tracking)
- [Security and authorization design](../security.md)
