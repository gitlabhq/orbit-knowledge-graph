---
title: "GKG ADR 019: Job ledger"
creation-date: "2026-09-21"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-09-21

## Context

`graph_status` must report `INDEXED` when the initial backfill of a namespace is
complete, and a rolling `last_completed_at` after that
([#1281](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1281)).

Today "a job is done" is computed in five places, each with its own SQL:

| Place | Method |
| --- | --- |
| `code_backfill.rs` | checkpoint query per project |
| `orbit_migrations::completion` | checkpoint key split on dots |
| `graph_status/code.rs` | count of checkpointed projects |
| `migration_completion.rs` | same count, for metrics |
| backfill sweep | `projects.is_empty()` in memory, one tick |

Run status lives in the NATS KV bucket `orbit_indexing_progress_<schema version>`.
The bucket is deleted with its schema version. The code pipeline writes path keys.
The SDLC pipeline writes entity keys. `graph_status` reads only entity keys.

Draft MR [!2450](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/2450)
derived a sticky per-namespace flag from checkpoints. This decision supersedes it.

## Decision

Jobs and campaigns are first-class types with one durable store: two unversioned
ClickHouse tables on the graph cluster, owned by the `jobs` crate.

### Model

| Type | Meaning |
| --- | --- |
| `JobKind`, `CampaignKind` | Opaque names, `^[a-z][a-z0-9_]{0,63}$`, validated at compile time. |
| `CampaignId` | `kind`, `subject`, `generation`. Subject and generation are opaque to the ledger. |
| `PhaseSpec` | One job kind plus `required`. A campaign is a list of phases. |
| `JobRef` | Optional campaign, `namespace_id`, `traversal_path`, `kind`, opaque `key`. |
| `JobState` | `pending, queued, running, retrying, deferred, failed, skipped, succeeded`. The only closed enum. |
| `JobTransition` | `JobRef`, `dispatch_id`, `attempt`, `state`, optional `reason`, `recorded_at`. |

The ledger never matches on a kind. Integrations own their constants:

| Owner | Constants |
| --- | --- |
| `indexer::modules::code::jobs` | `CODE`; key = project id |
| `indexer::modules::sdlc::jobs` | `NAMESPACE_DATA`; key = plan name |
| `indexer::orchestrator::backfill::campaign` | `INITIAL_BACKFILL`; both kinds required |
| `orbit-migrations` (later) | `SCHEMA_MIGRATION`; `NAMESPACE_DATA` required, `CODE` optional |

Ordinary runs are jobs with no campaign. This gives the rolling timestamp and
removes the KV progress bucket.

### Tables

`campaign` holds one row per phase. Sort key `(kind, subject, generation, job_kind)`.

`job` holds one row per (job, dispatch, attempt). The sort key starts with the
campaign columns, then `namespace_id, kind, key, dispatch_id, attempt`. Projection
`by_namespace` orders by `(namespace_id, kind, key, recorded_at)`. Rows with no
campaign expire after 30 days.

Both tables use `ReplacingMergeTree(_version, _deleted)` with `_version` = state
rank. A late or duplicate write cannot lower a state. `_deleted` is reserved.

| Job rank | 0 | 1 | 2 | 3 | 4 | 5 | 6 | 7 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| state | pending | queued | running | retrying | deferred | failed | skipped | succeeded |

Phase ranks: `open` 1, `discovery_closed` 2, `abandoned` 3.

### Derived, never stored

- Current row of a job: within a dispatch the highest attempt; across dispatches
  the most recent `recorded_at`.
- Phase complete: discovery closed and no job in `pending, queued, running,
  retrying, deferred`.
- Campaign complete: every phase complete. Ready: every required phase complete.
  Abandoned: any phase abandoned.
- `failed` is terminal. The summary exposes the failed count.

### Writes

Workers record outcomes at their durable commit points. Every write is one Arrow
batch with `async_insert = 1` and `wait_for_async_insert = 1`. Registration is
idempotent. Errors are errors; no store failure maps to a job state.

### Policies

| Situation | Behavior |
| --- | --- |
| Projects arrive during discovery | Registered in the open campaign. |
| Projects arrive after discovery closed | Campaign-less jobs. Readiness stays complete. |
| Namespace disabled | Campaign abandoned. Status `UNKNOWN`. |
| Namespace re-enabled | New generation. No prior evidence reused. |
| Redelivery of a completed job | Higher rank wins. Totals do not inflate. |
| Late row from an older attempt | Cannot change the newer attempt. |

### Schema evolution

Unversioned tables are created with `CREATE TABLE IF NOT EXISTS`. Boot also runs
`ALTER TABLE ... ADD COLUMN IF NOT EXISTS` for every declared column. Adding a
column is safe. Renames and type changes need a new table.

An ontology change still bumps `SCHEMA_VERSION`, because the ontology archive must
match the sources byte for byte. The ledger entry is `scope: none`, so the migration
clones every table and re-indexes nothing.

## Why not the alternatives

| Alternative | Rejected because |
| --- | --- |
| Sticky flag derived from checkpoints (!2450) | A sixth copy of "done". Cannot roll back, cannot list, cannot explain. |
| NATS KV ledger | No filters, no joins, no multi-key write. Buckets are deleted with the schema version. Needed a seal protocol to keep the denominator honest. |
| Central job-kind enum | Every new kind changes the ledger crate. |
| Trait plus repository layer | Adds a layer that forwards arguments. One concrete `JobLedger` is enough. |

Measured 2026-09-20 in production: SDLC completions about 15 per second, code
outcomes about 0.3 per second, 1.06 M code checkpoints. Ledger volume is about
1.3 M rows per day plus 2 M rows once per initial backfill. The graph cluster
already takes checkpoint writes at a higher rate.

## Consequences

- The ledger outlives schema promotion. Status does not reset on migration.
- `graph_status` becomes one read of the ledger. The checkpoint scans are deleted.
- The KV progress bucket and `indexing_status.rs` are deleted.
- A visualizer and admin retry can read `JobLedger::jobs`. Not in this series.

Delivery: MR1 crate and tables. MR2 dispatcher opens campaigns and registers jobs.
MR3 workers record outcomes. MR4 `graph_status` reads the ledger. MR5 schema
migration as a campaign.

## Non-goals

Retry or cancel controls, an operator CLI, a workflow engine.

## References

- [Issue #1281](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1281)
- [Superseded draft MR !2450](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/2450)
- [ADR 010: Graph status endpoint](010_graph_status_endpoint.md)
- [ADR 014: Entity-level SDLC indexing](014_entity_level_indexing.md)
- [ADR 017: Clone-based non-blocking schema migrations](017_clone_based_non_blocking_migrations.md)
- [Schema management](../schema_management.md)
