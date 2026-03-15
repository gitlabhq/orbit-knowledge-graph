---
title: "GKG ADR 005: PostgreSQL task table for code indexing triggers"
creation-date: "2026-03-14"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-03-14

## Context

The code indexer needs to know when code is pushed to the default branch of a Knowledge Graph-enabled project so it can re-index the repository. The previous approach consumed raw `push_event_payloads` from the Siphon CDC stream. That table receives roughly 40 inserts per second on `.com`, but fewer than 1% of those pushes are for KG-enabled projects. The indexer made a Rails API call and a ClickHouse query per event, only to throw away almost everything.

The problem: the indexer needs a filtered stream of KG-relevant push events, but only Rails can do the filtering because Rails owns the `EnabledNamespace` records and branch configuration.

## Decision

Rails writes a record to a dedicated PostgreSQL table (`p_knowledge_graph_code_indexing_tasks`) whenever a push lands on the default branch of a KG-enabled project. Siphon replicates this table to ClickHouse via NATS, and the GKG indexer consumes the NATS subject for that table.

### The table

The Rails table is daily-partitioned using the GitLab [time-range partitioning automation](https://docs.gitlab.com/development/database/partitioning/date_range/#using-time-range-partitioning) with 1-day retention. Each row carries `project_id`, `ref` (branch name), `commit_sha`, and `traversal_path`. These are transient work items: once consumed by the indexer, they are no longer needed.

### The Rails-side flow

In `EE::Git::BranchPushService`, when a push lands on the default branch:

1. Check whether the `knowledge_graph_infra` feature flag is enabled.
2. Check whether the project's root namespace has a `KnowledgeGraph::EnabledNamespace` record.
3. If both conditions are met, enqueue a low-urgency Sidekiq worker (`CodeIndexingWorker`).
4. The worker creates a `CodeIndexingTask` record with the project ID, ref, commit SHA, and traversal path.

The worker is idempotent and concurrency-capped at 100. It defers on database health signals.

### The indexer-side flow

The indexer subscribes to the NATS subject for `p_knowledge_graph_code_indexing_tasks` (replacing the previous `push_event_payloads` subscription). Because the task record already contains the traversal path, ref, and commit SHA, the handler no longer needs to:

- Call the Rails API to check the default branch.
- Query ClickHouse for the project's traversal path.
- Filter out non-push actions or non-branch ref types.

The event already contains everything the handler needs, so it goes straight to checkpoint check, lock acquisition, and indexing.

## Why not the alternatives

### Replicate `push_event_payloads` and filter with a materialized view

The `push_event_payloads` table receives roughly 40 inserts per second. Replicating it would mean ingesting hundreds of millions of rows at a high ingestion rate just to throw away 99% of them. GKG has no other use for this table. Siphon's logical replication cannot filter at the replication level, so the full volume would land in ClickHouse before any filtering could happen.

### Direct write from Rails to ClickHouse

Rails could insert directly into a ClickHouse table using `async_insert=1`. This adds another direct connection between the monolith and ClickHouse, which is one more integration surface to maintain. The Siphon CDC pipeline already handles PostgreSQL-to-ClickHouse replication, so there is no reason to build a second path.

### Rails sends events to GKG via gRPC

Rails could call the GKG webserver directly on each push, and GKG would put the event into NATS. No table needed. The concern is per-event overhead on the sender side: Rails has to handle signing, connection management, and backpressure from GKG. Batching would help but adds complexity to Rails for marginal benefit over the table approach.

### Data Ingestion Pipeline (DIP) with OTEL-like events

The GitLab DIP supports single-event HTTP ingestion with NATS as a buffer and batched export to ClickHouse. DIP was not yet available for this use case at the time of the decision. If DIP becomes generally available and supports custom event schemas, it could replace the Siphon replication path.

### NATS direct from Rails

Rails could publish events directly to NATS, bypassing both PostgreSQL and Siphon. Both GKG and Zoekt could then consume from NATS directly. But Rails has no event-driven architecture today. An [ADR for flow triggers and event design](https://gitlab.com/gitlab-com/content-sites/handbook/-/merge_requests/18106#note_3096730268) exists but is not expected to land in the near term.

## Consequences

**What improves:**

- The indexer processes only KG-relevant events instead of filtering 99% of the NATS stream.
- Two network round-trips per event are gone: the Rails API call for default branch validation and the ClickHouse query for traversal path.
- PostgreSQL partitioning automation handles cleanup. No manual TTL management.
- Same pattern as the rest of the Siphon CDC pipeline: Rails writes to PostgreSQL, Siphon replicates to ClickHouse via NATS, GKG consumes from NATS.
- Zoekt and other teams that need filtered push events can follow the same pattern.

**What gets harder:**

- A new table and model in the Rails monolith must be maintained. Changes to the task schema require a Rails migration.
- The indexer now depends on the Rails worker executing successfully. If the worker fails or is delayed, indexing is delayed. The Sidekiq worker's idempotency and retry semantics mitigate this.
- Two systems (Rails and the indexer) must agree on the table schema. Schema drift is caught by Siphon's CDC replication, which fails on column mismatches.

## References

- [Rails MR !227200: Add Knowledge Graph code indexing task on push to default branch](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/227200)
- [GKG MR !556: Consume code indexing tasks instead of push events](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/556)
- [Code indexing design document](../indexing/code_indexing.md)
- [GitLab time-range partitioning documentation](https://docs.gitlab.com/development/database/partitioning/date_range/#using-time-range-partitioning)
- [Flow triggers and event design ADR](https://gitlab.com/gitlab-com/content-sites/handbook/-/merge_requests/18106#note_3096730268)
