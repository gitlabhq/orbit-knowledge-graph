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

The code indexer re-indexes a repository after a push to the default branch of an Orbit-enabled project.
Before this decision, the indexer read raw push event payloads from the Siphon change data capture (CDC) stream.
On GitLab.com, the push event table receives roughly 40 inserts per second.
Fewer than 1% of those pushes are for Orbit-enabled projects.
For each event, the indexer made a Rails API call and a ClickHouse query, and then dropped almost every event.

The indexer needs a filtered stream of the push events that matter to Orbit.
Only Rails can filter the stream, because Rails owns the enabled namespace records and the branch configuration.

## Decision

**Rails writes a task row to a dedicated PostgreSQL table for each push to the default branch of an Orbit-enabled project.**

Siphon replicates the table to ClickHouse through NATS, and the indexer consumes the NATS subject for that table.

The table uses the GitLab time-range partitioning automation, with daily partitions and a short retention.
Each row holds the project ID, the branch name, the commit SHA, and the traversal path.
The rows are transient work items, and the indexer does not need a row after it consumes it.

On a push to the default branch, Rails checks the Orbit feature flag and the enabled namespace record of the root namespace.
If both checks pass, Rails enqueues a low-urgency Sidekiq worker that writes the task row.
The worker is idempotent and has a concurrency cap, and it defers when the database reports health problems.

The task row already holds the traversal path, the branch, and the commit SHA.
The indexer goes straight to the checkpoint check, the lock, and indexing.
The indexer no longer does these steps:

- Call the Rails API to check the default branch
- Query ClickHouse for the traversal path of the project
- Filter out other actions and other ref types

## Benefits of this approach

The indexer processes only the events that matter to Orbit, instead of filtering out 99% of the stream.
Each event saves two network round trips: the Rails API call and the ClickHouse query.
The PostgreSQL partitioning automation removes old rows, so we do not manage a time to live (TTL).
The flow matches the rest of the Siphon CDC pipeline: Rails writes to PostgreSQL, Siphon replicates through NATS, and Orbit consumes from NATS.
Zoekt and other teams that need filtered push events can use the same pattern.

## Consequences

The cost is a new table and model in the Rails monolith.
A change to the task schema needs a Rails migration.
Indexing depends on the Rails worker, so a failed or late worker delays indexing.
The worker is idempotent and Sidekiq retries it, which reduces this risk.
Rails and the indexer must agree on the table schema.
Siphon replication fails on a column mismatch, so schema drift does not go unnoticed.

## Why not the alternatives

### Replicate the push event table and filter with a materialized view

The push event table receives roughly 40 inserts per second.
Replication would ingest hundreds of millions of rows to drop 99% of them.
Orbit has no other use for this table.
Siphon logical replication cannot filter rows, so the full volume would land in ClickHouse before any filter runs.

### Write directly from Rails to ClickHouse

Rails could insert rows into a ClickHouse table with asynchronous inserts.
This adds one more direct connection between the monolith and ClickHouse to maintain.
The Siphon CDC pipeline already replicates PostgreSQL to ClickHouse, so a second path has no benefit.

### Send events from Rails to Orbit through gRPC

Rails could call the Orbit web server on each push, and Orbit would put the event into NATS.
This approach needs no table.
The cost is per-event overhead in Rails for signing, connection management, and flow control when Orbit is slow.
Batching reduces the overhead, but it adds complexity to Rails for a small gain over the table.

### Use the Data Ingestion Pipeline with OpenTelemetry-like events

The GitLab Data Ingestion Pipeline (DIP) accepts single events over HTTP, buffers them in NATS, and exports batches to ClickHouse.
DIP was not available for this use case at the time of the decision.
If DIP becomes generally available and supports custom event schemas, it could replace the Siphon replication path.

### Publish to NATS directly from Rails

Rails could publish events to NATS and skip PostgreSQL and Siphon.
Orbit and Zoekt could then consume from NATS directly.
Rails has no event-driven architecture today.
A design for flow triggers and events exists, but we do not expect it to land soon.

## References

- [Add a code indexing task on push to the default branch](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/227200)
- [Consume code indexing tasks instead of push events](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/556)
- [Code indexing design document](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/indexing/code_indexing.md)
- [GitLab time-range partitioning documentation](https://docs.gitlab.com/development/database/partitioning/date_range/#using-time-range-partitioning)
- [Add flow triggers and event design document](https://gitlab.com/gitlab-com/content-sites/handbook/-/merge_requests/18106#note_3096730268)
