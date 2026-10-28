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

The code indexer indexes a repository again after a push to the default branch of a project that has Orbit turned on.
Before this decision, it read every push event from Siphon, the change data capture (CDC) pipeline that copies PostgreSQL changes to ClickHouse through NATS.
On GitLab.com, that is roughly 40 pushes per second, and fewer than 1% of them are for Orbit projects.
For each event, the indexer made a Rails API call and a ClickHouse query, and then dropped almost every event.

The indexer needs only the push events that matter to Orbit.
Only Rails can pick them out, because Rails knows which namespaces have Orbit turned on and which branch is the default.

## Decision

**Rails writes a task row to a new PostgreSQL table for each push to the default branch of an Orbit project.**

Siphon copies the table through NATS, and the indexer reads the NATS stream for that table.

The table uses the GitLab time-range partitioning, with daily partitions that are dropped after a short time.
Each row holds the project ID, the branch name, the commit SHA, and the traversal path, which is the group hierarchy of the project.
The rows are short-lived work items, and the indexer does not need a row after it reads it.

On a push to the default branch, Rails checks the Orbit feature flag and whether the top-level namespace has Orbit turned on.
If both checks pass, Rails queues a low-urgency Sidekiq job that writes the task row.
The job is safe to run twice and has a concurrency limit, and it waits when the database is unhealthy.

Because the task row already holds the traversal path, the branch, and the commit SHA, the indexer can go straight to indexing.
The indexer no longer does these steps:

- Call the Rails API to check the default branch
- Query ClickHouse for the traversal path of the project
- Filter out other actions and other ref types

## Benefits of this approach

The indexer reads only the events that matter to Orbit, instead of dropping 99% of the stream.
Each event saves a Rails API call and a ClickHouse query.
The PostgreSQL partitioning removes old rows, so we do not manage an expiry time.
The flow matches the rest of the Siphon pipeline, where Rails writes to PostgreSQL, Siphon copies through NATS, and Orbit reads from NATS.
Zoekt and other teams that need filtered push events can use the same pattern.

## Consequences

The cost is a new table and model in the Rails monolith, and a change to the task schema needs a Rails migration.
Indexing depends on the Rails job, so a failed or late job delays indexing.
Sidekiq retries the job, and a second run is harmless, which reduces this risk.
Rails and the indexer must agree on the table schema.
Siphon fails when the columns do not match, so we notice schema drift.

## Why not the alternatives

### Replicate the push event table and filter with a materialized view

Siphon cannot filter rows, so every push event would land in ClickHouse before any filter runs.
That is hundreds of millions of rows, and we would drop 99% of them.
Orbit has no other use for this table.

### Write directly from Rails to ClickHouse

Rails could insert rows into a ClickHouse table directly.
That adds one more connection between the monolith and ClickHouse to maintain.
Siphon already copies PostgreSQL to ClickHouse, so a second path has no benefit.

### Send events from Rails to Orbit through gRPC

Rails could call Orbit on each push, and Orbit would put the event into NATS.
This needs no table, but Rails pays for each event in token signing, connections, and backoff when Orbit is slow.
Batching reduces that cost, but it adds complexity to Rails for a small gain over the table.

### Use the Data Ingestion Pipeline with OpenTelemetry-like events

The GitLab Data Ingestion Pipeline (DIP) accepts single events over HTTP, buffers them in NATS, and exports batches to ClickHouse.
DIP was not available for this use case at the time of the decision.
If DIP becomes generally available and supports custom event schemas, it could replace the Siphon replication path.

### Publish to NATS directly from Rails

Rails could publish events to NATS and skip PostgreSQL and Siphon, and Orbit and Zoekt could read them from NATS.
But Rails has no event system today, and we do not expect the design for flow triggers and events to land soon.

## References

- [Add a code indexing task on push to the default branch](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/227200)
- [Consume code indexing tasks instead of push events](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/556)
- [Code indexing design document](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/indexing/code_indexing.md)
- [GitLab time-range partitioning documentation](https://docs.gitlab.com/development/database/partitioning/date_range/#using-time-range-partitioning)
- [Add flow triggers and event design document](https://gitlab.com/gitlab-com/content-sites/handbook/-/merge_requests/18106#note_3096730268)
