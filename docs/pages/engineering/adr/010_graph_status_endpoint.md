---
title: "GKG ADR 010: Evolving GetGraphStats into GetGraphStatus"
creation-date: "2026-04-21"
authors: [ "@jgdoyon1" ]
toc_hide: true
---

## Status

Accepted

## Date

2026-04-21 (state semantics updated 2026-08-10 and 2026-09-26, see the amendments under Decision)

## Context

The graph stats RPC returns entity counts per ontology domain for one traversal path.
The Rails UI and GitLab Duo use it to see what data the graph holds for a group or project.

Entity counts alone do not answer the questions that users ask:

- Is my project indexed yet?
- When did indexing last run?
- How many projects in my group have code indexing?

The indexer checkpoint tables hold this information, but no RPC exposed it.

The original RPC had four limits:

- It took a raw traversal path and did not know whether the scope was a group or a project.
- It ran one count query per node table and grouped the results by domain.
- Its plain counts were too high on tables that keep several row versions until a background merge.
- It returned no indexing progress.

The Orbit gRPC contract is the source of truth for the response schema.
The Rails REST endpoint is a thin proxy that forwards to Orbit.
When the Orbit response shape changes, Rails adjusts its mapping.

## Decision

**We evolve the graph stats RPC into one graph status RPC that returns entity counts and indexing progress together.**

We delivered the change in four phases, one merge request each.

### Original design

Rails sends the scope type, group or project, with the traversal path.
Rails already knows the scope from the request, so Orbit does not spend ClickHouse queries to find it.

Orbit counts every node type under the traversal path with a prefix match, so a parent group includes its subgroups.
Orbit counts each type with an approximate distinct count of IDs.
That count reads a small per-table aggregate projection and does not scan the namespace.
It also removes the duplicate row versions that a plain count includes.
At the time of the decision, plain counts were up to 300% too high for types that change often.
The approximate count has an error of one to two percent, which is acceptable for a status indicator.
On production, every type except Group measured in about one percent of the exact count.

Group uses an exact deduplicated count.
Namespace deletion leaves deleted group rows with distinct IDs, and the projection cannot exclude them.
On a large namespace, the approximate group count was about six times too high.
The group table is small, so the exact count is cheap.

Entity counts respect the caller's access level.
Each ontology node can declare a minimum role, and the JSON Web Token (JWT) tags each traversal path with the caller's access level.
The query compiler already drops paths where the caller's role is below the minimum.
The status RPC uses the same check and leaves out any entity type that the caller cannot see at any path.
A user with only the Reporter role sees zero vulnerabilities in query results and in the status counts.

The response also reports project coverage, the number of projects with code indexing next to the number of known projects.
Both counts come from ClickHouse and skip deleted rows.
Namespace deletion soft-deletes the code checkpoint rows.
The entity counts and the project counts run concurrently.

The indexers write indexing progress to a NATS key-value bucket after each run.
Software development lifecycle (SDLC) progress has one key per top-level namespace, and code progress has one key per project.
Each value holds the start time, completion time, duration, and error of the last run.
A non-empty error means that the last run failed.
These reads are constant-time lookups and need no ClickHouse query.
After a schema migration, the previous entry stays until the reindex completes, because it is still correct for the old schema.

The response is flat and has the same fields for every scope.
Indexing metadata comes first, then project coverage, then entity counts by domain.
A subgroup gets indexing metadata from its top-level group, because SDLC indexing runs at the top-level namespace.
Its entity counts and project coverage stay scoped to the subgroup.
A project gets indexing metadata from its own code progress entry.

The last phase caches the full response in a NATS key-value bucket per traversal path with a short time to live (TTL).
A cache hit returns without a ClickHouse query.
The indexer invalidates the entry after each run, so consumers see new data when indexing completes.
The TTL covers bursts of calls between runs.

### Rails endpoint

Rails exposes the RPC through a REST endpoint that the Rails UI and GitLab Duo call.
The caller names the scope with exactly one of a group ID, a project ID, or a full path.
Rails resolves the parameter to a group and checks that the caller can read it.
Rails then derives the traversal path and the scope type.
For a project, the group is the parent namespace, and the scope type is project only when the caller passed a project ID.
Rails builds a JWT with the caller's access level on each path and forwards the request.
Rails maps the gRPC response to JSON.

Authorization has three levels:

- The route requires the Orbit read permission at the user boundary.
- Rails checks group read access before it calls Orbit.
- Orbit filters entity types by role, so Rails does not need to know which types need a higher role.

| Condition | HTTP status |
|---|---|
| No lookup parameter | 400 |
| Namespace not found or not accessible | 404 |
| Orbit unreachable | 503 |
| Orbit authorization error | 403 |

### Amendment: indexing state per surface (2026-08-10)

The single indexing state came only from run timestamps in the key-value store.
A run that completed without writes, for example on an empty data lake or after an aborted extract, still reported indexed.

The response now reports an SDLC state and a code state, and the combined state is the worse of the two:

- The SDLC state is the worst state across the progress of every namespaced pipeline, including node, edge, and derived pipelines.
- The code state comes from project coverage.
  With no projects indexed, it is not indexed, with some it is backfilling, and with all it is indexed.
- The code state is absent when the scope has no known projects.
- Each domain item has an optional state from its own pipeline or from code coverage.

A namespace without code indexing no longer reports plain indexed.
The wire shape and the state values did not change, so Rails needed no update.

The progress entry also records the rows read and written in the last run.
These counts help operators explain an empty graph.
They never drive the state, because an idle incremental run reads and writes nothing.

### Amendment: state from checkpoints (2026-09-26)

The RPC no longer reads the key-value progress store.
It presents the output of two server services, one for indexing status and one for item counts.
Both services take many scopes in one call.

The indexing status service reads the checkpoint tables of the served schema version:

- A plan is ready when its checkpoint for the root has a completion time.
- A plan with a checkpoint but no completion time is syncing.
- A plan with no checkpoint has not started.

The read takes the newest completion time after the last deletion marker, not the deduplicated row.
A background merge keeps only the newest row, so a late page write from an overlapping run could hide a completion.
In that case, the plan reads as syncing until the overlapping run completes.

A domain combines the plans that feed it and, for code entities, the project coverage.
A scope combines its domains.
Ready maps to indexed, syncing maps to backfilling, and not started maps to not indexed.
Some earlier progress fields are no longer set.

The item counts service counts each entity only under scopes where the caller holds the required role.
Checkpoint and coverage reads skip the ClickHouse query cache, so a phase change shows on the next call.

### Amendment: indexing status and item count RPCs (2026-09-27)

Two new RPCs expose the two services directly for the Rails status pages.
The indexing status RPC takes a capped list of traversal paths, and the item counts RPC takes one.
A path can be a top-level group, a subgroup, or a project.
The caller must have access to each path, and an administrator can ask for any path.

The indexing status RPC returns one phase per path and per domain: unknown, not started, syncing, ready, or error.
A project path reports the SDLC phases of its root, and its source code domain reports the project coverage under the path.
A plan or project that uses all its attempts, or that stops without a retry, is a gap.
A path or domain is in error when all its parts are settled and at least one is a gap.

The item counts RPC returns the entity counts per domain that the caller can see, and the graph status RPC counts the same way.
Rails rate-limits both callers, the GraphQL field per user and the REST endpoint per user and per group or project.
The responses send only the domain name, and Rails owns the display text.

### Amendment: last completed time (2026-10-11)

The indexing status RPC returns a last completed time per path, and the graph status RPC reports it again.
The value is the oldest completion time across the SDLC plans of the root, because each successful run sets that time.
A plan that stops completing keeps the value from moving.
The value is absent until each plan has completed once.

### Amendment: cached item counts (2026-10-16)

Item counts now use the ClickHouse query cache with its default short TTL.
The count queries were the largest share of ClickHouse time and bytes read.
A count can be as old as the TTL, even after a backfill ends.
Checkpoint and coverage reads still skip the cache, so a phase change shows on the next call.

## Benefits of this approach

One RPC answers what data exists and how fresh it is.
Operators get the coverage ratio of indexed projects to known projects.
Distinct counts remove the excess from duplicate row versions.
The response cache protects ClickHouse from repeated calls.

## Consequences

Each request runs several queries and lookups instead of one.
A subgroup request must resolve its top-level group for SDLC indexing metadata.

## Why not the alternatives

### A separate indexing status RPC

Consumers would have to call two RPCs and correlate them.
Low entity counts could mean that indexing has not run yet, or that the namespace is empty.
One RPC removes that ambiguity.
The 2026-09-27 amendment later added separate RPCs for the Rails status pages, and the graph status RPC still combines both.

### Exact deduplicated counts for every type

Query-time deduplication is expensive.
On staging, with 16 million edge rows, it read 14.4 million rows and 620 MB in 579 ms, against 71 ms without it.
At production scale it would take minutes.
The approximate distinct count removes duplicates at a fraction of that cost.

### Precomputed counts at index time

The indexer could write entity counts to the key-value store with the progress entry, so the full response is one lookup.
We rejected this because it adds work to the indexer for an endpoint with low traffic.
If traffic grows, this is the next step after the response cache.

## References

- [Code indexing design document](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/indexing/code_indexing.md)
- [SDLC indexing design document](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/indexing/sdlc_indexing.md)
- [ADR 005: PostgreSQL task table for code indexing triggers](005_code_indexing_task_table.md)
- [Security and authorization design](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/blob/2da7a9340bb4ae77d07050e0b01f50cf507af66c/docs/design-documents/security.md)
- [Enforce the per-entity role on aggregation target nodes](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/987)
- [Add the graph status REST endpoint to Rails](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/231381)
- [Fix silent wrong answers before we charge per query](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/issues/1137)
