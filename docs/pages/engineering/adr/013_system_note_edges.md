---
title: "GKG ADR 013: Materialize edges from system notes"
creation-date: "2026-05-18"
authors: [ "@dgruzd" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-05-18

## Context

Rails writes a system note when an issue closes, a merge request (MR) merges, a commit joins an MR, or one entity references another.
Orbit drops these notes before they become edges, because it reads only user notes.
The graph had no mentions between MRs or between MRs and work items, no links from MRs to commits, and no reopen transition.
Merged and closed edges came from foreign keys, but they were sparse on older data.
The graph-completeness epic and a team sync both named this gap.

The work has three difficulties:

- Siphon copies GitLab tables into the ClickHouse data lake, but it did not copy the system note metadata table.
  That table holds the structured action of each note.
- The target of a cross-reference is a GitLab Flavored Markdown reference in the free-text body, with no foreign key.
- The source is large, with about 6.7 million system notes in one large top-level group and a global notes table of about 4 TB.

A previous MR covered only the merged, closed, and reopened actions, with one filtered ClickHouse view per action.
It passed continuous integration (CI), but the author closed it for process reasons, to write this ADR first.
It taught us three things:

- The old edge extract shape had no filter clause, so it needed one view per action.
  That does not scale to more than 10 actions on three noteable types.
  The unified pipeline shape that replaced it has a filter predicate.
- The lifecycle slice adds little, because foreign keys already cover most of it.
  Only reopen is new.
- Nobody filed the Siphon prerequisite, so the MR would have done nothing in production.

Rails owns authorization and writes the note bodies.
We do not control the templates, and we accept Rails phrasing changes as a maintenance cost.
The Analytics team owns Siphon, so the new replication is cross-team work on the critical path.
The schema migration framework was complete and validated on staging, so a new edge kind needs only a schema version bump and no table rebuild.

## Decision

**The software development lifecycle (SDLC) indexer parses system notes in a Rust transform and resolves the targets with batched ClickHouse lookups.**

The transform depends on a one-time Siphon replication of the system note metadata table.
We ask for a skip index on the action and a primary key on the traversal path and note ID.
A proof of concept (POC) harness backs the decision, with results under [Benefits of this approach](#benefits-of-this-approach).

### Scope and edge kinds

The transform parses a subset of the Rails and Enterprise Edition cross-reference actions.
It also parses the commit action, which lists commit SHAs, and the merge action, which has an inline SHA or an MR reference.

| Edge | From and to | Source actions |
|---|---|---|
| MENTIONS | MR, work item, or commit to the same set of types | Cross-reference |
| RELATED_TO, as a supplement | Work item to work item | Add or remove a relation |
| MENTIONS, for hierarchy | Work item to work item | Add or remove a parent or child |
| MENTIONS, for moves | Work item to work item | Moved, cloned, or duplicate |
| ADDS_COMMIT | MR to commit | Commit |
| MERGED_AT_COMMIT | MR to commit | Merge, auto-merge variant |
| MERGED, as a supplement | User to MR | Merged |
| CLOSED, as a supplement | User to MR or work item | Closed |
| REOPENED, a new edge kind | User to MR or work item | Reopened |

The edge kinds stay declared in the ontology, and only the extract, transform, and load (ETL) logic is in Rust.
Each existing edge kind with supplemental writes names the transform as an additional emitter.

We leave these out of scope:

- User mention edges, which come from separate tables with their own Siphon replication
- Resource state, label, and milestone events, which have their own event tables
- Rendered HTML, because the parser matches plain text only
- External issue references, such as Jira
- The Siphon replication MR, which the Analytics team owns
- A link type property on mention edges, which the implementation MR settles, with a default of yes

### Transform design

Under [ADR 014](014_entity_level_indexing.md), each entity kind has its own handler, message subject, and checkpoint.
[ADR 015](015_pluggable_entity_pipelines.md) then made the transform stage the extension point.
The system note entity is a derived ontology entity whose pipeline names the Rust transform.
The shared pipeline owns extraction, paging, checkpoints, and writes.
The extract template owns the join of notes to metadata, the page bounds, and the runtime filters.

We did not use the built-in projection transform:

- One source table feeds about nine edge variants, and body parsing decides the target type.
- One view per action and noteable type would add about 30 redundant projections, and Siphon guidance prefers skip indexes.

```plaintext
for each page of system notes in the namespace:
  keep rows whose action is a target action
  parse each body into references
  look up the routes for the referenced project paths
  look up the MR and work item IDs for each project and internal ID pair
  write one edge per resolved reference
```

The deduplication key is the metadata ID, the edge kind, the source, and the target.
The ClickHouse table engine keeps one row for each key, so a second pass writes nothing new.
The project and group ontology nodes already use the same route lookup, in the other direction.

An unqualified reference, such as a bare MR number or a short SHA, takes its scope from the project of the noteable.
The transform does not call Gitaly to validate commit SHAs.
ClickHouse HTTP parameters go into the request URL, so the resolver splits each bound list into chunks of a configured size.

### Metadata mode and fallback mode

Both modes read only system notes that are not deleted.

In metadata mode, the extract joins notes to the metadata table on the note ID and filters on the target actions.
The join is necessary, because the body, the noteable, and the author are on the note row.
Production uses this mode.

In fallback mode, the metadata table is not available, and the transform dispatches on the body.
Lifecycle actions match the exact body text.
Cross-reference actions match an anchored pattern at the start of the body.
The system note filter keeps that pattern away from user notes with the same words.
Fallback mode is slower and exists only for staging benchmarks and demos while the Siphon change is in flight.
Configuration selects the mode.

### Action drift and rollout

We vendor the Rails list of system note actions, pinned to an upstream commit.
The list grew across releases, and Rails has changed system note phrasing more than once.
A drift check, modeled on the one in [ADR 012](012_goon_format.md), compares the copy to upstream.
It lists the missing values and runs in the pre-commit hook and the lint stage.
At runtime, the transform logs and drops an unknown action, and it does not crash.

A new metric counts unknown actions by action, and the size of the list bounds its cardinality.
The transform reuses the SDLC pipeline metrics and adds one general metric for edges written per edge kind.

The transform ships behind a configuration flag that is off by default and on in staging first.
The flag lives in the existing per-entity handler configuration, so a change needs no code change.
Staging benchmarks are the gate to general availability.
The implementation MR does not turn the flag on until the two staging benchmarks have results.

### Amendment: containment comes from parent links (2026-06-17)

The note path also wrote work item containment from four containment actions.
The parent link table writes the same edges and is the source of truth for current hierarchy.
Both writers shared one row identity in the table engine, which keeps the newest version of a row.
The note row carried a wall-clock version and was never marked deleted.
When a child left its parent, the note row outranked the deletion and brought the containment back.
We removed the containment actions from the note path, which stops new writes.
Old rows clear on the next reindex without a schema version bump, which the maintainer accepted in staging.

### Amendment: reopen comes from resource state events (2026-06-22)

The design assumed that a reopen is a system note action.
That premise was wrong, because Rails records a reopen as a resource state event.
The reopen branch of the note path matched nothing and never wrote an edge.
Reopen edges now come from two generated edge pipelines over the replicated resource state events, one for MRs and one for work items.
Each filters on the reopened state in its extract.
Merged and closed edges stay note-derived.
We defer a reconciliation of those edges with resource state events, which must settle current-state against event-history semantics and handle deduplication and deleted rows.

### Amendment: mention edge direction (2026-06-25)

The first emitter used the noteable as the source and the parsed reference as the target.
The noteable receives the note, so the edge pointed from the mentioned entity to the entity that mentions it.
For one-sided actions, every edge pointed the wrong way.
For symmetric actions, Rails writes notes on both sides, so the graph had both directions, but each row was wrong.

The parsed reference is now the source and the noteable the target.
The edge takes the traversal path of the noteable, so inbound queries on the target read the correct partition.
The fix changes the row identity, so an in-place reindex would keep the inverted rows.
We bumped the schema version, so a migration reindexes into new tables.

## Benefits of this approach

The design closes the edge gap between MRs, work items, and commits.
A new Rails cross-reference action needs only a parser change, with no ontology, fixture, or ETL change.
A slow system note pass does not redeliver messages for other entities in the namespace.

The POC measured parse speed on one CPU core with a release build:

| Corpus | Notes | Median time per note | Median notes per second |
|---|---:|---:|---:|
| Synthetic golden corpus | 21 | 666 nanoseconds | 1,501,501 |
| Seeded GitLab Development Kit (GDK) corpus | 75,125 | 575 nanoseconds | 1,739,130 |

The real corpus parsed about 16% faster, because about 40% of its notes are lifecycle actions that skip pattern work.
Over 100 iterations, it had no panics, wrong parses, or false positives across 16 action types.
The transform dropped the seeded unknown actions with a warning.
Both rates are about three times the pass criterion of 500,000 notes per second.
For 6.7 million notes, parsing takes about four seconds on one core.

On a small local GDK instance, three resolver queries took 8 ms for a batch of 1,000 and 15 ms for a batch of 5,000.
The budget for each batch is 50 ms.
Parse and resolve together ran at about 125,000 notes per second on one core.
Few references resolved, because the seed data references IDs that do not exist, and those produced no rows.

These runs validate the query plan but not production scale.
Two benchmarks need staging ClickHouse and the Siphon replication:

- A full pass over the large top-level group in 10 minutes or less.
  Compute alone extrapolates to about 54 seconds, so ClickHouse scan time takes most of the budget.
- An edge density gain of at least three times for MR-to-work-item edges and 10 times for MR-to-MR edges.
  We propose these numbers for the upstream acceptance criterion.

The POC numbers are a regression baseline.

## Consequences

We maintain a new Rust module, and template drift is an ongoing cost.
Siphon must replicate the metadata table, and probably add a skip index on the route path.
The transform departs from the ontology-first convention, as only the code indexer and namespace deletion did before.
We confine the departure to edge logic and record the reason in the code and the agent instructions.
Fallback mode is dead code after the replication lands.
If the replication slips, the transform ships in fallback mode, and the switch is one configuration change.
The parser anchors on the reference instead of the verb phrase, so a phrasing change does not break extraction.

### Same top-level namespace only

The lookup columns are not usable key prefixes, so a lookup without a traversal path filter scans the shared Siphon data lake.
Every resolver query filters on the top-level namespace of the source note.
Version 1 resolves references in the same project and in other projects under the same top-level namespace.
A reference to another top-level namespace does not resolve, so the graph misses that edge but never gets a wrong one.
A commit reference is parsed but not resolved, because the graph has no commit node type yet.
An integration test guards this limit.

### Later levers

The resolver could read a projected route set from the graph database instead of the data lake.
One option is a ClickHouse dictionary from project to traversal path, with constant-time lookups.
Its refresh cadence, owner, and source query are open questions with the Analytics team.
Another option loads the licensed route set into memory once per pass.
A dictionary would also resolve references across top-level namespaces and could check visibility at read time.
We revisit these if the full-pass benchmark fails or route lookups get twice as slow at staging scale.

Version 1 does not use partitions, because one pass takes about 54 seconds, and parallel parsing inside a batch would not help.
The partition support of ADR 014 would pick a column with few values, so partitions would need an override on the note ID.

## Why not the alternatives

### A Rails internal endpoint

A Rails call on the indexing path ties throughput to Puma thread capacity for work that is mostly text matching.
The MR diff resolver uses Rails because diffs live in object storage.
System notes are already in the data lake, so Rails has nothing to add.
A backfill of a 4 TB notes table through Rails could cause a production incident.
Rails could later act as a schema authority for unknown actions only.

### Structured actions only

The closed MR was this option, and its author found it insufficient.
It leaves the cross-reference gap open, and most lifecycle edges duplicate existing foreign keys.
We keep it only as the fallback mode.

### Extraction in Siphon

The Siphon table mapping supports only an ignore transformation.
Reference extraction would be a new Go feature in a repository that we do not own.
It has the same parser drift risk, and our reviewers cannot land fixes there.

### Parse the cached HTML

Rails resolves references when it saves a note and caches the rendered HTML.
Each resolved reference is a link with data attributes for the type, project, and target, and the Rails redactor depends on them.
We do not use it in version 1:

- Siphon replication of the cached HTML is not verified.
- HTML parse speed is not measured, and each payload is 10 to 50 times larger.
- Lifecycle actions render no link, so this option still needs the metadata table.
- The vendored list and the drift check cover the small body template surface.

We would switch if body parsing proves too brittle at scale, or if the scope grows to user notes and descriptions.
The HTML also carries the resolved project, which removes the default project lookup.

### One ClickHouse view per action

This pattern works for three lifecycle actions.
More than 10 cross-reference actions on three noteable types need about 30 redundant views.
One transform with one action filter is simpler, and Siphon guidance prefers skip indexes to many views.

### Two MRs, lifecycle first and cross-references second

The ADR review settles this question, and our recommendation is one MR.
Both slices share the Siphon prerequisite, the transform, the schema version bump, and the staging cycle.
Two MRs would give smaller reviews and earlier lifecycle value, and a cross-reference bug could not block lifecycle edges.
The transform has a flag per action, so the two-MR plan stays possible.

## References

- [Materialize edges from system notes](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/499)
- [Materialize lifecycle edges from system notes](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1109)
- [POC harness for system note edge materialization](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1335)
- [POC end-to-end validation report against GDK](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1335#note_3360033462)
- [Scaffold entity-level SDLC indexing](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/merge_requests/1341)
- [MR data ingestion gaps and the content resolver for MR diffs](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/482)
- [End-to-end validation on staging of the schema migration framework](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/443)
- [Reopen edges never emitted because reopen is a resource state event](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/883)
- [Add resource state events to Siphon replication](https://gitlab.com/gitlab-org/gitlab/-/merge_requests/241505)
- [Inverted mention edge direction in the system note transform](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/912)
- [Prior research and POC plan](https://gitlab.com/dgruzd/droid-workspace/-/tree/main/task/2685/)
- [End-to-end validation research](https://gitlab.com/dgruzd/droid-workspace/-/tree/main/task/2685-e2e/)
- [Entity refactor compatibility analysis](https://gitlab.com/dgruzd/droid-workspace/-/tree/main/task/2685-entity-refactor/)
- [ADR 009: Separate code indexer service](009_code_indexer_service.md)
