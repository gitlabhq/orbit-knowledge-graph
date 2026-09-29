---
title: "GKG ADR 019: GQL response format"
creation-date: "2026-09-28"
authors: [ "@aalgutifan" ]
toc_hide: true
---

## Status

Proposed

## Context

Agents write GQL patterns such as `MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest)`. Results come back as raw JSON or as GOON ([ADR 012](012_goon_format.md)), so the agent has to join edge lines back to node bodies. See [#1263](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1263).

## Decision

Add `RESPONSE_FORMAT_GQL` (`format=gql`). It prints results as a pipe table of pattern literals in the syntax the agent wrote:

```plaintext
// query_type: traversal, rows: 2, has_more: true, gql_version: 1.0.0
| path |
| (:User {id: 1, username: "alice"})-[:AUTHORED]->(:MergeRequest {id: 42, iid: 101}) |
| (:User {id: 1})-[:AUTHORED]->(:MergeRequest {id: 43, iid: 102}) |
```

- Search results use a `node` column. Traversal and neighbors use `path`, with one relationship per row and then any unlinked nodes. Path finding prints one chain per path. Aggregations use their own columns and add `// group_by:` and `// aggregations:` lines.
- A node prints its properties once. Later rows show only `(:Label {id: N})`.
- Values are openCypher literals. Property order, truncation, and edge ordering come from GOON through the shared `formatters/src/text.rs`.

`GqlFormatter` builds on the same `GraphResponse` as GOON, and only `ExecuteQuery` honors the new value. GKG renders it rather than Rails, so results still stream through Workhorse and MCP `query_graph` gets the format too.

## Consequences

- Rails and Workhorse must accept `gql` before REST and MCP callers can use it.
- Relationship rows repeat endpoint IDs, so traversal output can be larger than GOON's. The evals harness decides whether `gql` replaces GOON.
- If it does, `llm` points at `GqlFormatter` and `goon/` is deleted.
