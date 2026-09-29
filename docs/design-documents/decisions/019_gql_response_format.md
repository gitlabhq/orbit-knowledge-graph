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

- Graph results use a `path` column, even when the result is empty or contains only nodes. Traversal and neighbors print one relationship per row, then any unlinked nodes. Path finding prints one chain per path. Aggregations use their own columns and add `// group_by:` and `// aggregations:` lines.
- A node prints its properties once. Later rows show only `(:Label {id: N})`.
- Multi-hop results use `-[*N]->`. The stored edge type describes one hop, not every hop. Edges at different depths remain distinct.
- Scalar values use openCypher literals. Strings escape pipes as `\u007C` so table cells stay distinct. Property order, truncation, and edge ordering use the shared `formatters/src/text.rs`.

`GqlFormatter` builds on the same `GraphResponse` as GOON. Only `ExecuteQuery` renders GQL results; it rejects unknown format values. Schema queries return TOON text for `gql`, as they do for `llm`.

## Consequences

- REST and MCP support require a separate GitLab change. Rails must accept `gql` on query endpoints and preserve it in `CommandInterceptor#orbit_command_format`. The `query_graph` schema advertises it; other commands stay on `raw` and `llm`.
- Deploy the GKG release containing this change before enabling GQL responses in GitLab. Older servers return raw JSON for enum value 2. Workhorse must inspect the returned content, not just the requested format, and handle a mismatch as an error or explicit fallback. Otherwise it can return an empty success response.
- Query analytics need the selected response format, not just another version pin. Add that field in a separate Iglu schema change before measuring adoption through telemetry.
- GOON 4.0.4 preserves edges that differ only in depth. Other GOON output stays unchanged.
- Relationship rows repeat endpoint IDs, so traversal output can be larger than GOON's. Compare the formats in the evals harness before changing the default. Retiring GOON also requires updates to dispatch, consumers, tests, and format discovery; it is not just a directory deletion.
