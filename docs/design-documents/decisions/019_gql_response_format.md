---
title: "GKG ADR 019: GQL response format"
creation-date: "2026-09-28"
authors: [ "@aalgutifan" ]
toc_hide: true
---

## Status

Accepted

## Context

Agents write GQL patterns such as `MATCH (u:User)-[:AUTHORED]->(mr:MergeRequest)`. Results come back as raw JSON or as GOON ([ADR 012](012_goon_format.md)), so the agent has to join edge lines back to node bodies. See [#1263](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1263).

## Decision

Add `RESPONSE_FORMAT_GQL` (`format=gql`). It prints the result rows as a cypher-shell style table: one column per node alias, and each cell holds the full node literal.

```plaintext
+------------------------------------------------------------------------------------+
| u                                             | g                                  |
+------------------------------------------------------------------------------------+
| (:User {id: 1, username: "root"})             | (:Group {id: 22, name: "Toolbox"}) |
| (:User {id: 6, username: "georgine_keebler"}) | (:Group {id: 22, name: "Toolbox"}) |
+------------------------------------------------------------------------------------+

2 rows, more available
```

- Traversal columns are the query's node aliases, in pattern order. Each row is one authorized result row.
- Aggregation columns are the group and metric output names. Node groups print as node literals.
- Neighbors and path finding print one `path` column, such as `(:User {id: 1})-[:MEMBER_OF]->(:Group {id: 22})`. Neighbor direction follows the stored edge.
- Values use cypher-shell literals: quoted strings, `TRUE`, `FALSE`, and `NULL`. Long text uses the shared truncation limits and adds a `<key>_len` property with the original length.
- Cells pad to the widest cell in their column. Padding stops at 120 characters, so one long value does not widen every row; longer cells overflow.
- The footer reports the row count. A paginated page adds `more available` and the next cursor.
- A duplicate path or neighbor prints once, as in the raw format. Traversal and aggregation keep every row.

The formatter reads the authorized, hydrated `PipelineOutput` rows. It does not know the `RETURN` expressions, so a returned property prints inside its node. Only `ExecuteQuery` renders GQL results; it rejects unknown format values. Schema queries return TOON text for `gql`, as they do for `llm`.

`format=llm` returns the same table, so every agent gets it without a client change. This retires GOON: the server no longer emits `FORMAT_NAME_GOON`, and the proto reserves its value.

## Consequences

- REST and MCP support require a separate GitLab change. Rails must accept `gql` on query endpoints and preserve it in `CommandInterceptor#orbit_command_format`. Commands advertise only `raw` and `llm`, because `llm` returns the same table.
- Deploy the GKG release containing this change before enabling GQL responses in GitLab. Older servers return raw JSON for enum value 2. Workhorse must inspect the returned content, not just the requested format, and handle a mismatch as an error or explicit fallback. Otherwise it can return an empty success response.
- Query analytics need the selected response format, not just another version pin. Add that field in a separate Iglu schema change before measuring adoption through telemetry.
- Rows repeat full node bodies, so output is larger than GOON's was. The evals harness comparison gates this change.
- Query analytics stop filling `goon_output_format_version`. The field stays in published Iglu schemas and is null on new events.
