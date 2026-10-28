---
title: "GKG ADR 019: GQL response format"
creation-date: "2026-09-28"
authors: [ "@aalgutifan" ]
toc_hide: true
---

## Status

Proposed

## Context

Agents write graph patterns in GQL, for example a user who authored a merge request.
Orbit returns the result as raw JSON or in the GOON format ([ADR 012](012_goon_format.md)).
In both forms, the agent must join each edge back to the node bodies.

## Decision

**We add the `gql` response format, which prints result rows as a table of node literals.**

The table looks like the output of OpenCypher-like shells.
Each node alias in the query is one column, and each cell holds the full node literal.

```plaintext
+------------------------------------------------------------------------------------+
| u                                             | g                                  |
+------------------------------------------------------------------------------------+
| (:User {id: 1, username: "root"})             | (:Group {id: 22, name: "Toolbox"}) |
| (:User {id: 6, username: "georgine_keebler"}) | (:Group {id: 22, name: "Toolbox"}) |
+------------------------------------------------------------------------------------+

2 rows, more available
```

The columns depend on the query type:

- A traversal prints one column for each node alias, in pattern order, and one row for each authorized result row.
- An aggregation prints one column for each group and metric output name, and prints node groups as node literals.
- A neighbors query or a path-finding query prints one path column, and neighbor direction follows the stored edge.

Values use the literal forms of OpenCypher-like shells: quoted strings, true, false, and null.
Long text uses the shared truncation limits and adds a length property with the original length.
Cells pad to the widest cell in their column, up to a fixed width.
Longer cells overflow, so one long value does not widen every row.
The footer reports the row count.
A paginated page also reports that more rows are available and gives the next cursor.
A duplicate path or neighbor prints once, as in the raw format.
Traversals and aggregations keep every row.

The formatter reads the authorized, hydrated result rows.
It does not know the return expressions, so a returned property prints inside its node.
Only the query execution call renders GQL results, and it rejects unknown format values.
Schema queries return TOON text for `gql`, as they do for the `llm` agent format.

## Consequences

- REST and Model Context Protocol (MCP) support needs a separate GitLab change.
  Rails must accept the GQL format on the query endpoints and keep it when it forwards agent commands.
  The graph query tool schema advertises the format, and other commands stay on the `raw` and `llm` formats.
- We must deploy the Orbit release with this change before GitLab turns on GQL responses.
  Older servers return raw JSON for the new format value.
  Workhorse must inspect the format of the returned content, not only the requested format.
  Workhorse must treat a mismatch as an error or as an explicit fallback, or it can return an empty success response.
- Query analytics must record the selected response format, and a version pin is not enough.
  A separate analytics schema change adds that field before we measure adoption.
- Rows repeat full node bodies, so the output is larger than the TOON output of the agent format.
  The agent format uses TOON for query results after the removal of GOON.
  We compare the formats in the evaluation harness before we change the default.

## References

- [Add a Neo4j-style response format to the Orbit query API](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1263)
- [ADR 012: GOON format](012_goon_format.md)
