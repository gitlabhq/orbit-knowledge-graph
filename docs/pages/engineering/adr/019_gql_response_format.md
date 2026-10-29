---
title: "GKG ADR 019: GQL response format"
creation-date: "2026-09-28"
authors: [ "@aalgutifan" ]
toc_hide: true
---

## Status

Proposed

## Date

2026-09-28

## Context

Agents write graph patterns in Graph Query Language (GQL), such as a user who authored a merge request.
Orbit returns the result as raw JSON or in the Graph Object Output Notation (GOON) format ([ADR 012](012_goon_format.md)).
Both forms return edges apart from the nodes, so the agent must join each edge back to its node bodies.

## Decision

**We add a GQL response format that prints result rows as a table of node literals.**

A node literal is the node type with its properties, as in the example below.
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

Values use the literal forms of OpenCypher-like shells: quoted strings, `TRUE`, `FALSE`, and `NULL`.
Long text uses the shared truncation limits and adds a length property with the original length.
Cells pad to the widest cell in their column, up to a fixed width, so one long value does not widen every row.
Longer cells overflow.
The footer reports the row count, and on a paginated page it also says that more rows are available and gives the next cursor.
A duplicate path or neighbor prints once, as in the raw format, but traversals and aggregations keep every row.

The formatter reads the authorized, hydrated result rows.
It does not know the return expressions, so a returned property prints inside its node.
Only the query execution call renders GQL results, and it rejects unknown format values.
Schema queries in the GQL format return Token-Oriented Object Notation (TOON) text, as they do in the large language model (LLM) agent format.

## Consequences

- REST and Model Context Protocol (MCP) support needs a separate GitLab change.
  Rails must accept the GQL format on the query endpoints and keep it when it forwards agent commands.
  The graph query tool schema advertises the format, and other commands stay on the raw and agent formats.
- We must deploy the Orbit release with this change before GitLab turns on GQL responses.
  Older Orbit releases return raw JSON for the new format value.
  Workhorse must check the format of the returned content as well as the requested format.
  Workhorse must treat a mismatch as an error or as an explicit fallback.
  Otherwise it can return an empty success response.
- Query analytics must record the selected response format, and a version pin is not enough.
  We add that field in a separate analytics schema change before we measure adoption through telemetry.
- Rows repeat full node bodies, so the output is larger than the TOON output of the agent format.
  The agent format uses TOON for query results after the removal of GOON.
  We compare the formats in the evaluation harness before we change the default.

## References

- [Add a Neo4j-style response format to the Orbit query API](https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/1263)
- [ADR 012: GOON format](012_goon_format.md)
