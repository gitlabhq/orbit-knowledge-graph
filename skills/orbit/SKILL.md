---
name: orbit
description: Query the GitLab Knowledge Graph (Orbit) via the typed `glab orbit remote` CLI subcommands. Use for code-structure questions (who calls this function, where is this symbol defined), cross-project dependency and blast-radius analysis, merge-request and contributor queries, and any question answerable by traversing GitLab's unified entity graph (projects, users, MRs, issues, pipelines, files, definitions, vulnerabilities).
version: 0.5.0
license: MIT
metadata:
  audience: developers
  keywords: orbit, knowledge-graph, gkg, graph, query, glab
  workflow: ai
---

# Orbit (GitLab Knowledge Graph) skill

Query the GitLab Knowledge Graph (product name **Orbit**) via the typed
`glab orbit remote` CLI subcommands (shipped in glab v1.94.0+).

**Do not use `glab api orbit/*`.** The typed CLI handles the
`Content-Type` header, response framing, and exit codes for you.

## Discovery

`glab orbit remote --help` and `glab orbit remote query --help` are the
authoritative usage references. Pass entity names to `schema` to get scoped
properties — calling `schema` without arguments returns the full ontology
(~28 KB) and is rarely what you want:

```bash
glab orbit remote schema MergeRequest Project   # scoped properties
glab orbit remote tools                         # full DSL JSON Schema
```

## Running a query

Write the request body to a file and pass it to `glab orbit remote query`.
Default output is `llm` (compact, agent-friendly); pass `--format raw` to
pipe into `jq`. Endpoints are user-scoped — do **not** pass `-R owner/repo`.

```bash
cat > /tmp/q.json <<'JSON'
{
  "query": {
    "query_type": "traversal",
    "nodes": [
      {"id": "p",  "entity": "Project",
       "filters": {"id": {"op": "eq", "value": 278964}}},
      {"id": "mr", "entity": "MergeRequest",
       "columns": ["iid", "title", "state"]}
    ],
    "relationships": [
      {"type": "IN_PROJECT", "from": "mr", "to": "p"}
    ],
    "order_by": {"node": "mr", "property": "created_at", "direction": "DESC"},
    "limit": 5
  }
}
JSON
glab orbit remote query /tmp/q.json
```

`filters` is an **object keyed by property name** — not an array. Use either
shorthand equality (`{"state": "opened"}`) or the operator form
(`{"iid": {"op": "eq", "value": 1216}}`). Operators: `eq`, `gt`, `lt`,
`gte`, `lte`, `in`, `contains`, `starts_with`, `ends_with`, `is_null`,
`is_not_null`.

`query_type` dictates the top-level shape: `neighbors` and single-node
`traversal` use `node` (singular); multi-node `traversal`, `aggregation`,
and `path_finding` use `nodes` (array) plus `relationships`. `max_depth`
and `max_hops` are capped at 3 server-side.

## References

| Topic | Location |
|---|---|
| Full DSL reference | [`references/query_language.md`](references/query_language.md) |
| Paste-ready bodies per `query_type` | [`references/recipes.md`](references/recipes.md) |
| CLI exit codes (1-5) and common errors | [`references/troubleshooting.md`](references/troubleshooting.md) |

## Contributing

`references/query_language.md` is synced from
`docs/source/queries/query_language.md`. Edit the upstream file, then run
`mise run skill:sync:orbit`. The lefthook `orbit-skill-docs-sync` job fails
the commit if the two files drift.
