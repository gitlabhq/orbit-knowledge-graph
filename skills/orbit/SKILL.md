---
name: orbit
description: Query the GitLab Knowledge Graph (Orbit) via `glab orbit remote` CLI subcommands or run a local copy with `glab orbit local`. Use for code-structure questions (who calls this function, where is this symbol defined), cross-project dependency and blast-radius analysis, merge-request and contributor queries, and any question answerable by traversing GitLab's unified entity graph (projects, users, MRs, issues, pipelines, files, definitions, vulnerabilities).
version: 0.7.0
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
glab orbit remote dsl                           # full query DSL JSON Schema
glab orbit remote tools                         # MCP tool manifest
```

Always fetch the DSL with `glab orbit remote dsl` (which hits
`/api/v4/orbit/schema/dsl`) — it is the source of truth for the query body
shape. The `tools` manifest is for MCP wiring, not DSL discovery.

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

## Common pitfalls

Read [`references/recipes.md`](references/recipes.md) before constructing a
query — the same question often has one canonical paste-ready shape and
several wrong-looking-correct shapes. Two traps come up often:

- **"Pipelines for a merge request" requires `Pipeline.source =
  "merge_request_event"`.** The graph links every CI pipeline spawned in the
  context of an MR to that MR — including downstream child pipelines
  (`source = "parent_pipeline"`) that the top-level MR pipelines triggered.
  Both `Pipeline.merge_request_id` and the `MergeRequest --TRIGGERED-->
  Pipeline` edge return parents *and* children. Apply the
  `source = "merge_request_event"` filter (or use the canonical recipe in
  [`recipes.md`](references/recipes.md#pipelines-that-ran-for-one-merge-request))
  to match what the MR **Pipelines** tab, the REST
  `/merge_requests/:iid/pipelines` endpoint, and the GraphQL
  `mergeRequest.pipelines` connection return.
- **Prefer single-node queries when you can bound the target entity
  directly.** Adding extra nodes/relationships only to "anchor" the query
  (for example, joining `Project` + `MergeRequest` + `Pipeline` when you
  already know the MR's `merge_request_id`) can change the row shape in
  ways that affect `aggregation` counts. When `recipes.md` shows a
  single-node form for your question, use it.

## References

| Topic | Location |
|---|---|
| Full DSL reference | [`references/query_language.md`](references/query_language.md) |
| Paste-ready bodies per `query_type` | [`references/recipes.md`](references/recipes.md) |
| CLI exit codes (1-5) and common errors | [`references/troubleshooting.md`](references/troubleshooting.md) |
| `glab orbit local` install, update, config, and usage | [`references/local_cli.md`](references/local_cli.md) |

## Local CLI (glab orbit local)

`glab orbit local` downloads and runs a managed Orbit CLI binary for indexing
and querying a local copy of the Knowledge Graph. Key commands:

```bash
glab orbit local             # install (first run) and run
glab orbit local --install   # install only
glab orbit local --update    # update to latest compatible version
```

**Supported platforms:** macOS and Linux only (x86_64/aarch64; no Windows).

### When to prefer `glab orbit local` vs `glab orbit remote`

| Scenario | Recommended |
|---|---|
| Query the production GitLab Knowledge Graph | `glab orbit remote` |
| Index a local repository for offline analysis | `glab orbit local` |
| Use a custom or pre-built binary instead of the managed one | Set `orbit_local_binary_path` / `GLAB_ORBIT_LOCAL_BINARY_PATH` |

See [`references/local_cli.md`](references/local_cli.md) for full config keys,
pass-through args, and usage examples.

## Contributing

`references/query_language.md` is synced from
`docs/source/remote/queries/query-language.md`. Edit the upstream file, then run
`mise run skill:sync:orbit`. The lefthook `orbit-skill-docs-sync` job fails
the commit if the two files drift.
