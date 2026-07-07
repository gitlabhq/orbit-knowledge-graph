---
name: orbit
description: Query the GitLab Knowledge Graph (Orbit) via `glab orbit remote` CLI subcommands or run a local copy with `glab orbit local`. Use for code-structure questions (who calls this function, where is this symbol defined), cross-project dependency and blast-radius analysis, merge-request and contributor queries that require relationship traversal or aggregation, repository map / repo-map generation, and any question spanning relationships, cross-entity joins, or multi-entity aggregation across GitLab entities (projects, users, MRs, issues, pipelines, files, definitions, vulnerabilities). Do not use for single-entity GitLab lookups or write operations that `glab` handles directly (e.g. `glab mr view`, `glab mr create`).
version: 0.17.1
license: MIT
metadata:
  audience: developers
  keywords: orbit, knowledge-graph, gkg, graph, query, glab
  workflow: ai
---

# Orbit (GitLab Knowledge Graph) skill

Query the GitLab Knowledge Graph (product name **Orbit**) via the typed
`glab orbit remote` CLI subcommands (shipped in glab v1.94.0+). The typed CLI
handles the `Content-Type` header, response framing, and exit codes for you —
always go through `glab orbit remote`.

## Prerequisites

If `glab orbit` commands fail (command not found, auth errors, feature-flag
exit codes), work through the first-run setup checklist in
[`references/prerequisites.md`](references/prerequisites.md).

## Discovery

`glab orbit remote --help` and `glab orbit remote query --help` are the
authoritative usage references. For entity properties, prefer the recipes in
[`references/recipes.md`](references/recipes.md) over schema introspection —
they already encode the columns and filters known to work.

If you must introspect, call `glab orbit remote schema <Entity…>` with explicit
entity names — always pass the entity names you need rather than the unscoped
form, which returns ~17 KB+ of output. Call schema at most once per session;
schemas don't change mid-session. Use `glab orbit remote dsl` for the full DSL
JSON Schema. Note that per-node `outgoing_edges`/`incoming_edges` are arrays
of **strings** (edge type names), not objects — use `--jq` accordingly (e.g.
`schema Project --jq '.nodes[] | select(.name=="Project") | .properties'`).

Each `glab orbit remote query` has fixed per-call overhead. Prefer one
`aggregation` query over N traversal queries for "how many X grouped by Y", and
batch related lookups.

When editing Orbit docs or skills, fence executable query JSON as
`json orbit-query` so docs smoke tests run it.

## Running a query

Write the request body to a file and pass it to `glab orbit remote query`.
Default output is `llm` (compact, agent-friendly); pass `--format raw` to pipe
into `jq`. Endpoints are user-scoped — do **not** pass `-R owner/repo`.

Put the request body in `/tmp/q.json`:

```json orbit-query
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
```

```shell
glab orbit remote query /tmp/q.json
```

`filters` is an **object keyed by property name** — not an array. Use either
shorthand equality (`{"state": "opened"}`) or the operator form
(`{"iid": {"op": "eq", "value": 1216}}`). Operators: `eq`, `gt`, `lt`, `gte`,
`lte`, `in`, `contains`, `starts_with`, `ends_with`, `is_null`, `is_not_null`,
plus text-token operators (`token_match`, `all_tokens`, `any_tokens`) for
text-indexed properties — see [`query_language.md`](references/query_language.md).

`query_type` dictates the top-level shape: `neighbors` and single-node
`traversal` use `node` (singular); multi-node `traversal`, `aggregation`, and
`path_finding` use `nodes` (array) plus `relationships`.

- For multi-hop **traversal** edges, set `relationships[].max_hops` (and
  optionally `min_hops`). Default 1, max 3.
- For **path_finding** queries, set `path.max_depth` inside the required
  `path` sub-object. Max 3. `max_hops` does not apply to `path_finding`.
  When endpoints use filters, include `path.rel_types` to bound fan-out;
  path_finding follows edges only in their schema direction (see
  [recipe pitfall](references/recipes.md#path_finding--shortest-path-between-nodes)).

## Common pitfalls

Read [`references/recipes.md`](references/recipes.md) before constructing a
query — the same question often has one canonical paste-ready shape and several
wrong-looking-correct ones. Three traps recur:

- **"Pipelines for a merge request" requires `Pipeline.source =
  "merge_request_event"`.** Both `Pipeline.merge_request_id` and the
  `MergeRequest --TRIGGERED--> Pipeline` edge return parent *and* downstream
  child pipelines (`source = "parent_pipeline"`). Apply the
  `source = "merge_request_event"` filter (or the
  [canonical recipe](references/recipes.md#pipelines-that-ran-for-one-merge-request))
  to match the MR **Pipelines** tab.
- **Prefer single-node queries when you can bound the target entity directly.**
  Adding nodes/relationships only to "anchor" a query (joining `Project` +
  `MergeRequest` + `Pipeline` when you already know `merge_request_id`) can
  change the row shape and skew `aggregation` counts. If `recipes.md` shows a
  single-node form, use it.
- **`HAS_LATEST_DIFF` vs `HAS_DIFF` for file history.** `HAS_LATEST_DIFF`
  points only at the **most recent** diff snapshot of an MR. "Every MR that ever
  touched this file" needs `HAS_DIFF` (all snapshots) — `HAS_LATEST_DIFF` here
  can substantially undercount long-lived files. See
  [recipe](references/recipes.md#mrs-that-touched-a-file-historical-coverage).

## Iteration budget

A single user question should resolve in **at most 5 query attempts**. Tweaking
only `limit`/`columns` is not progress; changing `entity`, relationship type, or
a `filter` is. Validation errors (HTTP 400) count toward the budget. If you
exceed 5 without converging, **give up loudly**: report the shapes you tried,
what failed, and the next step — do not keep iterating or inflate a partial
answer. Full rules:
[`references/troubleshooting.md`](references/troubleshooting.md#iteration-budget-rules).

## Reporting results

Orbit answers are graph queries against ClickHouse, not an authoritative source
of truth. Always **surface known coverage gaps inline** (e.g. `HAS_LATEST_DIFF`
vs `HAS_DIFF`, time-bounded aggregates) and **show the query body** so the user
can audit it. Do not add a "Methodology" header that implies rigor the data
lacks. Full guidance and worked examples:
[`references/reporting.md`](references/reporting.md).

## Repository map helpers

For code-structure orientation before planning a change, use a bundled repo-map
helper (script paths are relative to this skill root, not the user's current
repo): the **local** helper for an uncommitted/branch-local checkout, the
**remote** helper for a project already indexed in Orbit Remote. See the
repository-map rows in [References](#references) below.

## Local CLI (glab orbit local)

`glab orbit local` downloads and runs a managed Orbit CLI binary for indexing
and querying a local copy of the Knowledge Graph (macOS/Linux only,
x86_64/aarch64). Prefer it over `glab orbit remote` when indexing a local
repository for offline analysis; use `remote` to query production. Install/run
with `glab orbit local` (add `--install` or `--update`). Full config keys and
pass-through args: [`references/local_cli.md`](references/local_cli.md).

## References

| Topic | Location |
|---|---|
| First-run setup checklist (install, auth, feature flag) | [`references/prerequisites.md`](references/prerequisites.md) |
| Full DSL reference | [`references/query_language.md`](references/query_language.md) |
| Paste-ready bodies per `query_type` | [`references/recipes.md`](references/recipes.md) |
| Reporting results & coverage caveats | [`references/reporting.md`](references/reporting.md) |
| Local repository map helper | [`references/local_repo_map.md`](references/local_repo_map.md) |
| Remote repository map helper | [`references/remote_repo_map.md`](references/remote_repo_map.md) |
| CLI exit codes (1-5), errors, iteration budget | [`references/troubleshooting.md`](references/troubleshooting.md) |
| Local CLI flags, config keys & pass-through args | [`references/local_cli.md`](references/local_cli.md) |
| Maintaining this skill (contributing, doc sync) | [`references/maintaining.md`](references/maintaining.md) |
