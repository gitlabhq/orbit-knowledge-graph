---
name: orbit
description: Query the GitLab Knowledge Graph (Orbit) via `glab orbit remote` CLI subcommands or run a local copy with `glab orbit local`. Use for code-structure questions (who calls this function, where is this symbol defined), cross-project dependency and blast-radius analysis, merge-request and contributor queries, and any question answerable by traversing GitLab's unified entity graph (projects, users, MRs, issues, pipelines, files, definitions, vulnerabilities).
version: 0.12.0
license: MIT
metadata:
  audience: developers
  keywords: orbit, knowledge-graph, gkg, graph, query, glab
  workflow: ai
---

# Orbit (GitLab Knowledge Graph) skill

Query the GitLab Knowledge Graph (product name **Orbit**) via the typed
`glab orbit remote` CLI subcommands (shipped in glab v1.94.0+). The typed
CLI handles the `Content-Type` header, response framing, and exit codes for
you — always go through `glab orbit remote`.

## Discovery

`glab orbit remote --help` and `glab orbit remote query --help` are the
authoritative usage references. For entity properties, prefer the recipes
in [`references/recipes.md`](references/recipes.md) over schema introspection
— recipes already encode the columns and filters that are known to work.

If you do need to introspect, call `schema` **at most once per session**
and pass entity names to scope the response:

```bash
glab orbit remote schema MergeRequest Project   # scoped properties
glab orbit remote dsl                           # full query DSL JSON Schema
glab orbit remote tools                         # MCP tool manifest
```

Always fetch the DSL with `glab orbit remote dsl` — it is the source of truth
for the query body shape. The `tools` manifest is for MCP wiring, not DSL
discovery.

Calling `schema` without arguments returns the full ontology (~28 KB) and is
rarely what you want. Re-fetching `schema` or `tools` between turns is pure
latency — schemas do not change within a session. Cache the response (or just
keep it in agent context) and reuse it.

Each `glab orbit remote query` invocation has fixed per-call overhead
(process startup, auth load, HTTPS round-trip). Prefer one `aggregation`
query over N traversal queries when the question is "how many X grouped
by Y", and batch related lookups where possible.

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
  to match what the MR **Pipelines** tab shows.
- **Prefer single-node queries when you can bound the target entity
  directly.** Adding extra nodes/relationships only to "anchor" the query
  (for example, joining `Project` + `MergeRequest` + `Pipeline` when you
  already know the MR's `merge_request_id`) can change the row shape in
  ways that affect `aggregation` counts. When `recipes.md` shows a
  single-node form for your question, use it.
- **`HAS_LATEST_DIFF` vs `HAS_DIFF` for file history.** `HAS_LATEST_DIFF`
  only points at the **most recent** diff snapshot of an MR (joined via
  `MergeRequest.latest_merge_request_diff_id`). Questions like "every MR
  that ever touched this file" need `HAS_DIFF` (all snapshots, joined
  via `MergeRequestDiff.merge_request_id`) — using `HAS_LATEST_DIFF`
  here can substantially undercount historical coverage on long-lived
  files. See
  [`recipes.md`](references/recipes.md#mrs-that-touched-a-file-historical-coverage).
- **Inheritance trees can be incomplete.** `Definition` indexing is known
  to under-cover large class hierarchies (e.g. `ApplicationRecord`) and
  EE-namespaced subclasses. Present a graph-only subclass count as graph
  coverage, not an authoritative total. See
  [`recipes.md`](references/recipes.md#subclasses--descendants-of-a-class).

## Iteration budget

A single user question should resolve in **at most 5 query attempts**. If
you exceed that budget without converging on a working answer, stop and
report what you tried, what failed, and what the next step would be — do
not keep iterating.

Concrete rules:

1. **Each retry must change something material.** Tweaking only `limit`
   or `columns` does not count as progress; changing `entity`, the
   relationship type, or a `filter` does.
2. **Validation errors (HTTP 400) count toward the budget.** Three
   consecutive validation errors on the same query shape means the shape
   is wrong — stop, re-read the relevant recipe, and pick a different
   shape.
3. **Empty results are not necessarily a failure.** Confirm with the
   known-good probe in
   [`troubleshooting.md`](references/troubleshooting.md#empty-result-body)
   before assuming the query is wrong.
4. **When you give up, give up loudly.** Tell the user: "Orbit did not
   return an answer after 5 attempts. The query shapes I tried were:
   [...]. Suggested next steps: [...]." A clear give-up is more useful
   than silently inflating a partial result.

Cost grows linearly in attempts, both in CLI shell-out time and in agent
context. A hard cap is cheaper than an ambiguous answer.

## Repository map helpers

For code-structure orientation before planning a change, use one of the bundled
repo-map helpers and load the matching reference for details:

- **Local checkout / uncommitted or branch-local code:** use the Orbit Local
  helper. See [`references/local_repo_map.md`](references/local_repo_map.md).
- **Project already indexed in Orbit Remote / no local checkout needed:** use
  the Orbit Remote helper. See
  [`references/remote_repo_map.md`](references/remote_repo_map.md).

Helper script paths are relative to the Orbit skill root (the directory
containing this `SKILL.md`), not the user's current repository. The reference
files include invocation examples and path-resolution notes.

## Reporting results

Orbit answers are graph queries against ClickHouse, not an authoritative
source of truth. Always present results with their coverage caveats. The
agent should:

1. **Distinguish counts from completeness.** Phrase results as "Orbit
   returned N matches" rather than "there are N". The graph is not an
   authoritative total, so reserve "there are N" for cases where the gap
   classes below do not apply.
2. **Surface known coverage gaps inline.** If the query falls into one of
   the documented gap classes — historical file coverage
   (`HAS_LATEST_DIFF` vs `HAS_DIFF`), large or EE-namespaced inheritance
   trees, time-bounded aggregates — append a one-line caveat to the
   answer, not a buried footnote.
3. **Show the query.** Include the JSON request body (collapsed if long)
   so the user can audit the traversal.
4. **Do not invent a "Methodology" header that implies rigor the
   underlying data does not support.** A "Methodology" section is
   appropriate when the query itself is non-obvious; it is not a
   substitute for coverage caveats.

Concretely, an answer to "how many pipelines ran for MR !235291?" should
look like:

> Orbit returned 16 pipelines for MR !235291 (filtered by
> `source = "merge_request_event"`). This matches the MR Pipelines tab.

Not:

> **Methodology**
> I queried `MergeRequest --TRIGGERED--> Pipeline` and got 98 results,
> broken down by status: ...

## References

| Topic | Location |
|---|---|
| Full DSL reference | [`references/query_language.md`](references/query_language.md) |
| Paste-ready bodies per `query_type` | [`references/recipes.md`](references/recipes.md) |
| Local repository map helper | [`references/local_repo_map.md`](references/local_repo_map.md) |
| Remote repository map helper | [`references/remote_repo_map.md`](references/remote_repo_map.md) |
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

If Orbit guidance, recipes, or helper behavior is inaccurate, update this skill
in `gitlab-org/orbit/knowledge-graph` rather than working around it silently.
Keep `SKILL.md`, `references/`, and `scripts/` in sync, and use `opencode run`
for meaningful behavior changes.

`references/query_language.md` is synced from
`docs/source/remote/queries/query-language.md`. Edit the upstream file, then run
`mise run skill:sync:orbit`. The lefthook `orbit-skill-docs-sync` job fails
the commit if the two files drift.
