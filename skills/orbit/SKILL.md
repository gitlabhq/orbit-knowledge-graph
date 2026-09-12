---
name: orbit
description: Use the single `glab orbit` CLI to query hosted GitLab data or index and query local repositories. Use for code-structure questions (who calls this function, where is this symbol defined), cross-project dependency and blast-radius analysis, merge-request and contributor queries that require relationship traversal or aggregation, repository map / repo-map generation, and any question spanning relationships, cross-entity joins, or multi-entity aggregation across GitLab entities (projects, users, MRs, issues, pipelines, files, definitions, vulnerabilities). Do not use for single-entity GitLab lookups or write operations that `glab` handles directly (e.g. `glab mr view`, `glab mr create`).
version: 0.27.0
license: MIT
metadata:
  audience: developers
  keywords: orbit, knowledge-graph, gkg, graph, query, glab
  workflow: ai
---

# Orbit skill

Query GitLab Orbit (previously GitLab Knowledge Graph) through the flat `glab orbit` command tree. It needs glab v1.117.0 or later. Hosted commands handle authentication, response framing, and exit codes. Local commands use the managed binary and the local DuckDB graph.

## Prerequisites

If a `glab orbit` command fails with "command not found", an auth error, or a feature-flag exit code, work through the [first-run setup](references/troubleshooting.md#first-run-setup).

## Discovery

`glab orbit help` and `glab orbit <command> --help` are the authoritative usage references. For entity properties, prefer the recipes in [`references/recipes.md`](references/recipes.md) over schema introspection. They already encode the columns and filters known to work.

If you must introspect, call `glab orbit ontology <Entity...>` with explicit entity names. The unscoped form returns about 17 KB of output. Call it at most once per session, because the ontology does not change mid-session. `glab orbit dsl` prints the full DSL JSON Schema. The ontology command returns an object with a `nodes` array and does not accept `--jq`, so pipe into `jq`. Per-node `outgoing_edges` and `incoming_edges` are arrays of edge type names, not objects:

```shell
glab orbit ontology Project |
  jq '.nodes[] | select(.name == "Project") | .properties'
```

Each `glab orbit query` has fixed per-call overhead. Prefer one `aggregation` query over N traversal queries for "how many X grouped by Y", and batch related lookups.

When editing Orbit docs or skills, fence executable query JSON as `json orbit-query` so docs smoke tests run it.

## Running a query

Write the request body to a file and pass it to `glab orbit query`. Default output is `llm` (compact, agent-friendly). Pass `--response-format raw` to pipe into `jq`. Endpoints are user-scoped, so do not pass `-R owner/repo`.

Many filters need a numeric project ID. For the repository you are in, let `glab` resolve it from the Git remote.

```shell
PROJECT_ID=$(glab api projects/:fullpath | jq -r '.id')
```

Put the request body in `/tmp/q.json`.

```json orbit-query
{
  "query": {
    "query_type": "traversal",
    "nodes": [
      {"id": "p",  "entity": "Project",
       "filters": {"id": {"eq": 278964}}},
      {"id": "mr", "entity": "MergeRequest",
       "columns": ["iid", "title", "state"]}
    ],
    "relationships": [
      {"type": "IN_PROJECT", "from": "mr", "to": "p"}
    ],
    "order_by": "-mr.created_at",
    "limit": 5
  }
}
```

```shell
glab orbit query /tmp/q.json
```

`filters` is an object keyed by property name, not an array. Every query declares its node selectors in the `nodes` array. Filter operators, multi-hop `hops`, and `path_finding` limits are in [`references/query_language.md`](references/query_language.md). Paste-ready shapes for each `query_type` are in [`references/recipes.md`](references/recipes.md).

## Common pitfalls

Read the recipes before you construct a query. The same question often has one canonical shape and several wrong shapes that look correct. Four traps recur:

- Pipelines for a merge request need the `source = "merge_request_event"` filter. See [the recipe](references/recipes.md#pipelines-that-ran-for-one-merge-request).
- Prefer a single-node query when you can bound the target entity directly. Extra anchor nodes can change the row shape and skew `aggregation` counts.
- File history needs `HAS_DIFF`, not `HAS_LATEST_DIFF`. See [the recipe](references/recipes.md#mrs-that-touched-a-file-historical-coverage).
- Issues, epics, tasks, and incidents are the `WorkItem` entity. There is no `Issue` node. See [the recipe](references/recipes.md#work-items-in-a-project).

## Iteration budget

Resolve a user question in at most 5 query attempts. Changing only `limit` or `columns` is not progress. Changing `entity`, the relationship type, or a `filter` is. Validation errors count toward the budget. After 5 attempts, stop and report the shapes you tried, what failed, and the next step. Full rules: [`references/troubleshooting.md`](references/troubleshooting.md#iteration-budget-rules).

## Reporting results

Orbit answers are graph queries against ClickHouse, not an authoritative source of truth. Surface known coverage gaps inline, and show the query body so the user can audit it. Do not add a "Methodology" header that implies rigor the data lacks. Full guidance: [`references/reporting.md`](references/reporting.md).

## Repository map helpers

For code-structure orientation before you plan a change, use `glab orbit repo-map` on a local checkout. For a project already indexed in Orbit Remote, use the bundled remote helper script. The script path is relative to this skill root, not the user's repository. See the repository-map rows in [References](#references).

## Managed CLI

`glab orbit` downloads, verifies, and runs the Orbit binary from the `orbit-local` package (macOS and Linux, x86_64 and aarch64). The command selects the backend. `index`, `grep`, `context`, `sql`, `schema`, `list`, `mcp`, and `repo-map` use the local graph. `query`, `status`, `ontology`, `dsl`, `tools`, and `graph-status` use Orbit Remote.

glab handles `--install`, `--update`, and `--yes` itself and forwards everything else to the binary. `--yes` skips the confirmation prompts, so pass it in scripts and agent runs. `glab orbit --help` shows the wrapper help. `glab orbit help` and `glab orbit <command> --help` show the binary's.

```bash
glab orbit --install --yes   # install without running
glab orbit --update          # install the latest compatible version
```

Skip the confirmation prompts for good with `glab config set orbit_local_auto_run true` and `glab config set orbit_local_auto_download true`. Point glab at your own build with `glab config set orbit_local_binary_path /path/to/orbit` or the `GLAB_ORBIT_LOCAL_BINARY_PATH` env var. That skips download, version checks, and updates.

## References

| Topic | Location |
|---|---|
| First-run setup, exit codes, errors, iteration budget | [`references/troubleshooting.md`](references/troubleshooting.md) |
| Full DSL reference | [`references/query_language.md`](references/query_language.md) |
| Paste-ready bodies per `query_type` | [`references/recipes.md`](references/recipes.md) |
| Reporting results and coverage caveats | [`references/reporting.md`](references/reporting.md) |
| Local repository map command (`glab orbit repo-map`) | [`references/local_repo_map.md`](references/local_repo_map.md) |
| Remote repository map helper | [`references/remote_repo_map.md`](references/remote_repo_map.md) |
| Maintaining this skill (contributing, doc sync) | [`references/maintaining.md`](references/maintaining.md) |
