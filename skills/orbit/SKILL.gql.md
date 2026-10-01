---
name: orbit
description: Use the `glab orbit` CLI for questions about code structure, blast radius, cross-project links, and relationships across GitLab entities, and to build a repo map. It works on hosted or local data. Skip it for single-entity lookups or writes that `glab` already handles.
version: 0.33.0+gql
license: MIT
compatibility: Requires the Orbit CLI (directly or through glab) and network access to the GitLab instance for Orbit Remote commands.
metadata:
  audience: developers
  keywords: orbit, knowledge-graph, gkg, graph, query, glab
  workflow: ai
  source-project: gitlab-org/orbit/knowledge-graph
  source-path: skills/orbit
---

# Orbit skill

Query GitLab Orbit (previously GitLab Knowledge Graph) through the flat `glab orbit` command tree. It needs glab v1.117.0 or later. Hosted commands handle authentication, response framing, and exit codes. Local commands use the managed binary and the local DuckDB graph.

## Prerequisites

If a `glab orbit` command fails with "command not found", an auth error, or a feature-flag exit code, work through the [first-run setup](references/troubleshooting.md#first-run-setup).

## Fix inaccurate guidance

If guidance is wrong or outdated (command, flag, or behavior), tell the user.
With their confirmation, open a focused MR against `metadata.source-project` fixing `metadata.source-path` (one fix per MR, Conventional Commits).
If they decline, note the discrepancy in one line and continue with the corrected command.

## Query language

Queries are read-only GQL text (`MATCH ... RETURN`). `glab orbit dsl` is unavailable.

## Discovery

`glab orbit help` and `glab orbit <command> --help` are the authoritative usage references. Run `glab orbit skills` to list available skills. Run `glab orbit skills get orbit` to print the composed skill (`SKILL.md` by default), or append a path such as `references/local/sql.md` to print a file.

For entity properties and relationship types, run `CALL db.schema('Node')` with an explicit node name. `CALL db.schema()` lists every node and relationship type; call it at most once per session, because the schema does not change mid-session. Query syntax and paste-ready queries are in [`references/gql.md`](references/gql.md).

The named-query catalog at `GET /api/v4/orbit/query/templates` is rendered in your mode. Do not reuse discovery results across users or mode changes. Entries contain only `name`, `description`, and `raw_query`; there is no client language selector. See [catalog troubleshooting](references/troubleshooting.md#named-query-catalog).

Each `glab orbit query` has fixed per-call overhead. Prefer one aggregate query, with `count(...)` in `RETURN`, over N lookups for "how many X grouped by Y", and batch related lookups.

When editing Orbit docs or skills, fence executable JSON queries as `json orbit-query` and GQL queries as `gql orbit-query` so docs smoke tests run them.

## Running a query

Pass query text inline. The CLI builds the request envelope, so a short query needs no temporary file:

```shell
glab orbit query 'CALL db.schema()'
glab orbit query "CALL db.schema('Project')"
glab orbit query "MATCH (p:Project {full_path: 'gitlab-org/gitlab'}) RETURN p.id, p.full_path LIMIT 1"
```

With a standalone install, use `orbit query` instead of `glab orbit query`. Quote the whole query for the shell; single-quoted GQL string literals then need double quotes around the query. Inline query text needs Orbit CLI 0.130.0 or later. If `glab orbit query` rejects the text argument, run `glab orbit --update`.

`--file` reads a JSON request envelope whose `query` field holds the GQL text, not a bare GQL file. `--file -` reads that envelope from stdin. Default output is `llm` (compact, agent-friendly). Pass `--response-format raw` to pipe into `jq`. Endpoints are user-scoped, so do not pass `-R owner/repo`.

## Common pitfalls

Read [`references/gql.md`](references/gql.md) before you construct a query. These traps recur:

- At least one node needs an ID or a property filter. `LIMIT` does not bound the scan, so `MATCH (p:Project) RETURN p LIMIT 5` rejects.
- Write relationship types after a colon: `-[:AUTHORED]->`. `-[AUTHORED]->` declares a variable and rejects.
- Pipelines for a merge request need `WHERE pl.source = 'merge_request_event'`.
- Prefer a single anchored node when you can bound the target directly. Extra anchor nodes can change the row shape and skew aggregate counts.
- File history needs `HAS_DIFF`, not `HAS_LATEST_DIFF`. It repeats a file or merge request once per diff snapshot. To list the files of one merge request, use [this recipe](references/gql.md#files-a-merge-request-touched). To list the merge requests that touched one file, use [this one](references/gql.md#merge-requests-that-touched-a-file).
- Code questions (subclasses, callers) and word search use `Definition` with `EXTENDS`/`CALLS` and `token_match`/`any_tokens`/`all_tokens`; see [the recipes](references/gql.md#subclasses-of-a-class) and [token search](references/gql.md#token-search).
- If a response reports `pagination.truncated` (or `truncated:true` in `llm` output), more rows matched than were returned; say so.
- Issues, epics, tasks, and incidents are the `WorkItem` node. There is no `Issue` node.
- There is no `OR`, general `NOT`, `DISTINCT`, `count(*)`, `OPTIONAL MATCH`, or `WITH`.

## Iteration budget

Resolve a user question in at most 5 query attempts, validation errors included. Changing only the row limit or the returned properties is not progress. Full rules: [`references/troubleshooting.md`](references/troubleshooting.md#iteration-budget-rules).

## Reporting results

Orbit answers come from graph queries, not an authoritative source. Show the query body and its coverage gaps with every result. Full guidance: [`references/reporting.md`](references/reporting.md).

## Repository map helpers

For code-structure orientation before you plan a change, use `glab orbit repo-map` on a local checkout.
See the repository-map rows in [References](#references).

<!-- orbit:include local:quick-start -->

## Managed CLI

`glab orbit` downloads, verifies, and runs the Orbit binary from the `orbit-cli` package (macOS, Linux, and Windows). The command selects the backend. `index`, `grep`, `context`, `sql`, `schema`, `list`, `mcp`, and `repo-map` use the local graph. `query`, `status`, `ontology`, `tools`, and `graph-status` use Orbit Remote.

glab handles `--install`, `--update`, and `--yes` itself and forwards everything else to the binary. `--install` and `--update` are mutually exclusive. `--yes` skips the confirmation prompts, so pass it in scripts and agent runs. `glab orbit --help` shows the wrapper help. `glab orbit help` and `glab orbit <command> --help` show the binary's.

```shell
glab orbit --install --yes   # install without running
glab orbit --update          # install the latest compatible version
```

Skip the confirmation prompts for good with `glab config set orbit_cli_auto_run true` and `glab config set orbit_cli_auto_download true`. Point glab at your own build with `glab config set orbit_cli_binary_path /path/to/orbit` or the `GLAB_ORBIT_CLI_BINARY_PATH` env var. That skips download, version checks, and updates. glab records `orbit_cli_binary_version`, `orbit_cli_binary_checksum`, and `orbit_cli_last_update_check` itself. Do not set them by hand.

## References

| Topic | Location |
|---|---|
| First-run setup, exit codes, catalog, iteration budget | [`references/troubleshooting.md`](references/troubleshooting.md) |
| GQL syntax, paste-ready queries, and GQL errors | [`references/gql.md`](references/gql.md) |
| Reporting results and coverage caveats | [`references/reporting.md`](references/reporting.md) |
| Local repository map command (`glab orbit repo-map`) | [`references/local_repo_map.md`](references/local_repo_map.md) |
| Maintaining this skill (contributing, doc sync) | [`references/maintaining.md`](references/maintaining.md) |
