---
name: orbit-cli
description: >
  Index and query a local checkout offline with the Orbit CLI (`orbit`, or
  `glab orbit`). Reach for it when a question names a symbol or spans code
  structure. Examples: who calls or extends this, where it is defined, what
  a file defines, how definitions are distributed. One call replaces many
  file reads and text greps. Works on the working tree and unpushed branches.
  Use grep with --connections for discovery; context expands a definition's source and relationships.
  Not a fit: plain-text or config search, or hosted GitLab data (use the `orbit` skill).
version: 0.19.0
license: MIT
compatibility: Requires the Orbit CLI (directly or through glab); local indexing needs filesystem access to the checkout.
metadata:
  audience: developers
  keywords: orbit, orbit-cli, orbit-local, knowledge-graph, code-graph, duckdb, sql, repo-map
  workflow: ai
  source-project: gitlab-org/orbit/knowledge-graph
  source-path: skills/orbit-cli
---

# Orbit local CLI skill

The local CLI parses a checkout into a DuckDB property graph. `grep` finds
definitions. `context` reads their source and relationships. `sql` runs
read-only aggregations. `repo-map` orients you at the directory level. For
production data, use the `orbit` skill.

Run `orbit` or `glab orbit` in the shell. Query first; `grep` and `context`
index automatically when needed. `--yes` is for setup/uninstall or the glab
wrapper, not direct grep/context/index. Check `orbit <command> --help` for flags.
Run `orbit skills` to list available skills. Use `orbit skills get orbit [path]`
to print the composed skill (`SKILL.md` by default), or specify a path to print
a file from the skill tree.

Wrapper details: [`references/local/cli.md`](references/local/cli.md).

## Fix inaccurate guidance

If guidance is wrong or outdated (command, flag, or behavior), tell the user.
With their confirmation, open a focused MR against `metadata.source-project` fixing `metadata.source-path` (one fix per MR, Conventional Commits).
If they decline, note the discrepancy in one line and continue with the corrected command.

## Orient with grep and context

Find related symbols together with `grep 'a|b|c'`; add `--connections` for callers/callees.
Use `context` for a definition's source and connections. Batch IDs or paths in one call.
Chain independent lookups. Inspect relevant code, tests, and helpers together.
Reuse shown source; search again only for a specific missing fact.

```shell
orbit grep "rate limit" --path src --kind Method,Function
orbit grep 'query_arrow|insert_batch|execute' --path crates/duckdb-client --connections
orbit context Definition:<id> src/lib.rs:120-180 tests/test_lib.rs
```

Quote `a|b|c` for OR alternatives. Each alternative uses conjunctive FTS, and a
single token must also match literally, ignoring case. Results rank exact names,
then name/path hits, then body mentions. Rows include Definition IDs and ranges;
body mentions also show the count and first matching line. `--connections` adds
up to three indexed connections per section per match, with omitted counts.

Pass Definition IDs, exact FQNs, paths, ranges, or directories to `context`.
Definition targets show full source and indexed relationships. File targets show a
compact map and up to ten connections per section, with omitted counts.
`<--` is a caller and `-->` is a callee.

<!-- orbit:section quick-start -->
## Query and map

```shell
orbit schema gl_definition
orbit sql "SELECT definition_type, count(*) n FROM gl_definition GROUP BY 1 ORDER BY n DESC"
orbit repo-map overview                 # then tree, api, class, extends, imports
```

<!-- /orbit:section -->

Gotchas:

- Queries are SQL against `gl_definition`, `gl_edge`, `gl_file`,
  `gl_directory`, and `gl_imported_symbol`. They are not the JSON DSL, which
  belongs to Orbit Remote.
- `definition_type` values are capitalized. `WHERE definition_type='function'`
  returns zero rows.
- `sql` scopes to the current commit; `--all` spans indexed checkouts.
  Unlike grep/context, SQL needs an explicit `orbit index .` if the commit is missing.

## References

| Topic | Location |
|---|---|
| CLI wrapper flags, config keys, pass-through args | [`references/cli.md`](references/local/cli.md) |
| DuckDB tables and paste-ready SQL recipes | [`references/sql.md`](references/local/sql.md) |
| Repository-map workflow (`orbit repo-map`) | [`references/repo_map.md`](references/local/repo_map.md) |
