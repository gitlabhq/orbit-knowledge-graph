---
name: orbit-cli
description: >
  Index and query a local checkout offline with the Orbit CLI (`orbit`, or
  `glab orbit`). Reach for it when a question names a symbol or spans code
  structure. Examples: who calls or extends this, where it is defined, what
  a file defines, how definitions are distributed. One call replaces many
  file reads and text greps, including matches in config, templates, and docs.
  Works on the working tree and unpushed branches. Not a fit: reading one known
  file, or hosted GitLab data (use the `orbit` skill).
version: 0.22.0
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
every matching line in code, tests, config, and docs, labeled with its enclosing definition. `context` reads their source and relationships. `sql` runs
read-only aggregations. `repo-map` orients you at the directory level. For
production data, use the `orbit` skill.

The binary is `orbit`, or `glab orbit` through the wrapper. Add `--yes` in
non-interactive shells. Run `orbit <command> --help` before you guess a flag.
Run `orbit skills` to list available skills. Use `orbit skills get orbit [path]`
to print the composed skill (`SKILL.md` by default), or specify a path to print
a file from the skill tree.

Wrapper details: [`references/local/cli.md`](references/local/cli.md).

## Fix inaccurate guidance

If guidance is wrong or outdated (command, flag, or behavior), tell the user.
With their confirmation, open a focused MR against `metadata.source-project` fixing `metadata.source-path` (one fix per MR, Conventional Commits).
If they decline, note the discrepancy in one line and continue with the corrected command.

## Find, then read

```shell
orbit index .
orbit grep "rate limit" --path src --kind Method,Function
orbit grep 'query_arrow|insert_batch|execute' --path crates/duckdb-client
orbit context duckdb_client::search::DuckDbSearch::grep src/lib.rs:120-180 crates/duckdb-client
```

Quote `a|b|c` for OR alternatives. Plain words ignore case, `_`, and `-`; terms
with regex characters match as regex. Output is rg's: `path:line:text` for each
match. The usual rg and grep flags work: `-A`/`-B`/`-C`, `-l`, `-c`, `-w`,
`-F`, `-v`, `-g`, `--include`, `-t`. Before the lines of each definition, a
`path-N-» Kind name:start-end ←callers →callees` line names it. Files that
define a term come first, then code, tests, and config or docs. Read a
definition's body with `context file:start-end`.

Pass names as printed by `grep`, paths, ranges, or directories to `context`.
Definition targets show full source and indexed relationships. File targets show
a compact map and at most ten connections per section, with omitted counts.
`<--` is a caller and `-->` is a callee. Reuse the returned source.

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
- The graph holds every indexed checkout. `sql` scopes to the current commit,
  and `--all` spans them all. Re-index after you check out a different commit.

## References

| Topic | Location |
|---|---|
| CLI wrapper flags, config keys, pass-through args | [`references/cli.md`](references/local/cli.md) |
| DuckDB tables and paste-ready SQL recipes | [`references/sql.md`](references/local/sql.md) |
| Repository-map workflow (`orbit repo-map`) | [`references/repo_map.md`](references/local/repo_map.md) |
