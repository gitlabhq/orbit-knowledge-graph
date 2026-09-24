---
name: orbit-cli
description: >
  Index and query a local checkout offline with the Orbit CLI (`orbit`, or
  `glab orbit`). Reach for it when a question names a symbol or spans code
  structure. Examples: who calls or extends this, where it is defined, what
  a file defines, how definitions are distributed. One call replaces many
  file reads and text greps. Works on the working tree and unpushed branches.
  Not a fit: text or config search, reading one known file, or hosted
  GitLab data (use the `orbit` skill).
version: 0.18.2
license: MIT
compatibility: Requires the Orbit CLI (directly or through glab); local indexing needs filesystem access to the checkout.
metadata:
  audience: developers
  keywords: orbit, orbit-cli, orbit-local, knowledge-graph, code-graph, duckdb, sql, repo-map
  workflow: ai
  source-project: gitlab-org/orbit/knowledge-graph
  source-path: plugins/orbit/skills/orbit-cli
---

# Orbit CLI skill

Orbit CLI parses a checkout into a DuckDB property graph. `grep` finds
definitions. `context` reads their source and relationships. `sql` runs
read-only aggregations. `repo-map` orients you at the directory level. For
production data, use the `orbit` skill.

The binary is `orbit`, or `glab orbit` through the wrapper.
Run `orbit <command> --help` before you guess a flag. Use `--yes` only when
that command's help lists it, such as `orbit setup`. Search commands do not
accept `--yes`.
Run `orbit skills` to read this skill; it lists additional skill trees afterward
when any are available. Use `orbit skills get orbit [path]` to read a file from
this tree. The path defaults to `SKILL.md`.

Wrapper details: [`references/local/cli.md`](references/local/cli.md).

## Fix inaccurate guidance

If guidance is wrong or outdated (command, flag, or behavior), tell the user.
With their confirmation, open a focused MR against `metadata.source-project` fixing `metadata.source-path` (one fix per MR, Conventional Commits).
If they decline, note the discrepancy in one line and continue with the corrected command.

## Find, then read

```shell
orbit grep "rate limit" --path src --kind Method,Function
orbit grep 'query_arrow|insert_batch|execute' --path crates/duckdb-client
orbit context Definition:<id> src/lib.rs:120-180 crates/duckdb-client
```

Use ordinary text tools for literal text, regex, configuration, and unsupported
files. Orbit FTS is not a regex replacement.

Quote `a|b|c` for OR alternatives. Each alternative uses conjunctive FTS, and a
single token must also match literally, ignoring case. Results rank exact names,
then name/path hits, then body mentions. Rows include Definition IDs and ranges;
body mentions also show the count and first matching line.

Pass Definition IDs, exact FQNs, paths, ranges, or directories to `context`.
Definition targets show full source and indexed relationships. File targets show
a compact map and at most ten connections per section, with omitted counts.
`<--` is a caller and `-->` is a callee. Reuse the returned source.
For caller/callee questions, follow the matching Definition ID with
`orbit context`. Grep body matches do not prove a call edge.

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
- An empty result does not prove that code is absent. The index can be stale
  or miss a language, so verify critical findings in source.
- If `orbit list` shows no index for the repository, ask before you run
  `orbit index .`.

## References

| Topic | Location |
|---|---|
| CLI wrapper flags, config keys, pass-through args | [`references/cli.md`](references/local/cli.md) |
| DuckDB tables and paste-ready SQL recipes | [`references/sql.md`](references/local/sql.md) |
| Repository-map workflow (`orbit repo-map`) | [`references/repo_map.md`](references/local/repo_map.md) |
