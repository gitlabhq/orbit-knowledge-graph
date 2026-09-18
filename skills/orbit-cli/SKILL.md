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
version: 0.13.0
license: MIT
metadata:
  audience: developers
  keywords: orbit, orbit-cli, orbit-local, knowledge-graph, code-graph, duckdb, sql, repo-map
  workflow: ai
---

# Orbit local CLI skill

The local CLI parses a checkout into a DuckDB property graph. `grep` finds
definitions. `context` reads their source and relationships. `sql` runs
read-only aggregations. `repo-map` orients you at the directory level. For
production data, use the `orbit` skill.

The binary is `orbit`, or `glab orbit` through the wrapper. Add `--yes` in
non-interactive shells. Run `orbit <command> --help` before you guess a flag.
Run `orbit skills` to read this skill; it lists additional skill trees afterward
when any are available. Use `orbit skills orbit [path]` to name this tree, or
pass a path alone to use it by default.

Wrapper details: [`references/local/cli.md`](references/local/cli.md).

## Find, then read

```shell
orbit index .
orbit grep "rate limit" --path src --kind Method,Function
orbit grep 'query_arrow|insert_batch|execute' --path crates/duckdb-client
orbit context Definition:<id> [Definition:<id>...]
orbit context src/lib.rs File:<id> Definition:<id>
```

One query per call. Quote `a|b|c` for OR alternatives with a shared result limit.
Different questions need separate calls. DuckDB FTS requires all searchable terms
in each alternative and orders results by BM25. OR uses each match's best
alternative score, not a bonus for matching more alternatives. Identifier
alternatives report case-insensitive exact symbol-name hits and misses within the
selected scope, before the result limit. Exact labels do not affect ranking.
Each search previews up to three returned exact-name hits, or its first name/path
match if none are exact. Body-only mentions remain listed without automatic
source or context suggestions; explicitly request their IDs to read them.
Previews share a 120-line budget per search; repeated previews and overlapping
source print once per invocation. Narrow with `--path`/`--kind` or raise `--limit`
instead of piping output through `head` or `tail`.

`grep` returns `Definition:<id>` references. Pass them to `context`, which
also accepts exact FQNs but not short names or globs. A path is shorthand for
its indexed `File:<id>`. File targets return a compact definition map with kinds,
line ranges, and every followable ID. Connections are bounded to ten per section
with clear omitted counts; choose a Definition ID for complete source and its
indexed connections. `<--` is a caller, `-->` is a callee. Connections from test,
fixture, and generated files appear in their own section. Reuse the returned
source. The printed `next:` command after a preview adds relationships and
complete source, repeating previewed lines.

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
