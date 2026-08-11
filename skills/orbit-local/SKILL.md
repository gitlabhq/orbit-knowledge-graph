---
name: orbit-local
description: >
  Answers structural questions about a code repository: which classes extend a
  base type, what a module exposes with signatures, where a common-named symbol
  is defined, and how an unfamiliar repository is organized. Resolves symbols
  instead of matching text. Prefer it over grep/rg when a name has many text
  hits or the answer spans files; prefer grep/rg for literal strings, config,
  docs, and unique symbols. Indexing a checkout often takes a few seconds
  (`orbit index .`); large monorepos take longer.
version: 0.4.0
license: MIT
metadata:
  audience: developers
  keywords: orbit, orbit-local, knowledge-graph, code-graph, duckdb, sql, repo-map
  workflow: ai
---

# Orbit local CLI skill

Use Orbit Local's Code Graph to answer structural questions about the current
checkout. Start with `repo-map`: it returns source locations and resolved
relationships without requiring SQL.

## Start with `repo-map`

Index the repository, then choose the narrowest map for the question. Indexing
often takes a few seconds, with longer times for large monorepos. This example
runs from the knowledge-graph repository root and shows its structure,
implementations of `HasRules`, and the API surface of the Orbit Local crate:

```bash
orbit index .
orbit repo-map overview
orbit repo-map extends HasRules
orbit repo-map api crates/orbit-local
```

Use the map that matches the question:

- `repo-map overview` for languages, directories, and key abstractions in an
  unfamiliar repository.
- `repo-map tree <path>` for what lives below a directory.
- `repo-map api <path>` for definitions and extracted signatures below a path.
- `repo-map class <name>` for a class, module, or trait and its members.
- `repo-map extends <name>` for descendants of a base type.
- `repo-map imports <pattern>` for files importing matching symbols or paths.

Use returned `path:line` locations to read the relevant source instead of
immediately repeating the search with grep. Keep `api` scoped to a feature
directory, package, or crate rather than a large repository root. See the full
[`repo-map` reference](references/repo_map.md) for options and output details.

## Choose graph or grep

Prefer the graph when the answer depends on code structure or relationships,
especially when a common name produces many text matches or the answer spans
multiple files. Examples include inheritance trees, module API surfaces,
same-named definitions, imports, and repository orientation.

Prefer grep/rg when searching for literal strings, configuration keys,
documentation, generated text, or a unique symbol whose text match directly
identifies the file. Read a known file directly rather than mapping it again.

## Focused SQL queries

Use read-only SQL when `repo-map` does not express the relationship you need.
Start with partial matching for an unfamiliar symbol instead of a brittle exact
match:

```bash
orbit sql "SELECT name, definition_type, file_path, start_line
FROM gl_definition
WHERE name LIKE '%HasRules%'
ORDER BY file_path, start_line"
```

Search paths with `gl_file.path` or use `repo-map tree` when the search term may
name a directory rather than a definition. See [`references/sql.md`](references/sql.md)
for callers, definitions in a file, subclasses, imports, and table details.
Member-assigned JavaScript or TypeScript exports may not be indexed as
definitions, so partial `LIKE` matching can also return no rows. For a unique
symbol in that form, grep/rg is the right fallback.

### Callers recall limitation

`CALLS` edges are statically resolved and are not complete. Calls through
attributes or parameters may not resolve to the receiver's type, so a "who calls
X" query can return direct and test callers while missing the production caller,
for example `client.check_quota_available()`. Verify caller searches with grep/rg
when recall matters.

## Invocation and reference

The binary is `orbit`. Through glab, prefix commands with `glab orbit local
--yes`, for example `glab orbit local --yes repo-map overview`. Run `glab orbit
local --install --yes` to install or update the managed binary.

Index a repository root, not a plain subdirectory. The graph is scoped to a
commit, so run `orbit index .` again after checking out another commit. Use the
`orbit` skill instead for cross-project and GitLab SDLC questions against Orbit
Remote, and use `glab` for single-entity GitLab lookups and writes.

### Command and schema appendix

| Command | Purpose |
|---|---|
| `orbit index <path> [--stats] [--db P]` | Index Git repositories found under a path |
| `orbit repo-map <subcommand> [--repo P] [--ext E]` | Query a high-level repository map |
| `orbit sql [query] [-f file] [-F table\|json\|ndjson\|csv]` | Run read-only SQL |
| `orbit schema [table...] [--raw]` | Describe live tables and columns |
| `orbit list [-F ...]` | List indexed repositories, branches, and commits |
| `orbit mcp serve` | Expose `run_sql`, `get_graph_schema`, and `index` over MCP |
| `orbit skill [path]` | Print bundled skill or reference content |

SQL uses `gl_definition`, `gl_edge`, `gl_file`, `gl_directory`, and
`gl_imported_symbol`. `definition_type` values are capitalized, such as
`Function`, `Method`, and `Struct`. Relationships in `gl_edge` use
`source_id`/`target_id` and `DEFINES`, `CALLS`, `IMPORTS`, `CONTAINS`, or
`EXTENDS`; join IDs back to node tables to resolve names. Inspect the shipped
schema instead of assuming columns: `orbit schema gl_definition gl_edge`.

- [CLI wrapper and configuration reference](references/cli.md)
- [SQL tables and recipes](references/sql.md)
- [`repo-map` workflow and subcommands](references/repo_map.md)
