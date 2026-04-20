---
name: orbit-code-graph
description: Query the local Orbit code graph (DuckDB index built by `orbit index`) to answer structural questions about this repo — where definitions live, what files import what, directory trees. Use instead of rg/Grep when the answer depends on code structure (callers, defs, imports) rather than raw text.
---

# Orbit code graph (local DuckDB)

The `orbit` CLI builds a property graph of this repo into `~/.orbit/graph.duckdb` and answers graph queries against it. Prefer it over text grep for structural questions like *"where is X defined"*, *"what imports Y"*, *"what definitions does file Z contain"*, or *"what's under directory D"*.

## Prerequisites

1. **Binary on PATH**: `which orbit` should print something. If not, build it:
   ```bash
   cargo build --release -p orbit --features duckdb-client/bundled
   ln -sf "$(pwd)/target/release/orbit" "$HOME/.local/bin/orbit"
   ```

2. **Repo indexed**: `orbit index . --v2` (takes seconds for most repos; adds rows to `~/.orbit/graph.duckdb`). Worktrees under the repo are auto-discovered and indexed as separate rows. Re-run whenever you've made file changes you want the graph to reflect.

## Step 1 — discover the schema, always

Do not guess entity or column names. Run this first:

```bash
orbit schema                            # 4 entities + 3 edges, condensed
orbit schema --expand File              # props + edges for File
orbit schema --expand '*'               # full detail for everything local
orbit schema --raw                      # parseable JSON
```

Local scope has 4 entities (`Directory`, `File`, `Definition`, `ImportedSymbol`) and 3 edges (`CONTAINS`, `DEFINES`, `IMPORTS`). `--all` shows the full server ontology — useful only when you're reasoning about what the server can do, not about the local index.

**Gotcha:** `Definition` has `file_path` (not `path`). `File` has `path` (and `name`, `language`). Always `--expand` before writing queries.

## Step 2 — check freshness before trusting answers

```bash
duckdb ~/.orbit/graph.duckdb -c \
  "SELECT repo_path, branch, substr(commit_sha,1,10), status, last_indexed_at \
   FROM _orbit_manifest WHERE repo_path LIKE '%knowledge-graph%' \
   ORDER BY last_indexed_at DESC NULLS LAST;"
```

If `last_indexed_at` is older than recent edits the user mentioned, propose `orbit index .` before answering. The manifest is not exposed through `orbit query` (not an ontology entity) — duckdb CLI is the only way to read it.

## Step 3 — query via `orbit query`

Filter syntax: `{"op":"<op>","value":<v>}`. Valid ops: `eq`, `gt`, `lt`, `gte`, `lte`, `in`, `contains`, `starts_with`, `ends_with`, `is_null`, `is_not_null`.

**Output**: default is Goon text (LLM-friendly). Use `--raw` for JSON you can pipe to `jq`.

### Copy-pasteable templates

Find a definition by name (e.g. struct/fn/class):
```bash
orbit query --raw '{
  "query_type":"search",
  "node":{"id":"d","entity":"Definition",
          "filters":{"name":{"op":"eq","value":"RepositoryIndexer"}},
          "columns":["name","definition_type","file_path","start_line"]},
  "limit":20
}'
```

All definitions in one file (File → DEFINES → Definition):
```bash
orbit query --raw '{
  "query_type":"traversal",
  "nodes":[
    {"id":"f","entity":"File",
     "filters":{"path":{"op":"ends_with","value":"crates/cli/src/main.rs"}},
     "columns":["path"]},
    {"id":"d","entity":"Definition","columns":["name","definition_type","start_line"]}
  ],
  "relationships":[{"type":"DEFINES","from":"f","to":"d"}],
  "limit":200
}'
```

What a file imports (File → IMPORTS → ImportedSymbol/Definition/File):
```bash
orbit query --raw '{
  "query_type":"traversal",
  "nodes":[
    {"id":"f","entity":"File",
     "filters":{"path":{"op":"ends_with","value":"crates/ontology/src/lib.rs"}}},
    {"id":"i","entity":"ImportedSymbol","columns":["name","module_path"]}
  ],
  "relationships":[{"type":"IMPORTS","from":"f","to":"i"}],
  "limit":200
}'
```

Files under a directory path:
```bash
orbit query --raw '{
  "query_type":"search",
  "node":{"id":"f","entity":"File",
          "filters":{"path":{"op":"contains","value":"crates/cli/src/"}},
          "columns":["path","language"]},
  "limit":50
}'
```

Count files by language (post-process with jq):
```bash
orbit query --raw '{
  "query_type":"search",
  "node":{"id":"f","entity":"File","columns":["language"]},
  "limit":1000
}' | jq -r '.nodes[].language' | sort | uniq -c | sort -rn
```

## Step 4 — escape hatches when the DSL doesn't fit

**Inspect generated SQL** without running it:
```bash
orbit compile --local '<same JSON>'
```

**Direct DuckDB** when the DSL can't express it (aggregates across manifest, cross-repo joins, custom projections). Tables are prefixed `gl_` — `gl_file`, `gl_definition`, `gl_directory`, `gl_imported_symbol`, `gl_edge`. The `_orbit_manifest` table tracks which repos/branches have been indexed.

```bash
duckdb ~/.orbit/graph.duckdb -c \
  "SELECT language, COUNT(*) n FROM gl_file GROUP BY language ORDER BY n DESC;"
```

## Gotchas observed in practice

- **Filter syntax is `{"op":"eq","value":X}` not `{"eq":X}`.** The error message for `{"eq":X}` is `"schema violation: ... not valid under any of the schemas listed in the 'oneOf' keyword"` — cryptic. Always use the `op/value` shape.

- **`Definition` has no `path` column.** It has `file_path`. Running `orbit schema --expand Definition` tells you the truth in under a second — always do that before composing a query.

- **`orbit index --stats` currently reports zeros** for counts (`directories:0, files:0, definitions:0...`) even when indexing succeeds. Known bug (MR !949 thread). Don't rely on `--stats` to verify indexing worked — query the data instead: `duckdb ~/.orbit/graph.duckdb -c "SELECT COUNT(*) FROM gl_file;"`.

- **Worktrees under the repo are auto-discovered.** `orbit index /path/to/repo` indexes the main checkout *and* any `.claude/worktrees/` or `git worktree` subfolders it finds, as separate rows in `_orbit_manifest` with distinct `commit_sha`. Good for cross-worktree queries; surprising if you expected one index.

- **Unsupported languages silently skip.** v2 output like `[v2] rust: not supported, skipping 507 files` means those files are absent from the graph. Rust v2 support landed in MR !949 (`d1041af4` on origin/main, 2026-04-20); if your local `main` is behind, Rust files won't be indexed. `git fetch origin main` to get the latest v2 language support.

- **Server-only entities (`User`, `MergeRequest`, `Pipeline`, `WorkItem`, etc.) don't exist locally.** If a question requires SDLC/CI data, this skill cannot answer it — that lives in the hosted GKG server against ClickHouse, not the local DuckDB.

- **Error messages list the full server ontology.** `allowlist rejected: "Foo" is not one of ...` enumerates all 24 entities, including ones that aren't queryable locally. Filter what's plausible by what `orbit schema` showed.

## When not to use this

- Pure text search ("find the string `FIXME`"): use `rg`/Grep. The graph doesn't index arbitrary source text.
- Cross-repo questions about SDLC data (MRs, pipelines, users, work items): hosted GKG/gRPC only.
- Very recent edits the user just made: re-index first, or fall back to Grep for that specific file.

## Pointers

- DSL schema: `config/schemas/graph_query.schema.json`
- DSL reference: `docs/source/queries/query_language.md`
- Example queries: `crates/integration-tests/fixtures/queries/cli.json`
- Ontology YAML (source of truth for all entity fields): `config/ontology/nodes/source_code/*.yaml`, `config/ontology/edges/{contains,defines,imports}.yaml`
