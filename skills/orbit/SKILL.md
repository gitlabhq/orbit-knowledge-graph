---
name: orbit
description: Query the GitLab Knowledge Graph (Orbit) via `glab orbit remote` CLI subcommands or run a local copy with `glab orbit local`. Use for code-structure questions (who calls this function, where is this symbol defined), cross-project dependency and blast-radius analysis, merge-request and contributor queries, and any question answerable by traversing GitLab's unified entity graph (projects, users, MRs, issues, pipelines, files, definitions, vulnerabilities).
version: 0.6.0
license: MIT
metadata:
  audience: developers
  keywords: orbit, knowledge-graph, gkg, graph, query, glab
  workflow: ai
---

# Orbit (GitLab Knowledge Graph) skill

Query the GitLab Knowledge Graph (product name **Orbit**) via the typed
`glab orbit remote` CLI subcommands (shipped in glab v1.94.0+).

**Do not use `glab api orbit/*`.** The typed CLI handles the
`Content-Type` header, response framing, and exit codes for you.

## Discovery

`glab orbit remote --help` and `glab orbit remote query --help` are the
authoritative usage references. Pass entity names to `schema` to get scoped
properties — calling `schema` without arguments returns the full ontology
(~28 KB) and is rarely what you want:

```bash
glab orbit remote schema MergeRequest Project   # scoped properties
glab orbit remote tools                         # full DSL JSON Schema
```

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

## References

| Topic | Location |
|---|---|
| Full DSL reference | [`references/query_language.md`](references/query_language.md) |
| Paste-ready bodies per `query_type` | [`references/recipes.md`](references/recipes.md) |
| CLI exit codes (1-5) and common errors | [`references/troubleshooting.md`](references/troubleshooting.md) |

## Local CLI (glab orbit local)

`glab orbit local` downloads, installs, and runs the Orbit local CLI binary
(project: `gitlab-org/orbit/knowledge-graph`, package: `orbit-local`). The binary
is managed for you — verified, cached in `<config-dir>/bin/orbit`, and kept up to
date automatically.

**Supported platforms:** macOS and Linux (x86_64 and aarch64). Windows is not
supported (the binary is not published for Windows).

### First run / install

```bash
# Download and install the managed binary, then run it
glab orbit local

# Install only (do not run)
glab orbit local --install

# Skip all confirmation prompts (for CI/scripts)
glab orbit local --install --yes
```

### Update

```bash
# Check for and install the latest compatible version
glab orbit local --update
```

### Pass-through args

All arguments that are not `--install`, `--update`, `--yes`/`-y`, or `--help` are
passed directly to the Orbit local binary:

```bash
glab orbit local <subcommand> [flags...]
glab orbit local --help        # shows this glab wrapper's help
glab orbit local -- --help     # passes --help through to the orbit binary
```

### Configuration

| Config key | Env var | Purpose |
|---|---|---|
| `orbit_local_auto_run` | — | When `true`, skip the "Run the Orbit local CLI?" confirmation prompt. |
| `orbit_local_auto_download` | — | When `true`, skip the "Download the binary?" confirmation prompt. |
| `orbit_local_binary_path` | `GLAB_ORBIT_LOCAL_BINARY_PATH` | Use a custom/local binary instead of the managed one. Skips download, version checks, and updates. |
| `orbit_local_binary_version` | — | (managed by glab) Installed version; used to detect when updates are available. |
| `orbit_local_binary_checksum` | — | (managed by glab) Checksum of the installed binary for integrity verification. |
| `orbit_local_last_update_check` | — | (managed by glab) Timestamp of the last background update check. |

Set config keys via `glab config set`:

```bash
glab config set orbit_local_auto_run true
glab config set orbit_local_auto_download true
glab config set orbit_local_binary_path /path/to/custom/orbit
```

### When to prefer `glab orbit local` vs `glab orbit remote`

| Scenario | Recommended |
|---|---|
| Query the production GitLab Knowledge Graph | `glab orbit remote` |
| Index a local repository for offline analysis | `glab orbit local` |
| Use the Orbit binary directly without glab wrappers | `glab orbit local --` |

## Contributing

`references/query_language.md` is synced from
`docs/source/remote/queries/query-language.md`. Edit the upstream file, then run
`mise run skill:sync:orbit`. The lefthook `orbit-skill-docs-sync` job fails
the commit if the two files drift.
