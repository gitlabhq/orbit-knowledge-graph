# `glab orbit local` reference

`glab orbit local` downloads, installs, and runs the Orbit local CLI binary
(project: `gitlab-org/orbit/knowledge-graph`, package: `orbit-local`). The binary
is managed for you — verified, cached in `<config-dir>/bin/orbit`, and kept up to
date automatically.

**Supported platforms:** macOS and Linux (x86_64 and aarch64). Windows is not
supported (the binary is not published for Windows).

See [`SKILL.md`](../SKILL.md) for a quick summary and when to prefer `local` over
`remote`.

## First run / install

```bash
# Download and install the managed binary, then run it
glab orbit local

# Install only (do not run)
glab orbit local --install

# Skip all confirmation prompts (for CI/scripts)
glab orbit local --install --yes
```

## Update

```bash
# Check for and install the latest compatible version
glab orbit local --update
```

`--install` and `--update` are mutually exclusive; passing both returns an error.

## Pass-through args

All arguments that are not `--install`, `--update`, `--yes`/`-y`, or `--help` are
passed directly to the Orbit local binary:

```bash
glab orbit local <subcommand> [flags...]
glab orbit local --help           # shows this glab wrapper's help
glab orbit local help             # shows the orbit binary's top-level help
glab orbit local index --help     # shows orbit's help for the 'index' subcommand
```

> **Note:** `glab orbit local -- --help` does **not** show orbit's help. glab forwards
> `--` and `--help` as-is, but orbit's argument parser treats `--` as end-of-flags and
> then sees `--help` as an unknown subcommand name, resulting in an error. Use
> `glab orbit local help` instead.

## Configuration

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

## Binary subcommands and flags

These belong to the `orbit` binary and are the same whether invoked directly or
passed through `glab orbit local`. Shared flag: `--db <PATH>` overrides the
database (default `~/.orbit/graph.duckdb`).

| Command | Flags |
|---|---|
| `index <PATH>` | `-t/--threads <N>` (0 = auto), `-s/--stats` (detailed timings), `-v/--verbose` (stderr logs), `--db` |
| `sql [QUERY]` | positional `QUERY` or `-` for stdin, `-f/--file <PATH>`, `-F/--format table\|json\|ndjson\|csv` (default `table`), `--db` |
| `schema [TABLE…]` | optional table names to scope output, `--raw` (JSON instead of table), `--db` |
| `list` | `-F/--format …`, `--db` |
| `mcp serve` | stateless MCP server over stdio (`run_sql`, `get_graph_schema`, `index`) |
| `skill [PATH]` | print the bundled, version-matched skill content; no arg prints `SKILL.md`, else a relative path such as `references/sql.md` |
| `version` | prints the version string |

The skill content is embedded in the binary, so `orbit skill` serves a copy that
matches the binary version regardless of how it was installed. To run the bundled
`repo_map.py`, write it to a file first:

```bash
orbit skill scripts/repo_map.py > /tmp/repo_map.py
ORBIT_CMD=orbit python3 /tmp/repo_map.py ~/path/to/repo
```

`orbit help` shows the binary's top-level help; `orbit <cmd> --help` shows a
subcommand's. Through the wrapper, `glab orbit local help` reaches the binary
help, but `glab orbit local -- --help` does not (see the note above).
