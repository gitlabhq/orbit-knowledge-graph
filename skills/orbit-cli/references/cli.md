# `glab orbit` reference

`glab orbit <command>` runs the Orbit CLI binary (project:
`gitlab-org/orbit/knowledge-graph`, package: `orbit-local`). `glab` downloads,
verifies, caches the binary in `<config-dir>/bin/orbit`, and keeps it up to date.

glab manages the binary on macOS and Linux (x86_64 and aarch64). On Windows,
download the release archive by hand as the Orbit docs describe.

See [`SKILL.md`](../SKILL.md) for guidance on using the local graph.

## First run / install

```bash
# Show the glab wrapper help
glab orbit

# Install the managed binary without running it
glab orbit --install

# Skip all confirmation prompts (for CI/scripts)
glab orbit --install --yes
```

## Update

```bash
# Check for and install the latest compatible version
glab orbit --update
```

`--install` and `--update` are mutually exclusive.

## Pass-through args

glab handles only `--install`, `--update`, `--yes`/`-y`, and `--help`. Every
other argument goes straight to the Orbit binary.

```bash
glab orbit <subcommand> [flags...]
glab orbit --help           # shows this glab wrapper's help
glab orbit help             # shows the orbit binary's top-level help
glab orbit -- --help        # also shows the orbit binary's top-level help
glab orbit index --help     # shows orbit's help for the 'index' subcommand
```

## Configuration

| Config key | Env var | Purpose |
|---|---|---|
| `orbit_local_auto_run` | none | When `true`, skip the "Run the Orbit local CLI?" confirmation prompt. |
| `orbit_local_auto_download` | none | When `true`, skip the "Download the binary?" confirmation prompt. |
| `orbit_local_binary_path` | `GLAB_ORBIT_LOCAL_BINARY_PATH` | Use a custom binary instead of the managed one. Skips download, version checks, and updates. |
| `orbit_local_binary_version` | none | Managed by glab. Installed version, used to detect available updates. |
| `orbit_local_binary_checksum` | none | Managed by glab. Checksum of the installed binary. |
| `orbit_local_last_update_check` | none | Managed by glab. Timestamp of the last background update check. |

Set config keys via `glab config set`:

```bash
glab config set orbit_local_auto_run true
glab config set orbit_local_auto_download true
glab config set orbit_local_binary_path /path/to/custom/orbit
```

## Binary help

The skill content is embedded in the binary, so `orbit skill` serves a copy that
matches the installed version. `orbit <cmd> --help` documents every flag.
