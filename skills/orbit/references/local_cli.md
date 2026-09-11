# `glab orbit` reference

`glab orbit <command>` runs the Orbit CLI binary (project:
`gitlab-org/orbit/knowledge-graph`, package: `orbit-local`). `glab` downloads,
verifies, caches the binary in `<config-dir>/bin/orbit`, and keeps it up to date
automatically.

**Supported platforms:** macOS and Linux (x86_64 and aarch64). Windows is not
supported (the binary is not published for Windows).

See [`SKILL.md`](../SKILL.md) for the commands that use the local graph or Orbit
Remote.

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

`--install` and `--update` are mutually exclusive; passing both returns an error.

## Pass-through args

All arguments that are not `--install`, `--update`, `--yes`/`-y`, or `--help` are
passed directly to the Orbit binary:

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
