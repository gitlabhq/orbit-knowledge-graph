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
glab orbit local --help        # shows this glab wrapper's help
glab orbit local -- --help     # forwards both '--' and '--help' as raw args to the orbit binary
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
