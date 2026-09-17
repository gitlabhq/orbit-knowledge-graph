# `glab orbit` reference

`glab orbit <command>` runs the Orbit CLI binary (project:
`gitlab-org/orbit/knowledge-graph`, package: `orbit-local`). `glab` downloads,
verifies, caches the binary in `<config-dir>/bin/orbit`, keeps it up to date,
and passes your GitLab credential to it on every call.

glab manages the binary on macOS, Linux, and Windows (x86_64 and aarch64;
Windows ships x86_64 only).

See [`SKILL.md`](../../SKILL.md) for guidance on using the local graph.

## Install and update

```shell
glab orbit --install          # install the managed binary without running it
glab orbit --install --yes    # skip confirmation prompts (CI, scripts)
glab orbit --update           # check for and install the latest compatible version
```

`--install` and `--update` are mutually exclusive. Passing both is an error.

## Pass-through args

glab handles only `--install`, `--update`, and `--yes`/`-y`. Every other
argument, including `--help`, goes straight to the Orbit binary.

```shell
glab orbit <subcommand> [flags...]
glab orbit --help           # the orbit binary's top-level help
glab orbit index --help     # orbit's help for the 'index' subcommand
glab help orbit             # the glab wrapper's own help and flags
```

## Configuration

| Config key | Env var | Purpose |
|---|---|---|
| `orbit_local_auto_run` | none | When `true`, skip the "Run the Orbit local CLI?" confirmation prompt. |
| `orbit_local_auto_download` | none | When `true`, skip the "Download the binary?" confirmation prompt. |
| `orbit_local_binary_path` | `GLAB_ORBIT_LOCAL_BINARY_PATH` | Use a custom binary instead of the managed one. Skips download, version checks, and updates. |

glab also writes `orbit_local_binary_version` and
`orbit_local_last_update_check`. Leave them alone.

Set config keys via `glab config set`:

```shell
glab config set orbit_local_auto_run true
glab config set orbit_local_auto_download true
glab config set orbit_local_binary_path /path/to/custom/orbit
```

## Binary help

The skill content is embedded in the binary, so `orbit skill` serves a copy that
matches the installed version.
