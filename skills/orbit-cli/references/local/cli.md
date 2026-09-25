# `glab orbit` reference

`glab orbit <command>` runs the Orbit CLI binary. glab downloads, caches, and
updates it, and passes your GitLab credential on each call. It runs on macOS,
Linux, and Windows.

glab owns `--install`, `--update`, and `--yes`. It forwards every other
argument to the binary. Run `glab help orbit` for the wrapper. Run
`glab orbit --help` for the binary.

```shell
glab orbit --install --yes    # install the managed binary
glab orbit --update           # update to the latest compatible version
glab orbit <subcommand> [flags...]
```

## Configuration

| Config key | Env var | Purpose |
|---|---|---|
| `orbit_cli_auto_run` | none | `true` skips the run confirmation prompt. |
| `orbit_cli_auto_download` | none | `true` skips the download confirmation prompt. |
| `orbit_cli_binary_path` | `GLAB_ORBIT_CLI_BINARY_PATH` | Use a custom binary. Skips download, version checks, and updates. |

Set keys with `glab config set <key> <value>`. Leave
`orbit_cli_binary_version` and `orbit_cli_last_update_check` alone.
`orbit skills` lists available skills. `orbit skills get orbit [path]` prints
the composed skill (`SKILL.md` by default), or a specified file from its tree.
