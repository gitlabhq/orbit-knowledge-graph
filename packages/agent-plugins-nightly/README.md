# Nightly agent plugins

Features the team tries first, with no guarantees. They can change or disappear
between releases. The stable plugins live in `packages/agent-plugins`.

`features.yaml` is the one switch. It lists each feature and turns it on or off
per channel. `ORBIT_NIGHTLY=1 orbit setup` installs the features with
`nightly: true` instead of the stable set; a plain `orbit setup` installs the
features with `stable: true` on top of what stable ships. Setup embeds this
package and builds each channel from the registry, so a flag is the only edit.
Nightly never runs next to stable: Claude Code gets the stable plugin plus the
feature hooks in the same `gitlab-orbit` plugin, and Pi gets the extension.
Switching channels, or running `orbit setup` again after a flip, removes files
of features that are off, unless you edited them.

| Feature | Claude Code | Pi |
| --- | --- | --- |
| Output guard: cuts a large Bash result to its head and tail and keeps the full output in a file | `output-guard/claude.mjs` | `output-guard/pi.mjs` (also guards `codemode`) |
| Search guard: after an `rg`/`grep` for one identifier, appends Orbit's definition, callers and callees | `search-guard/claude.mjs` | `search-guard/pi.mjs` |

`ORBIT_OUTPUT_GUARD` sets the limit in characters (default 10000, for example
`20k`) or `off`. The Claude Code hook caps the limit at 29000 characters,
because Claude Code shows only a short preview of a replaced result over about
30,000.

The Claude Code hook runs with `node`, so `node` must be on your `PATH`. The
guard leaves output that another guard already cut, such as the orbitcode one,
alone, so the two can run side by side. Any guard failure passes the result
through, and the Pi guard leaves nested calls made by a `codemode` script
alone because the script needs their full output. A script that raises
`max_output_tokens` raises the limit with it.

The search guard reads only an index that already exists: every Orbit call passes `--no-index`, so
a lookup never builds or refreshes one, and without an index of the current commit it adds nothing.
The whole lookup (git, index listing, `grep`, `context`) shares one budget, 1500 ms by default;
`ORBIT_SEARCH_GUARD_TIMEOUT_MS` changes it, capped at 4000. A timeout or failure stops lookups for
the rest of the session, and `ORBIT_SEARCH_GUARD=off` disables the guard. A graph that an index
run holds locked is not waited for: the lookup adds nothing and the next search tries again. The
Claude Code hook keeps its per-session state in the plugin data directory (or `~/.cache/orbit`),
not in the shared temp directory. The Claude Code hook tries
`glab orbit` and then `orbit`; Pi uses the launcher that setup chose. It parses the `grep` definition
header and the `context` `Connections (N indexed):` block, which `crates/orbit-cli/tests/no_index.rs`
pins against the real CLI.

Per assistant, a feature lists what it ships in `features.yaml`: `claude` merges
hooks into the Claude Code plugin and copies files under `features/`; `pi`
copies files into `orbit-<feature>` under the Pi extension directory.

## Adding a feature

Put the sources in this package and add an entry to `features.yaml` with its
channels and, per assistant, the files and Claude Code hooks it ships. Nothing
else lists features. Existing stable features can become entries the same way
later.

## Promoting or reverting

To promote a feature, set `stable: true`. Nightly replaces stable, so a feature
that is on for stable must be on for nightly: `stable: true` with
`nightly: false` fails the tests. To revert a feature, set both flags to
`false`; setup then removes its unedited files from every install. Delete the
entry only after a release that shipped with both flags `false`, because setup
learns a feature's file paths from its entry. Each Pi feature installs as its
own extension, `orbit-<feature>`, so features never share files.

Commits scoped `agent-plugins-nightly` never cut a release, but the release
notes of the next release still list them. A promotion that changes stable
behavior is a normal `feat(agent-plugins)` commit.
