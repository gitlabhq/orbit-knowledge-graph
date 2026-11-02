# Nightly agent plugins

Features the team tries first, with no guarantees. They can change or disappear
between releases. The stable plugins live in `packages/agent-plugins`.

`ORBIT_NIGHTLY=1 orbit setup` installs the nightly plugins instead of the stable
ones. Nightly never runs next to stable: Claude Code gets the stable plugin plus
the nightly hooks in the same `gitlab-orbit` plugin, and Pi gets the nightly
extension. Run `orbit setup` without the variable to go back; it removes the
nightly files again, unless you edited them.

| Feature | Claude Code | Pi |
| --- | --- | --- |
| Output guard: cuts a large Bash result to its head and tail and keeps the full output in a file | `output-guard/claude.mjs` | `output-guard/pi.mjs` (also guards `codemode`) |

`ORBIT_OUTPUT_GUARD` sets the limit in characters (default 10000, for example
`20k`) or `off`.

## Adding a feature

Put the sources in this package and embed them from
`crates/orbit-cli/src/commands/setup/spec.rs`. Claude Code hooks go into
`claude-hooks.json`. Pi files go into `config/setup/agents/pi.yaml` with
`nightly: true`.

## Promoting or reverting

To promote a feature, port it into the stable plugin in `packages/agent-plugins`
(for Claude Code, its hooks module) and delete it here. To revert one, delete
it here. Commits scoped `agent-plugins-nightly` never cut a release.
