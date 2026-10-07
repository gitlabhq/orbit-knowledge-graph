# Orbit plugin

Teaches agents to use `orbit grep` and `orbit context` for code search and callers.
Install [Orbit CLI](https://docs.gitlab.com/orbit/local/access/cli/) and index your
repository first. The plugin does not download binaries, index code, or enable MCP.

The plugin ships the `orbit-wrapper` skill, which loads the skill from the installed
CLI with `glab orbit skills get orbit`, so its guidance always matches your binary.
Its hook reminds the agent to use `orbit grep` when it reaches for grep or rg. The
hook runs `orbit` when it is on `PATH`, and `glab orbit` otherwise.

In Claude Code's default permission mode, Claude Code asks you to approve the
skill the first time it runs. To skip the prompt, for example with `claude -p`,
allow it in `settings.json`:

```json
{ "permissions": { "allow": ["Skill(orbit:orbit-wrapper)"] } }
```

## Install

Claude Code:

```shell
claude plugin marketplace add https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
claude plugin install orbit@gitlab-orbit
```

Codex:

```shell
codex plugin marketplace add https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
codex plugin add orbit@gitlab-orbit
```

For a checkout or extracted release archive, replace the Git URL above with `.`.
From that directory, Pi can install the same bundle:

```shell
pi install ./plugins/orbit
```

Keep the directory while the Pi package is installed. OpenCode uses CLI setup:

```shell
orbit setup opencode --no-index
```

Restart your agent after installation. Use one install method per agent.
When replacing `orbit setup`, preview `orbit uninstall <agent> --dry-run` in the
same scope (`--project` for project setup). Uninstall removes managed MCP config
and shared skills. Rerun setup for other agents that still need those skills.
Review any files uninstall keeps for duplicate Orbit instructions or hooks.

## Develop

Run `mise test:plugins` and `mise plugins:package` from the checkout.
Try a single session with `claude --plugin-dir ./plugins/orbit` or
`pi -e ./plugins/orbit`. `skills/orbit-wrapper/SKILL.md` is a copy of
`skills/orbit-wrapper/SKILL.md` at the repository root, because plugins cannot
reference files outside their directory. `mise test:plugins` fails when the two
differ. Bump both plugin manifest versions when the plugin changes.
