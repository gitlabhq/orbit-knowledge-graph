# Orbit CLI plugin

Orbit CLI indexes a checkout so a coding agent can find code and follow its
relationships. This plugin teaches Claude Code and Codex to use `orbit grep` and
`orbit context`. Both agents use the same skill files.

Install the [Orbit CLI](https://docs.gitlab.com/orbit/local/access/cli/) first.
The plugin does not install a binary, index a repository, or register an MCP
server. Index each repository when you choose:

```shell
orbit index .
```

## Claude Code

Add the GitLab Orbit marketplace, then install its plugin:

```text
/plugin marketplace add https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
/plugin install orbit@gitlab-orbit
```

Start a new session. Ask a code question, or invoke `/orbit:orbit-cli`.
The plugin adds search and read hooks that call the existing `orbit hook-guard`
command. They do nothing when `orbit` is missing from `PATH`.
The hooks use `sh`; on Windows, use the shell supplied with Git for Windows.

## Codex

Use a Codex release with Agent Plugins support:

```shell
codex plugin marketplace add https://gitlab.com/gitlab-org/orbit/knowledge-graph.git
```

Open the plugin browser and install Orbit from GitLab Orbit. Start a new session
and ask it to use Orbit to find code and its callers. The Codex package loads the
skill without Claude's hooks. The Codex IDE extension does not support plugins.
Use `orbit setup codex` where plugins are not available.

## Local install and release archive

A checkout is also a local marketplace. From its root:

```shell
claude plugin marketplace add .
claude plugin install orbit@gitlab-orbit
codex plugin marketplace add .
```

For a single Claude session, use `claude --plugin-dir ./plugins/orbit` instead.
To use a release archive, extract `orbit-agent-plugin.zip` into a directory and
run the local marketplace commands there. Keep that directory while the
marketplace is registered. The archive contains the plugin and both catalogs.

For other agents, keep using `orbit setup`. The binary embeds the same local
skill that this plugin ships.

## Switch from orbit setup

Use one install method per agent. Enabling the plugin alongside an existing
setup can load the skill and Claude hooks twice.

Preview removal of the old setup before you enable the plugin:

```shell
orbit uninstall claude codex --dry-run
```

Review the paths, then run `orbit uninstall claude codex` to confirm removal.
Add `--project` if you previously used project-scoped setup. Check both scopes
when both were configured. Uninstall also removes the managed MCP entry.
The skill directory is shared with other agents; rerun setup for any other agent
that still needs it. Keep any edited files the uninstall report preserves under
review, because those can still contain Orbit instructions or hooks.

To switch back, disable the plugin in the agent's plugin manager, then run
`orbit setup` for that agent. Neither path changes the local graph.

## Develop and release

From the knowledge-graph checkout:

```shell
mise test:plugins
mise plugins:package
mise exec -- claude plugin validate plugins/orbit --strict
mise exec -- claude plugin validate .claude-plugin/marketplace.json --strict
```

The package has no files outside its own directory. Keep the canonical local
skill under `plugins/orbit/skills/orbit-cli`; do not add a second copy.
Bump its version and both plugin manifest versions together when the package
changes. CI checks their equality and the skill version increase.

Tag releases publish the archive and its SHA-256 checksum alongside the Orbit
binary assets. Marketplace installs use the repository version. Directory
listing and vendor approval are separate from publishing these files.

For a behavior check, use an indexed fixture with one function calling another.
Ask each agent to find the function and identify its callers. Check that it uses
`orbit grep` followed by `orbit context`, and that the named caller is correct.
Repeat without an index and without the binary. The agent should explain setup,
not download or index without permission.

## References

- [Claude Code plugins](https://code.claude.com/docs/en/plugins)
- [OpenAI plugin packaging](https://developers.openai.com/plugins/build/plugins)
