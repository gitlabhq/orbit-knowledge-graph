# gitlab-orbit Claude Code plugin

A Claude Code [mod](https://code.claude.com/docs/en/plugins/mods/overview) that shows GitLab Orbit calls in the transcript, like the Orbit tool rows in GitLab Duo Chat.

- **Finished calls**: `◎ Explored Orbit  User → MergeRequest → Project`. The mark and the label are Orbit blue, `#949dcb`, and the entity path is grey. A failed call shows a red mark and `· failed`.
- **Running calls**: the mark and the label fade through `#b5dedc`, `#949dcb`, and `#6851ac` and back every 2 seconds. The entity path stays grey. The spinner stays Claude Code's own.
- **One row per run**: back-to-back Orbit calls share one row that shows the current call. When the run finishes, a run with queries reads `Explored Orbit` with every entity path it walked; any other run shows its last call.
- **ctrl+o**: an expanded Orbit query shows one clause per line with openCypher highlighting, above Claude Code's own row.
- **Status line**: after the first Orbit call, the session's calls, nodes, and edges show right-aligned under the prompt hint, ending with `◎ GitLab Orbit`.

The plugin only changes what Claude Code draws. Its `tool.call` hook counts Orbit calls for the status line and passes every call through unchanged.

## What counts as an Orbit call

- `Bash` calls that run `glab orbit <subcommand>`. The label follows the subcommand, for example `query` shows Explored Orbit, `grep` shows Searched code, `context` shows Read code, and `skills get` shows Read Orbit guide.
- MCP tools whose server or name mentions Orbit, such as `list_commands`, `invoke_command`, `query_graph`, and `get_graph_schema`.

## Requirements

- Claude Code v2.1.287 or later, the first version that loads mods.
- A terminal with 24-bit color for a smooth fade. A 256-color terminal shows the fade in steps.

## Try it

```shell
claude --plugin-dir config/setup/claude_plugin/gitlab-orbit
```

Run `claude plugin validate config/setup/claude_plugin/gitlab-orbit` to list the events the plugin handles and the calls it makes.

## Files

- `hooks/register.js`: the hooks. `tool.call` counts Orbit calls, and `ui.render` draws `ToolUse`, `ToolGroup`, and `PromptHint`.
- `hooks/orbit.js`: the colors, the mark, and how a tool call maps to a label.
- `hooks/orbit-mark.js`: the animated row for a running call.
- `hooks/cypher.js`: the openCypher tokenizer and clause layout for the ctrl+o view.
- `hooks/stats.js`: parses Orbit responses for the status line.
