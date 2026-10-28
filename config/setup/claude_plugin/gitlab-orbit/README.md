# gitlab-orbit Claude Code plugin

A Claude Code [mod](https://code.claude.com/docs/en/plugins/mods/overview) that shows GitLab Orbit calls in the transcript, like the Orbit tool rows in GitLab Duo Chat.

- **Finished calls**: `◎ Explored Orbit  User → MergeRequest → Project`. The mark and the label are Orbit blue, `#949dcb`, and the entity path is grey. A failed call shows a red mark and `· failed`.
- **Entity paths**: the data a query explores, one path per `MATCH` pattern, read from source to target. Repeated node types collapse into one, so a call graph query reads `Definition`. A pattern with arrows both ways splits into separate paths, `—` marks an undirected relationship, and distinct paths are joined with commas.
- **Running calls**: the mark and the label fade through `#b5dedc`, `#949dcb`, and `#6851ac` and back every 2 seconds. The entity path stays grey. The spinner stays Claude Code's own.
- **One row per run**: back-to-back Orbit calls share one row. Connected query paths join into one chain, so `MergeRequest → Project` and then `User → MergeRequest` read `User → MergeRequest → Project`. A query that doesn't connect starts its own row.
- **ctrl+o**: an expanded Orbit query shows one clause per line with openCypher highlighting, above Claude Code's own row.
- **Cypher in replies**: a ```` ```cypher ````, ```` ```opencypher ````, or ```` ```gql ```` block in Claude's reply draws with the same highlighting, in the author's own lines. Comments (`//`, `/* */`, and a line that starts with `-- `) are dim. Claude Code draws the prose before the first block.
- **Status line**: after the first Orbit call, `3 queries  ·  536 / 1,675,400 nodes total  ◎ GitLab Orbit` shows right-aligned under the prompt hint. The first number is the nodes this session's queries returned or counted. The total is the project's whole graph, read once per session from `glab orbit graph-status` for the repository Claude Code runs in. Without an indexed project, the line shows the session's nodes alone. Edges aren't shown, because `graph-status` reports no edge counts.

The plugin only changes what Claude Code draws. Its `tool.call` hook counts Orbit calls for the status line and passes every call through unchanged.

## What counts as an Orbit call

- `Bash` calls that run `glab orbit <subcommand>`. The label follows the subcommand, for example `query` shows Explored Orbit, `grep` shows Searched code, `context` shows Read code, and `skills get` shows Read Orbit guide.
- MCP tools whose server or name mentions Orbit, such as `list_commands`, `invoke_command`, `query_graph`, and `get_graph_schema`.

## Requirements

- Claude Code v2.1.287 or later, the first version that loads mods.
- A terminal with 24-bit color for a smooth fade. A 256-color terminal shows the fade in steps.

## Develop

The hooks are TypeScript, which Claude Code loads directly, with no build step.

```shell
claude --plugin-dir config/setup/claude_plugin/gitlab-orbit
mise run test:claude-plugin
```

The first command loads the plugin for one session and reloads it when you save. It also writes the type declarations for your Claude Code version to `.claude-plugin/types/`, which `tsconfig.json` extends and git ignores. Run it once before the second command, which validates the plugin, typechecks it, and runs the tests in `tests/` with `claude plugin test`.

## Files

- `hooks/register.ts`: the hooks. `tool.call` counts Orbit calls, and `ui.render` draws `ToolUse`, `ToolGroup`, and `PromptHint`.
- `hooks/orbit.ts`: the colors, the mark, and how a tool call maps to a label and an entity path.
- `hooks/rows.ts`: how back-to-back calls in a tool group become rows, and how their paths join.
- `hooks/orbit-mark.ts`: the animated row for a running call.
- `hooks/cypher.ts`: the openCypher tokenizer, the clause layout for the ctrl+o view, and code blocks in replies.
- `hooks/stats.ts`: parses Orbit responses for the status line.
- `tests/`: unit tests for the pure modules, and render tests that mount the plugin's `ToolUse` and `PromptHint` sites.
