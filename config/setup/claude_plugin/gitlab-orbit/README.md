# GitLab Orbit plugin for Claude Code

A Claude Code [mod](https://code.claude.com/docs/en/plugins/mods/overview) that shows GitLab Orbit calls in the transcript, like the Orbit tool rows in GitLab Duo Chat.

- **Finished calls**: `◎ Explored Orbit  User → MergeRequest → Project`. The mark and the label are Orbit blue, `#949dcb`, and the entity path is grey. A failed call shows a red mark and `· failed`.
- **Entity paths**: the data a query explores, one path per `MATCH` pattern, read from source to target. Repeated node types collapse into one, so a call graph query reads `Definition`. A pattern with arrows both ways splits into separate paths, `—` marks an undirected relationship, and distinct paths are joined with commas.
- **Running calls**: the mark and the label fade through `#b5dedc`, `#949dcb`, and `#6851ac` and back every 2 seconds. The entity path stays grey. The spinner stays Claude Code's own.
- **One row per run**: back-to-back Orbit calls share one row. Connected query paths join into one chain, so `MergeRequest → Project` and then `User → MergeRequest` read `User → MergeRequest → Project`. A query that doesn't connect starts its own row.
- **ctrl+o**: an expanded Orbit query shows one clause per line with openCypher highlighting, above Claude Code's own row.
- **Cypher in replies**: a ```` ```cypher ````, ```` ```opencypher ````, or ```` ```gql ```` block in Claude's reply draws with the same highlighting, in the author's own lines. Comments (`//`, `/* */`, and a line that starts with `-- `) are dim. Claude Code draws the prose before the first block.
- **Status line**: after the first Orbit call, `3 queries  ·  33 API calls replaced  ·  536 / 1,675,400 nodes  ·  540 edges  ◎ GitLab Orbit` shows right-aligned under the prompt hint. API calls replaced is the fewest GitLab API requests that read the same data, the GraphQL minimum with joins nested: ⌈rows ÷ 100⌉ for a traversal, ⌈records counted ÷ 100⌉ for a grouped count, which has to read every record, 1 for a plain count, and 0 for schema, guide, and local calls. The first number is the nodes this session's queries returned or counted. The total is the project's whole graph, read once per session from `glab orbit graph-status` for the repository Claude Code runs in. Without an indexed project, the line shows the session's nodes alone. Edges aren't shown, because `graph-status` reports no edge counts.

The plugin only changes what Claude Code draws. Its `tool.call` hook counts Orbit calls for the status line and passes every call through unchanged.

## What counts as an Orbit call

- Shell calls that run `glab orbit <subcommand>` or the standalone `orbit <subcommand>`, at the start of a command or after `&&`, `;`, `|`, or a newline. Orbit named inside a quoted argument doesn't count. The labels come from [`config/setup/call_labels.yaml`](../../config/setup/call_labels.yaml). A subcommand without a label there isn't an Orbit row.
- MCP tools whose server or name mentions Orbit, such as `list_commands`, `invoke_command`, `query_graph`, and `get_graph_schema`.

## Requirements

- Claude Code v2.1.287 or later, the first version that loads mods.
- A terminal with 24-bit color for a smooth fade. A 256-color terminal shows the fade in steps.

## Develop

The plugin source lives here, and the logic it shares with the OpenCode plugin lives in [`@orbit/agent-plugin-core`](../agent-plugin-core). A hooks module can import only its own files and `claude-code`, so `bun run build` copies the plugin and the core sources into `dist/` with relative imports, and copies it without tests to `config/setup/claude_plugin/gitlab-orbit`, where `orbit setup --claude-ui` installs from.

```shell
bun run build
claude --plugin-dir packages/claude-code-plugin/dist
mise run test:agent-plugins
```

The second command loads the built plugin for one session. The last one typechecks every agent plugin, runs the core and OpenCode tests, and runs the tests in `tests/` against `dist/` with `claude plugin test`. CI runs everything except `claude plugin test`, and fails when `config/setup` differs from a fresh build.

`types/` holds the Claude Code declarations the typecheck uses. After a Claude Code update, load the plugin once with `claude --plugin-dir .` from this directory, then run `bun run types` to copy the new declarations into `types/`.

## Files

- `hooks/register.ts`: the hooks. `tool.call` counts Orbit calls, and `ui.render` draws `ToolUse`, `ToolGroup`, `PromptHint`, and `AssistantMessage`.
- `hooks/theme.ts`: the colors, the fade, and the Cypher token styles.
- `hooks/rows.ts`: how back-to-back calls in a tool group become rows, and how their paths join.
- `hooks/orbit-mark.ts`: the animated row for a running call.
- `scripts/build.ts`: writes `dist/` and `config/setup/claude_plugin/gitlab-orbit`.
- `tests/`: render tests that mount the plugin's sites, and tests for rows and the theme.
