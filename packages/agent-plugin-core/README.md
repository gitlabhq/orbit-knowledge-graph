# @orbit/agent-plugin-core

The logic the GitLab Orbit agent plugins share: the [Claude Code plugin](../claude-code-plugin) and the [OpenCode plugin](../opencode-plugin).

- `src/command.ts`: finds each `glab orbit` or standalone `orbit` call in a shell command. It splits the command at unquoted `;`, `&`, `|`, `(`, `)`, and newlines, so Orbit named inside an argument is not a call.
- `src/calls.ts`: what an Orbit call shows, its finished and running labels and what it read. Labels come from [`config/setup/call_labels.yaml`](../../config/setup/call_labels.yaml), which a Rust test checks against the CLI's commands and Orbit's tools.
- `src/cypher.ts`: the openCypher tokenizer, clause layout, entity paths, and hop count.
- `src/stats.ts`: parses Orbit responses into node, edge, and API call counts.
- `src/tally.ts`: a session's running counts.

```shell
bun test
bun run typecheck
```
