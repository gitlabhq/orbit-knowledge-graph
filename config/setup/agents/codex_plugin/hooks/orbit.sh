#!/bin/sh
# Codex waits for PostToolUse hooks after every tool call, and starting Node can take hundreds of
# milliseconds behind a version manager. For a tool call (`calls`, or the async `lookups`), Node starts
# only for input that holds an `orbit` command word, which describeOrbitCall needs for a shell call,
# or an Orbit MCP server or tool name. A working directory such as `src/orbit-knowledge-graph/` alone
# does not start it.
# The hook always exits 0, because Codex reports a failed hook after every tool call: without Node,
# or with a Node that cannot start, it does nothing.
input=$(cat)
command -v node >/dev/null 2>&1 || exit 0
case "${1:-}" in
  calls | lookups)
    printf '%s' "$input" | grep -qE '(^|[^[:alnum:]_.-])orbit($|[^[:alnum:]_./-])' ||
      printf '%s' "$input" | grep -qiE 'mcp__[^"]*(orbit|knowledge[-_]?graph|gkg|query_graph|get_graph_schema)' ||
      exit 0
    ;;
esac
printf '%s' "$input" | node "$(dirname "$0")/orbit.mjs" "${1:-}" 2>/dev/null || exit 0
