---
name: sessions-graph
description: |
  Build and query a local knowledge graph of AI coding sessions.
  Register sessions, link related work, tag topics, create dynamic
  nodes, and search for context. The graph persists in DuckDB at
  ~/.orbit/sessions.duckdb.
allowed-tools:
  - Bash(orbit sessions *)
---

# Session Knowledge Graph

Build working memory by maintaining a local knowledge graph of coding sessions.
The graph is fully dynamic — create any node kind (Session, Concept, Decision,
Bug, Pattern) and any edge type.

## At session start

Register the current session using context from the SessionStart hook:

```bash
orbit sessions register \
  --id <SESSION_ID> \
  --tool claude \
  --project "$(pwd)" \
  --title "<summarize first user message>" \
  --model <MODEL> \
  --git-branch "$(git branch --show-current 2>/dev/null || echo unknown)" \
  --filepath <TRANSCRIPT_PATH>
```

Then find relevant prior sessions:

```bash
orbit sessions context --project "$(pwd)" --json
orbit sessions search "<topic>" --format json
```

## Link related sessions

When you discover this session relates to prior work:

```bash
orbit sessions link <current-session-id> <prior-session-id> \
  --reason "continuation of auth refactor" \
  --link-type continuation
```

Link types: `related`, `continuation`, `follow_up`, `depends_on`

## Tag sessions with topics

```bash
orbit sessions tag <session-id> "knowledge-graph"
orbit sessions tag <session-id> "query-optimization"
```

## Create dynamic nodes

Capture concepts, decisions, or patterns as graph nodes:

```bash
orbit sessions node create --kind Decision \
  --props '{"title":"Use DuckDB for local graph","rationale":"Single-file embedded DB, native JSON, recursive CTEs"}'

orbit sessions node create --kind Concept \
  --props '{"name":"SIP Pre-filter","description":"Sideways Information Passing optimization"}'

orbit sessions edge create --from <session-id> --to <node-id> --rel MADE_DECISION
```

## Update session after work

```bash
orbit sessions update <session-id> \
  --summary "Implemented DuckDB session graph with dynamic property graph"
```

## Query with graph DSL

```bash
orbit sessions query --json '{
  "query_type": "search",
  "node": {"id": "s", "entity": "Session", "columns": ["id", "tool", "title", "project"]},
  "limit": 10
}'
```

## Traverse related nodes

```bash
orbit sessions traverse <session-id> --depth 2 --format json
```

## Other commands

```bash
orbit sessions list [--kind Session] [-n 15] [--format json]
orbit sessions show <id> [--format json]
orbit sessions search "query" [-n 10] [--kind Session] [--format json]
orbit sessions kinds
orbit sessions stats
orbit sessions node update <id> --props '{"key":"value"}'
orbit sessions node delete <id>
orbit sessions edge delete --from <id> --to <id> --rel LINKED_TO
orbit sessions edge list <id>
orbit sessions export ~/session-backup           # Parquet backup
orbit sessions import ~/session-backup           # restore from backup
orbit sessions reindex                           # rebuild FTS index
```

## Data safety

Export your graph to Parquet before risky operations:

```bash
orbit sessions export ~/.orbit/backups/$(date +%Y%m%d)
```

## When to use

- **Session start**: Register current session, search for relevant prior sessions
- **During work**: Link to related sessions, create Concept/Decision nodes
- **End of session**: Update summary, tag final topics
- **Before major changes**: Search for prior sessions that touched the same area
