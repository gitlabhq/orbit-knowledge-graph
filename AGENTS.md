# AGENTS.md

Main repository for the GitLab Knowledge Graph (aka "gitlab orbit").

## Development

- Run tasks with mise.

## Related repositories

The `/related-repositories` skill lists dependent systems and their local paths:

- GitLab Handbook - design documents at `content/handbook/engineering/architecture/design-documents/gitlab_knowledge_graph`
- Siphon - CDC stream project for PostgreSQL logical replication
- Gitaly - Git RPC service
- GitLab - main GitLab project
- gitlab-zoekt-indexer - Zoekt code search indexer

## Architecture

The Knowledge Graph builds a property graph from GitLab data and exposes it through a JSON-based Cypher-like DSL (full Cypher support planned).

### Data flow

1. PostgreSQL changes stream through Siphon (CDC) into NATS
2. The indexer consumes NATS events and writes property graph tables to ClickHouse
3. For code indexing, the indexer fetches repositories from Gitaly, parses them into call graphs, and stores them in ClickHouse
4. The webserver translates the JSON DSL into ClickHouse SQL and returns results

### What gets indexed

- Code: call graphs, definitions, references, repository metadata
- SDLC: MRs, CI pipelines, issues, work items, groups, projects
- Custom entities (planned): user-defined nodes

### Tech stack

- Siphon streams PostgreSQL logical replication events into NATS
- NATS JetStream brokers messages and handles distributed coordination
- ClickHouse stores the property graph and runs queries via a custom graph engine
