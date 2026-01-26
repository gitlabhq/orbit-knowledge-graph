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

## Crates

### etl-engine

Message processing framework for the Knowledge Graph. Consumes events from NATS JetStream, routes them through handlers, and writes to destinations:

- Handler and Module traits for message processing
- Two-level concurrency control (global + per-module)
- BatchWriter and StreamWriter destination abstractions
- Comprehensive test utilities in `testkit/`

Integration tests require Docker for NATS testcontainers.

### gitaly-client

Rust gRPC client for Gitaly. Provides repository operations for the code indexer:

- Unix socket and TCP connection support
- HMAC-SHA256 v2 token authentication
- Repository extraction via GetArchive RPC
- `RepositorySource` trait for testing abstraction

Build with: `GITALY_PROTO_ROOT=/path/to/gitaly cargo build -p gitaly-client`

Integration tests require a running Gitaly instance. Set `GITALY_CONNECTION_INFO` JSON with address, storage, and token.

## Infrastructure

See [docs/dev/INFRASTRUCTURE.md](docs/dev/INFRASTRUCTURE.md) for sandbox environment details (GCP project, VMs, networking).

Kubernetes deployments are managed via Helm charts in `./helm-dev/`. The charts are the source of truth for:
- Component configuration (NATS, siphon-producer, siphon-consumer)
- Secret management (External Secrets Operator integration)
- Service connectivity
