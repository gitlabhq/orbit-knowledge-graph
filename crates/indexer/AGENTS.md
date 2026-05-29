# Indexer

Message processing framework and domain modules for the GitLab Knowledge Graph. Consumes events from NATS JetStream, routes them through handlers, and writes property graph data to ClickHouse.

## Architecture

```plaintext
NATS JetStream → Engine → Handler Registry → ClickHouse
                    ↓
              Worker Pool
```

### Engine components

| Component | File | Purpose |
|-----------|------|---------|
| Engine | `engine.rs` | Message dispatch and lifecycle |
| HandlerRegistry | `handler.rs` | Handler-to-topic routing |
| NatsBroker | `nats/broker.rs` | JetStream connection |
| WorkerPool | `worker_pool.rs` | Concurrency control |
| Destination | `destination.rs` | Output abstraction |
| ClickHouse | `clickhouse/` | ClickHouse destination implementation |

### Domain modules

| Module | Directory | Purpose |
|--------|-----------|---------|
| `code::register_handlers` | `modules/code/` | Git repository indexing via Rails internal API, call graph extraction |
| `sdlc::register_handlers` | `modules/sdlc/` | SDLC entity indexing (projects, MRs, CI, issues, etc.) |
| `namespace_deletion::register_handlers` | `modules/namespace_deletion/` | Soft-deletes all graph data for a namespace across ontology-driven tables |

### Traits

- **Handler**: Message processor (`name`, `subscription`, `handle`)
- **Destination**: Provides BatchWriter or StreamWriter
- **Event**: Type-safe message serialization

### Schema migration

The **dispatcher** owns schema migration. At boot, `schema::migration::run_if_needed()` compares
the embedded `SCHEMA_VERSION` with the active version in ClickHouse. On a mismatch, it acquires a
NATS KV distributed lock, generates DDL from the ontology via `generate_graph_tables_with_prefix()`,
creates new-prefix ClickHouse tables, and marks the new version as `migrating`.

Indexers do not run DDL. Before consuming, the indexer calls `schema::version::wait_until_ready()`,
which polls `gkg_schema_version` with backoff until its version is `active`/`migrating`, exiting
non-zero (→ restart) if the budget is exhausted or the binary is outdated. All write paths
(checkpoints, namespace deletion, ontology-driven tables) use
`prefixed_table_name(table, SCHEMA_VERSION)` so they always target the current schema version's
table-set.

### Migration completion and cleanup

`migration_completion::MigrationCompletionChecker` runs as a scheduled task in DispatchIndexing
mode. It detects when all enabled namespaces have been re-indexed into new-prefix tables (by
comparing checkpoint entries against enabled namespaces), promotes the `migrating` version to
`active`, retires the old active version, and drops tables for retired versions outside the
`max_retained_versions` retention window.

### Entry point

The `run()` function in `lib.rs` wires everything together: waits for the schema version to be
ready, connects to NATS and ClickHouse, registers handlers via `sdlc::register_handlers()`,
`code::register_handlers()`, and `namespace_deletion::register_handlers()`, builds the engine,
and runs until shutdown.

`IndexerConfig` holds all configuration (NATS, ClickHouse graph/datalake, engine concurrency, handler configs, GitLab client). Handler configs are typed via `HandlersConfiguration` in `configuration.rs` — no string-keyed lookups.

## Development

### Running tests

```shell
# Unit tests
cargo test --lib

# Integration tests (requires Docker, make sure to look for Colima if docker is not found)
cargo test --test '*'
```

### Test utilities

Located in `testkit/`:

- `MockNatsServices`, `MockDestination`, `MockHandler`
- `TestEngineBuilder` for integration tests
- `TestEnvelopeFactory` for message creation

## Common tasks

### Adding a handler

1. Define event type implementing `Event`
2. Create handler implementing `Handler` (`name`, `subscription`, `handle`)
3. Add topic config to `engine.topics` in `config/default.yaml` for retry/concurrency policy
4. If handler needs domain config, add a typed config field to `HandlersConfiguration` in `engine.rs`
5. Register in `sdlc::register_handlers()`, `code::register_handlers()`, or `namespace_deletion::register_handlers()`

### Concurrency

- `max_concurrent_workers`: Global limit (default 16)
- Per-subscription concurrency groups configured via `engine.topics` in YAML
