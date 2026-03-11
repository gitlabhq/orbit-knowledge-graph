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
| `code::register_handlers` | `modules/code/` | Git repository indexing via Gitaly, call graph extraction |
| `sdlc::register_handlers` | `modules/sdlc/` | SDLC entity indexing (projects, MRs, CI, issues, etc.) |
| `namespace_deletion::register_handlers` | `modules/namespace_deletion/` | Soft-deletes all graph data for a namespace across ontology-driven tables |

### Traits

- **Handler**: Message processor (`name`, `topic`, `engine_config`, `handle`)
- **Destination**: Provides BatchWriter or StreamWriter
- **Event**: Type-safe message serialization

### Entry point

The `run()` function in `lib.rs` wires everything together: connects to NATS and ClickHouse, registers handlers via `sdlc::register_handlers()`, `code::register_handlers()`, and `namespace_deletion::register_handlers()`, builds the engine, and runs until shutdown.

`IndexerConfig` holds all configuration (NATS, ClickHouse graph/datalake, engine concurrency, handler configs, Gitaly). Handler configs are typed via `HandlersConfiguration` in `configuration.rs` — no string-keyed lookups.

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
2. Create handler implementing `Handler` (including `engine_config()`)
3. Add a typed config field to `HandlersConfiguration` in `configuration.rs`
4. Register in `sdlc::register_handlers()`, `code::register_handlers()`, or `namespace_deletion::register_handlers()`

### Concurrency

- `max_concurrent_workers`: Global limit (default 16)
- Per-handler concurrency groups configurable via `EngineConfiguration`
