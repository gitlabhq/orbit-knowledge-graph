# ETL Engine

Message processing framework for the GitLab Knowledge Graph. Consumes events from NATS JetStream, routes them through handlers, and writes to destinations.

## Architecture

```
NATS JetStream → Engine → Handler Registry → Destination
                    ↓
              Worker Pool
```

### Key components

| Component | File | Purpose |
|-----------|------|---------|
| Engine | `engine.rs` | Message dispatch and lifecycle |
| ModuleRegistry | `module.rs` | Handler-to-topic routing |
| NatsBroker | `nats/broker.rs` | JetStream connection |
| WorkerPool | `worker_pool.rs` | Concurrency control |
| Destination | `destination.rs` | Output abstraction |

### Traits

- **Handler**: Message processor (`name`, `topic`, `handle`)
- **Module**: Groups handlers and entities
- **Destination**: Provides BatchWriter or StreamWriter
- **Event**: Type-safe message serialization

## Development

### Running tests

```bash
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
2. Create handler implementing `Handler`
3. Register in a module's `handlers()` method

### Adding a module

1. Implement `Module` trait
2. Return handlers via `handlers()`
3. Register with `ModuleRegistry::register()`

### Concurrency

- `max_concurrent_workers`: Global limit (default 16)
- Per-module limits configurable via `EngineConfiguration`
