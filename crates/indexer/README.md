# Indexer

Message processing framework and domain modules for the GitLab Knowledge Graph. Consumes events from NATS JetStream, routes them through domain-specific handlers, and writes property graph data to ClickHouse.

## How it works

```plaintext
NatsBroker → Engine → Handlers → ClickHouse
```

The crate has two layers:

1. **Engine** - generic message routing, concurrency control, and destination abstraction
2. **Domain modules** - the actual indexing logic for SDLC entities and code

The engine subscribes to NATS JetStream subscriptions, dispatches messages to handlers, and acks or nacks based on the handler result. Domain modules provide the handlers that transform GitLab events into property graph records.

## Quick start

```rust
use indexer::{IndexerConfig, run};
use tokio_util::sync::CancellationToken;

let config: IndexerConfig = load_config();
let shutdown = CancellationToken::new();

// Blocks until shutdown is triggered
run(&config, shutdown).await?;
```

The `run()` function connects to NATS and ClickHouse, creates SDLC and Code handlers, registers them, and runs the engine.

## Domain modules

### SDLC

Indexes software development lifecycle entities from Siphon CDC events: projects, merge requests, CI pipelines, issues, work items, groups, labels, milestones, notes, and security findings.

### Code

Indexes git repositories via the Rails internal API. Fetches archives on code indexing tasks, runs the code-graph to extract call graphs, definitions, and references, then writes results to ClickHouse.

## Engine internals

### Handlers

A handler listens on one subscription and processes messages from it. Return `Ok(())` to ack, return an error to nack (the message gets redelivered). Each handler provides its own `engine_config()` controlling retry policy and concurrency group.

```rust
pub struct UserCreatedHandler;

#[async_trait]
impl Handler for UserCreatedHandler {
    fn name(&self) -> &str {
        "user-created"
    }

    fn subscription(&self) -> Subscription {
        Subscription::new("users", "user.created")
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        &self.config.engine
    }

    async fn handle(
        &self,
        context: HandlerContext,
        envelope: Envelope,
    ) -> Result<(), HandlerError> {
        let event: UserCreatedEvent = envelope.to_event()?;

        let writer = context.destination.new_batch_writer("users").await?;
        writer.write_batch(&[to_record_batch(&event)?]).await?;

        Ok(())
    }
}
```

Handlers are registered directly in a `HandlerRegistry`:

```rust
let registry = HandlerRegistry::default();
registry.register_handler(Box::new(UserCreatedHandler::new(config)));
```

### Envelopes

Messages arrive wrapped in an `Envelope`:

```rust
pub struct Envelope {
    pub id: MessageId,            // UUID
    pub payload: Bytes,           // your serialized message
    pub timestamp: DateTime<Utc>, // when it was created
    pub attempt: u32,             // how many times this has been tried
}
```

The engine handles acking based on the handler's return value. For manual control, use the `AckHandle` with `ack()` and `nack()`.

### Destinations

A destination creates batch writers for a storage backend:

```rust
#[async_trait]
pub trait BatchWriter: Send + Sync {
    async fn write_batch(&self, batches: &[RecordBatch]) -> Result<(), DestinationError>;
}

#[async_trait]
pub trait Destination: Send + Sync {
    async fn new_batch_writer(&self, table: &str) -> Result<Box<dyn BatchWriter>, DestinationError>;
}
```

### ClickHouse

A ClickHouse destination is included:

```rust
use std::sync::Arc;
use indexer::clickhouse::{ClickHouseConfiguration, ClickHouseDestination};
use indexer::metrics::EngineMetrics;

let config = ClickHouseConfiguration {
    database: "analytics".to_string(),
    url: "127.0.0.1:9000".to_string(),
    username: "default".to_string(),
    password: None,
};

let destination = ClickHouseDestination::new(config, Arc::new(EngineMetrics::default()))?;
let writer = destination.new_batch_writer("users").await?;
writer.write_batch(&batches).await?;
```

## Configuration

`IndexerConfig` holds all settings:

```rust
pub struct IndexerConfig {
    pub nats: NatsConfiguration,
    pub graph: ClickHouseConfiguration,
    pub datalake: ClickHouseConfiguration,
    pub engine: EngineConfiguration,
    pub gitlab: Option<GitlabClientConfiguration>,
}
```

`EngineConfiguration` controls concurrency and per-handler settings:

```toml
# config.toml
max_concurrent_workers = 16

[concurrency_groups]
sdlc = 12
code = 4

[handlers.global-handler]
concurrency_group = "sdlc"
max_attempts = 1

[handlers.code-indexing-task]
concurrency_group = "code"
max_attempts = 5
retry_interval_secs = 60
```

### Why two concurrency levels?

The global limit (`max_concurrent_workers`) caps total concurrency across the engine, protecting shared resources like CPU and database connections.

Per-handler concurrency groups let you run multiple handler types in a single pod without one starving the others. For example, give the SDLC and Code handlers each a group limit so neither can monopolize all workers, but both can burst when the other is idle.

If you only need a global limit, skip the concurrency group config.

## Errors

Three error types, nested:

- `EngineError` wraps either `BrokerError` or `HandlerError`
- `HandlerError` has `Processing(String)` and `Deserialization(serde_json::Error)`
- `BrokerError` has variants for publish, subscribe, ack, nack, connection issues, etc.

When a handler returns an error, the engine nacks the message so the broker can redeliver it (if retries are configured for that handler). When retries are exhausted, the outcome depends on the subscription's `dead_letter_on_exhaustion` setting:

- **`dead_letter_on_exhaustion: true`** (e.g. Siphon CDC subscriptions): the message is published to the `GKG_DEAD_LETTERS` stream for inspection and replay, then acked. If the DLQ publish fails, the message is nacked for redelivery instead.
- **`dead_letter_on_exhaustion: false`** (default, used by internal dispatch subscriptions): the message is term-acked, since it will be regenerated on the next dispatch cycle.

`IndexerError` wraps top-level failures: NATS connection, ClickHouse connection, engine errors, and handler initialization.

## Testing

The `testkit` module has mocks for everything:

```rust
use indexer::testkit::{
    MockDestination,
    TestEngineBuilder,
    TestEnvelopeFactory,
};

#[tokio::test]
async fn test_user_handler() {
    let (engine, config) = TestEngineBuilder::new(broker)
        .with_handler(Box::new(UserCreatedHandler::new()))
        .build();

    // publish message, run engine, assert writes...
}
```

## Threading model

Everything is async and runs on tokio. All traits require `Send + Sync`. The registry, destination, and handlers are shared via `Arc`.

Shutdown is cooperative: cancel the `CancellationToken` passed to `run()` and the engine finishes in-flight messages before exiting.
