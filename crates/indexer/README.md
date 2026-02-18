# Indexer

Message processing framework and domain modules for the GitLab Knowledge Graph. Consumes events from NATS JetStream, routes them through domain-specific handlers, and writes property graph data to ClickHouse.

## How it works

```plaintext
NatsBroker → Engine → Handlers → ClickHouse
```

The crate has two layers:

1. **Engine** - generic message routing, concurrency control, and destination abstraction
2. **Domain modules** - the actual indexing logic for SDLC entities and code

The engine subscribes to NATS topics, dispatches messages to handlers, and acks or nacks based on the handler result. Domain modules provide the handlers that transform GitLab events into property graph records.

## Quick start

```rust
use indexer::{IndexerConfig, run};
use tokio_util::sync::CancellationToken;

let config: IndexerConfig = load_config();
let shutdown = CancellationToken::new();

// Blocks until shutdown is triggered
run(&config, shutdown).await?;
```

The `run()` function connects to NATS and ClickHouse, initializes the SDLC and Code modules, and runs the engine.

## Domain modules

### SDLC

Indexes software development lifecycle entities from Siphon CDC events: projects, merge requests, CI pipelines, issues, work items, groups, labels, milestones, notes, and security findings.

### Code

Indexes git repositories via Gitaly. Fetches archives on push events, runs the code-indexer to extract call graphs, definitions, and references, then writes results to ClickHouse. Requires a Gitaly connection to be configured; disabled otherwise.

## Engine internals

### Handlers

A handler listens to one topic and processes messages from it. Return `Ok(())` to ack, return an error to nack (the message gets redelivered).

```rust
pub struct UserCreatedHandler;

#[async_trait]
impl Handler for UserCreatedHandler {
    fn topic(&self) -> &str {
        "user-events"
    }

    async fn handle(
        &self,
        context: HandlerContext,
        envelope: Envelope,
    ) -> Result<(), HandlerError> {
        let event: UserCreatedEvent = envelope.to_event()?;

        let writer = context.destination.new_batch_writer(&self.entity()).await?;
        writer.write_batch(&[self.to_record_batch(&event)?]).await?;

        Ok(())
    }
}
```

### Modules

Modules group handlers together. They also declare what entities (tables) the handlers produce.

```rust
pub struct UserModule;

impl Module for UserModule {
    fn name(&self) -> &str {
        "user-module"
    }

    fn handlers(&self) -> Vec<Box<dyn Handler>> {
        vec![
            Box::new(UserCreatedHandler),
            Box::new(UserUpdatedHandler),
            Box::new(UserDeletedHandler),
        ]
    }

    fn entities(&self) -> Vec<Entity> {
        vec![
            Entity::Node {
                name: "users".to_string(),
                fields: vec![
                    Field::new("id", DataType::String, false),
                    Field::new("email", DataType::String, false),
                    Field::new("created_at", DataType::DateTime, false),
                ],
                primary_keys: vec!["id".to_string()],
            },
        ]
    }
}
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
    async fn new_batch_writer(&self, entity: &Entity) -> Result<Box<dyn BatchWriter>, DestinationError>;
}
```

### ClickHouse

A ClickHouse destination is included:

```rust
use indexer::clickhouse::{ClickHouseConfiguration, ClickHouseDestination};

let config = ClickHouseConfiguration {
    database: "analytics".to_string(),
    url: "127.0.0.1:9000".to_string(),
    username: "default".to_string(),
    password: None,
};

let destination = ClickHouseDestination::new(config)?;
let writer = destination.new_batch_writer(&entity).await?;
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
    pub gitaly: Option<GitalyConfiguration>,
    pub code_indexing: CodeIndexingConfig,
}
```

`EngineConfiguration` controls concurrency limits. It implements `Serialize` and `Deserialize`, so you can load it from a config file.

```toml
# config.toml
max_concurrent_workers = 32

[modules.heavy-processing-module]
max_concurrency = 4
```

### Why two concurrency limits?

The global limit caps total concurrency across the engine, protecting shared resources like CPU and database connections.

Per-module limits let you run multiple indexers in a single pod without one starving the others. For example, give the SDLC and Code modules each a limit of 4 with a global limit of 6. Neither module can monopolize all workers, but both can burst when the other is idle.

If you only need a global limit, skip the per-module config.

## Entities

Entities describe what handlers produce. There are two kinds:

`Node` is a standalone record (like a ClickHouse table):

```rust
Entity::Node {
    name: "users".to_string(),
    fields: vec![
        Field::new("id", DataType::String, false),
        Field::new("email", DataType::String, false),
        Field::new("age", DataType::Int, true), // nullable
    ],
    primary_keys: vec!["id".to_string()],
}
```

`Edge` is a relationship between nodes:

```rust
Entity::Edge {
    source: "user_id".to_string(),
    source_type: "users".to_string(),
    target: "org_id".to_string(),
    target_type: "organizations".to_string(),
    relationship_type: "belongs_to".to_string(),
}
```

Supported types: `String`, `Int`, `Float`, `Bool`, `DateTime`.

## Errors

Three error types, nested:

- `EngineError` wraps either `BrokerError` or `HandlerError`
- `HandlerError` has `Processing(String)` and `Deserialization(serde_json::Error)`
- `BrokerError` has variants for publish, subscribe, ack, nack, connection issues, etc.

When a handler returns an error, the engine nacks the message so the broker can redeliver it.

`IndexerError` wraps top-level failures: NATS connection, ClickHouse connection, Gitaly configuration, engine errors, and module initialization.

## Testing

The `testkit` module has mocks for everything:

```rust
use indexer::testkit::{
    MockMessageBroker,
    MockDestination,
    TestEngineBuilder,
    TestEnvelopeFactory,
};

#[tokio::test]
async fn test_user_handler() {
    let broker = MockMessageBroker::new();
    let destination = MockDestination::new();

    let envelope = TestEnvelopeFactory::new()
        .with_payload(serde_json::to_vec(&UserCreatedEvent { id: "123" }).unwrap())
        .build();
    broker.queue_message("user-events", envelope);

    let engine = TestEngineBuilder::new()
        .with_broker(broker)
        .with_destination(destination.clone())
        .with_module(UserModule)
        .build();

    engine.run_once().await.unwrap();

    let writes = destination.get_writes("users");
    assert_eq!(writes.len(), 1);
}
```

## Threading model

Everything is async and runs on tokio. All traits require `Send + Sync`. The registry, destination, and handlers are shared via `Arc`.

Shutdown is cooperative: cancel the `CancellationToken` passed to `run()` and the engine finishes in-flight messages before exiting.
