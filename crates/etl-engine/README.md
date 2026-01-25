# ETL Engine

An async Rust library for building ETL pipelines into Graph. You give it a message broker, some handlers, and a destination. It pulls messages, runs your handlers, writes the results, and acks or nacks based on whether your code succeeded.

## How it works

```
MessageBroker → Engine → Handlers → Destination
```

You implement three things:

1. `MessageBroker` - talks to NATS, Kafka, or whatever you use
2. `Handler` (grouped into `Module`s) - your processing logic
3. `Destination` - writes to ClickHouse, S3, a data lake, etc.

The engine handles the rest: subscribing to topics, managing concurrency, acking messages, and shutting down cleanly.

## Quick start

```rust
use etl_engine::{Engine, EngineConfiguration, ModuleRegistry};

let broker = Box::new(NatsBroker::new(client));
let destination = Arc::new(ClickHouseDestination::new(pool));
let registry = Arc::new(ModuleRegistry::default());

registry.register_module(&MyModule);

let engine = Engine::new(broker, registry, destination);

// Blocks until you call engine.stop() from another task
engine.run(&EngineConfiguration::default()).await?;
```

## Handlers

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

        let writer = context.destination.new_batch_writer(&self.entity());
        writer.write_batch(&[self.to_record_batch(&event)?]).await?;

        Ok(())
    }
}
```

## Modules

Modules group handlers together. They also declare what entities (tables, basically) the handlers produce.

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

## Envelopes

Messages arrive wrapped in an `Envelope`:

```rust
pub struct Envelope {
    pub id: MessageId,            // UUID
    pub payload: Bytes,           // your serialized message
    pub timestamp: DateTime<Utc>, // when it was created
    pub attempt: u32,             // how many times this has been tried
}
```

The engine handles acking for you based on your handler's return value. If you need manual control, there's an `AckHandle` with `ack()` and `nack()`.

## Destinations

A destination creates writers for your storage backend. There are two kinds:

`BatchWriter` writes records in one shot:

```rust
pub trait BatchWriter: Send + Sync {
    async fn write_batch(&self, batches: &[RecordBatch]) -> Result<(), DestinationError>;
}
```

`StreamWriter` keeps a connection open and buffers writes:

```rust
pub trait StreamWriter: Send + Sync {
    async fn write(&mut self, batch: RecordBatch) -> Result<(), DestinationError>;
    async fn flush(&mut self) -> Result<(), DestinationError>;
    async fn close(&mut self) -> Result<(), DestinationError>;
}
```

Here's a ClickHouse example:

```rust
pub struct ClickHouseDestination {
    pool: Pool,
}

impl Destination for ClickHouseDestination {
    fn new_batch_writer(&self, entity: &Entity) -> Box<dyn BatchWriter> {
        Box::new(ClickHouseBatchWriter::new(self.pool.clone(), entity))
    }

    fn new_stream_writer(&self, entity: &Entity) -> Box<dyn StreamWriter> {
        Box::new(ClickHouseStreamWriter::new(self.pool.clone(), entity))
    }
}
```

## Message brokers

Implement `MessageBroker` for your queue:

```rust
pub struct NatsBroker {
    client: async_nats::Client,
}

#[async_trait]
impl MessageBroker for NatsBroker {
    async fn publish(&self, topic: &str, envelope: Envelope) -> Result<(), BrokerError> {
        // publish to NATS subject
    }

    async fn subscribe(&self, topic: &str) -> Result<Subscription, BrokerError> {
        // return a stream of messages from NATS
    }
}
```

## Configuration

`EngineConfiguration` controls concurrency limits. It implements `Serialize` and `Deserialize`, so you can load it from a config file (TOML, JSON, YAML, etc.) and let users tune it without recompiling.

```toml
# config.toml
max_concurrent_workers = 32

[modules.heavy-processing-module]
max_concurrency = 4
```

```rust
let config: EngineConfiguration = toml::from_str(&std::fs::read_to_string("config.toml")?)?;
engine.run(&config).await?;
```

Or build it in code:

```rust
let mut config = EngineConfiguration::default(); // 16 workers
config.max_concurrent_workers = 32;

config.modules.insert(
    "heavy-processing-module".to_string(),
    ModuleConfiguration {
        max_concurrency: Some(4),
    },
);
```

### Why two limits?

The global limit caps total concurrency across the engine, protecting shared resources like CPU and database connections.

Per-module limits exist so you can run multiple indexers in a single pod without one starving the others. Say you run both the SDLC indexer and the code indexer together. You could give each a limit of 4 while keeping the global limit at 6. Neither indexer can monopolize all workers, but both can burst when the other is idle.

If you only need a global limit, skip the per-module config entirely.

## Entities

Entities describe what your handlers produce. There are two kinds:

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

## Testing

The `testkit` module has mocks for everything:

```rust
use etl_engine::testkit::{
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

Shutdown is cooperative: call `engine.stop()` and it finishes in-flight messages before exiting.
