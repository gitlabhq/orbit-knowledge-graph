//! Integration tests for the ETL engine.
//!
//! These tests verify the full message flow: NATS -> Handler -> ClickHouse.
//! They require a Docker-compatible runtime (Docker, Colima, etc).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::StreamExt;
use indexer::clickhouse::{ArrowClickHouseClient, ClickHouseConfiguration, ClickHouseDestination};
use indexer::configuration::{EngineConfiguration, ModuleConfiguration};
use indexer::dead_letter::{DEAD_LETTER_STREAM, DeadLetterEnvelope};
use indexer::engine::{Engine, EngineBuilder};
use indexer::entities::Entity;
use indexer::metrics::EngineMetrics;
use indexer::module::{Handler, HandlerContext, HandlerError, Module, ModuleRegistry};
use indexer::nats::{NatsBroker, NatsConfiguration};
use indexer::types::{Envelope, Event, Topic};
use serde::{Deserialize, Serialize};
use testcontainers::GenericImage;
use testcontainers::core::{ContainerPort, ImageExt, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

const STREAM: &str = "test_stream";
const SUBJECT: &str = "test.events";
const TABLE: &str = "processed_events";

const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
const CLICKHOUSE_TAG: &str = "25.12";
const USERNAME: &str = "default";
const PASSWORD: &str = "testpass";
const DATABASE: &str = "test";

fn test_topic() -> Topic {
    Topic::new(STREAM, SUBJECT)
}

fn test_entity() -> Entity {
    Entity::Node {
        name: TABLE.to_string(),
        fields: vec![],
        primary_keys: vec!["id".to_string()],
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestEvent {
    id: i32,
    name: String,
}

impl Event for TestEvent {
    fn topic() -> Topic {
        test_topic()
    }
}

struct TestHandler;

#[async_trait]
impl Handler for TestHandler {
    fn name(&self) -> &str {
        "test-handler"
    }

    fn topic(&self) -> Topic {
        test_topic()
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        let event: TestEvent = message
            .to_event()
            .map_err(|error| HandlerError::Processing(error.to_string()))?;

        let writer = context
            .destination
            .new_batch_writer(TABLE)
            .await
            .map_err(|error| HandlerError::Processing(error.to_string()))?;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![event.id])),
                Arc::new(StringArray::from(vec![event.name.as_str()])),
            ],
        )
        .map_err(|error| HandlerError::Processing(error.to_string()))?;

        writer
            .write_batch(&[batch])
            .await
            .map_err(|error| HandlerError::Processing(error.to_string()))?;

        Ok(())
    }
}

struct TestModule;

impl Module for TestModule {
    fn name(&self) -> &str {
        "test-module"
    }

    fn handlers(&self) -> Vec<Box<dyn Handler>> {
        vec![Box::new(TestHandler)]
    }

    fn entities(&self) -> Vec<Entity> {
        vec![test_entity()]
    }
}

struct SecondModule;

impl Module for SecondModule {
    fn name(&self) -> &str {
        "second-module"
    }

    fn handlers(&self) -> Vec<Box<dyn Handler>> {
        vec![Box::new(TestHandler)]
    }

    fn entities(&self) -> Vec<Entity> {
        vec![]
    }
}

struct TestContext {
    _nats_container: testcontainers::ContainerAsync<Nats>,
    _clickhouse_container: testcontainers::ContainerAsync<GenericImage>,
    nats_url: String,
    clickhouse_endpoint: String,
}

impl TestContext {
    async fn new() -> Self {
        let (nats_container, nats_url) = Self::start_nats().await;
        let (clickhouse_container, clickhouse_endpoint) = Self::start_clickhouse().await;

        Self::create_nats_stream(&nats_url).await;
        Self::setup_clickhouse_table(&clickhouse_endpoint).await;

        Self {
            _nats_container: nats_container,
            _clickhouse_container: clickhouse_container,
            nats_url,
            clickhouse_endpoint,
        }
    }

    async fn start_nats() -> (testcontainers::ContainerAsync<Nats>, String) {
        let container = Nats::default()
            .with_cmd(&NatsServerCmd::default().with_jetstream())
            .with_tag("2.11-alpine")
            .with_mapped_port(0, ContainerPort::Tcp(4222))
            .with_ready_conditions(vec![WaitFor::seconds(3)])
            .start()
            .await
            .expect("failed to start NATS container");

        let host = container.get_host().await.expect("failed to get host");
        let port = container
            .get_host_port_ipv4(4222)
            .await
            .expect("failed to get port");

        (container, format!("{host}:{port}"))
    }

    async fn start_clickhouse() -> (testcontainers::ContainerAsync<GenericImage>, String) {
        let http_port = ContainerPort::Tcp(8123);

        let container = GenericImage::new(CLICKHOUSE_IMAGE, CLICKHOUSE_TAG)
            .with_exposed_port(http_port)
            .with_env_var("CLICKHOUSE_USER", USERNAME)
            .with_env_var("CLICKHOUSE_PASSWORD", PASSWORD)
            .with_env_var("CLICKHOUSE_DB", DATABASE)
            .start()
            .await
            .expect("failed to start ClickHouse container");

        let host = container.get_host().await.expect("failed to get host");
        let port = container
            .get_host_port_ipv4(http_port)
            .await
            .expect("failed to get port");

        let host = if host.to_string() == "localhost" {
            "127.0.0.1".to_string()
        } else {
            host.to_string()
        };

        (container, format!("http://{host}:{port}"))
    }

    async fn create_nats_stream(url: &str) {
        let client = async_nats::connect(format!("nats://{url}"))
            .await
            .expect("failed to connect to NATS");

        async_nats::jetstream::new(client)
            .create_stream(async_nats::jetstream::stream::Config {
                name: STREAM.to_string(),
                subjects: vec![format!("{SUBJECT}.>"), SUBJECT.to_string()],
                ..Default::default()
            })
            .await
            .expect("failed to create stream");
    }

    async fn setup_clickhouse_table(endpoint: &str) {
        let client = Self::connect_clickhouse_with_retry(endpoint).await;
        client
            .execute(&format!(
                "CREATE TABLE IF NOT EXISTS {DATABASE}.{TABLE} (
                    id Int32,
                    name String
                ) ENGINE = MergeTree() ORDER BY id"
            ))
            .await
            .expect("failed to create table");
    }

    async fn connect_clickhouse_with_retry(endpoint: &str) -> ArrowClickHouseClient {
        for attempt in 1..=30 {
            let client = ArrowClickHouseClient::new(endpoint, "default", USERNAME, Some(PASSWORD));

            match client.execute("SELECT 1").await {
                Ok(_) => return client,
                Err(error) if attempt == 30 => {
                    panic!("failed to connect to ClickHouse after 30 attempts: {error}")
                }
                Err(_) => tokio::time::sleep(Duration::from_millis(500)).await,
            }
        }
        unreachable!()
    }

    async fn create_broker(&self) -> Arc<NatsBroker> {
        Arc::new(
            NatsBroker::connect(&NatsConfiguration {
                url: self.nats_url.clone(),
                ..Default::default()
            })
            .await
            .expect("failed to connect to NATS"),
        )
    }

    async fn create_destination(&self) -> Arc<ClickHouseDestination> {
        Arc::new(
            ClickHouseDestination::new(
                ClickHouseConfiguration {
                    database: DATABASE.to_string(),
                    url: self.clickhouse_endpoint.clone(),
                    username: USERNAME.to_string(),
                    password: Some(PASSWORD.to_string()),
                },
                Arc::new(EngineMetrics::default()),
            )
            .expect("failed to create destination"),
        )
    }

    async fn publish_event(&self, broker: &NatsBroker, id: i32, name: &str) {
        let event = TestEvent {
            id,
            name: name.to_string(),
        };
        let envelope = Envelope::new(&event).expect("failed to create envelope");
        broker
            .publish(&test_topic(), &envelope)
            .await
            .expect("failed to publish");
    }

    async fn query_count(&self) -> u64 {
        let client = ArrowClickHouseClient::new(
            &self.clickhouse_endpoint,
            DATABASE,
            USERNAME,
            Some(PASSWORD),
        );

        let batches = client
            .query_arrow(&format!("SELECT count() FROM {TABLE}"))
            .await
            .expect("query failed");

        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("expected UInt64Array")
            .value(0)
    }
}

async fn run_engine_for(engine: Arc<Engine>, duration: Duration) {
    run_engine_with_config(engine, EngineConfiguration::default(), duration).await;
}

async fn run_engine_with_config(
    engine: Arc<Engine>,
    config: EngineConfiguration,
    duration: Duration,
) {
    let engine_handle = engine.clone();
    let task = tokio::spawn(async move {
        engine_handle.run(&config).await.expect("engine failed");
    });

    tokio::time::sleep(duration).await;
    engine.stop();
    task.await.expect("engine task panicked");
}

fn create_engine(
    broker: Arc<NatsBroker>,
    destination: Arc<ClickHouseDestination>,
    module: &dyn Module,
) -> Arc<Engine> {
    let registry = Arc::new(ModuleRegistry::default());
    registry.register_module(module);
    Arc::new(EngineBuilder::new(broker, registry, destination).build())
}

#[tokio::test]
async fn single_message_flows_through_engine() {
    let context = TestContext::new().await;
    let broker = context.create_broker().await;
    let destination = context.create_destination().await;
    let engine = create_engine(broker.clone(), destination, &TestModule);

    context.publish_event(&broker, 1, "alice").await;
    run_engine_for(engine, Duration::from_secs(2)).await;

    assert_eq!(context.query_count().await, 1);
}

#[tokio::test]
async fn multiple_messages_processed() {
    let context = TestContext::new().await;
    let broker = context.create_broker().await;
    let destination = context.create_destination().await;
    let engine = create_engine(broker.clone(), destination, &TestModule);

    for i in 1..=5 {
        context
            .publish_event(&broker, i, &format!("user-{i}"))
            .await;
    }

    run_engine_for(engine, Duration::from_secs(3)).await;

    assert_eq!(context.query_count().await, 5);
}

#[tokio::test]
async fn multiple_handlers_receive_same_message() {
    let context = TestContext::new().await;
    let broker = context.create_broker().await;
    let destination = context.create_destination().await;

    let registry = Arc::new(ModuleRegistry::default());
    registry.register_module(&TestModule);
    registry.register_module(&SecondModule);

    let engine = Arc::new(EngineBuilder::new(broker.clone(), registry, destination).build());

    context.publish_event(&broker, 100, "shared").await;
    run_engine_for(engine, Duration::from_secs(2)).await;

    assert_eq!(context.query_count().await, 2);
}

struct AlwaysFailingHandler;

#[async_trait]
impl Handler for AlwaysFailingHandler {
    fn name(&self) -> &str {
        "always-failing-handler"
    }

    fn topic(&self) -> Topic {
        test_topic()
    }

    async fn handle(
        &self,
        _context: HandlerContext,
        _message: Envelope,
    ) -> Result<(), HandlerError> {
        Err(HandlerError::Processing("simulated failure".into()))
    }
}

struct AlwaysFailingModule;

impl Module for AlwaysFailingModule {
    fn name(&self) -> &str {
        "always-failing-module"
    }

    fn handlers(&self) -> Vec<Box<dyn Handler>> {
        vec![Box::new(AlwaysFailingHandler)]
    }

    fn entities(&self) -> Vec<Entity> {
        vec![]
    }
}

#[tokio::test]
async fn exhausted_message_lands_in_dead_letter_queue() {
    // Start NATS only — no ClickHouse needed since the handler never writes
    let (_nats_container, nats_url) = TestContext::start_nats().await;
    TestContext::create_nats_stream(&nats_url).await;

    // Wire up an engine whose only handler always fails
    let broker = Arc::new(
        NatsBroker::connect(&NatsConfiguration {
            url: nats_url.clone(),
            ..Default::default()
        })
        .await
        .expect("failed to connect to NATS"),
    );
    let registry = Arc::new(ModuleRegistry::default());
    registry.register_module(&AlwaysFailingModule);
    let destination = Arc::new(indexer::testkit::mocks::MockDestination::new());
    let engine = Arc::new(EngineBuilder::new(broker.clone(), registry, destination).build());

    // Publish one message that will fail on every attempt
    let event = TestEvent {
        id: 42,
        name: "doomed".to_string(),
    };
    broker
        .publish(&test_topic(), &Envelope::new(&event).unwrap())
        .await
        .expect("failed to publish event");

    // Run the engine with max_retry_attempts=1 so the first attempt exhausts retries
    let config = EngineConfiguration {
        modules: HashMap::from([(
            "always-failing-module".to_string(),
            ModuleConfiguration {
                max_retry_attempts: Some(1),
                ..Default::default()
            },
        )]),
        ..Default::default()
    };
    run_engine_with_config(engine, config, Duration::from_secs(3)).await;

    // Read the DLQ stream and verify the dead letter envelope
    let nats_client = async_nats::connect(format!("nats://{nats_url}"))
        .await
        .expect("failed to connect to NATS");
    let jetstream = async_nats::jetstream::new(nats_client);
    let mut dlq_stream = jetstream
        .get_stream(DEAD_LETTER_STREAM)
        .await
        .expect("DLQ stream should have been created by ensure_streams");

    let dlq_info = dlq_stream
        .info()
        .await
        .expect("failed to get DLQ stream info");
    assert!(
        dlq_info.state.messages >= 1,
        "expected at least 1 dead letter, got {}",
        dlq_info.state.messages,
    );

    let consumer = dlq_stream
        .create_consumer(async_nats::jetstream::consumer::pull::Config {
            filter_subject: format!("dlq.{STREAM}.{SUBJECT}"),
            ..Default::default()
        })
        .await
        .expect("failed to create DLQ consumer");
    let mut messages = consumer
        .fetch()
        .max_messages(1)
        .messages()
        .await
        .expect("failed to fetch from DLQ");
    let raw = messages
        .next()
        .await
        .expect("DLQ should contain a message")
        .expect("failed to read DLQ message");

    let dead_letter: DeadLetterEnvelope =
        serde_json::from_slice(&raw.payload).expect("DLQ payload should be valid JSON");

    assert_eq!(dead_letter.original_stream, STREAM);
    assert_eq!(dead_letter.original_subject, SUBJECT);
    assert_eq!(
        dead_letter.original_payload,
        serde_json::json!({"id": 42, "name": "doomed"})
    );
    assert!(dead_letter.last_error.contains("simulated failure"));
}
