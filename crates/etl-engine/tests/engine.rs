//! Integration tests for the ETL engine.
//!
//! These tests verify the full message flow: NATS -> Handler -> ClickHouse.
//! They require a Docker-compatible runtime (Docker, Colima, etc).

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use clickhouse_arrow::{ArrowClient, ClientBuilder};
use etl_engine::clickhouse::{ClickHouseConfiguration, ClickHouseDestination};
use etl_engine::configuration::EngineConfiguration;
use etl_engine::engine::{Engine, EngineBuilder};
use etl_engine::entities::Entity;
use etl_engine::module::{Handler, HandlerContext, HandlerError, Module, ModuleRegistry};
use etl_engine::nats::{NatsBroker, NatsConfiguration};
use etl_engine::types::{Envelope, Event, Topic};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serial_test::serial;
use testcontainers::GenericImage;
use testcontainers::core::{ContainerPort, ImageExt};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

const STREAM: &str = "test_stream";
const SUBJECT: &str = "test.events";
const TABLE: &str = "processed_events";

const CLICKHOUSE_IMAGE: &str = "clickhouse/clickhouse-server";
const CLICKHOUSE_TAG: &str = "25.11";
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
        let container = GenericImage::new(CLICKHOUSE_IMAGE, CLICKHOUSE_TAG)
            .with_exposed_port(ContainerPort::Tcp(9000))
            .with_env_var("CLICKHOUSE_USER", USERNAME)
            .with_env_var("CLICKHOUSE_PASSWORD", PASSWORD)
            .with_env_var("CLICKHOUSE_DB", DATABASE)
            .start()
            .await
            .expect("failed to start ClickHouse container");

        let host = container.get_host().await.expect("failed to get host");
        let port = container
            .get_host_port_ipv4(9000)
            .await
            .expect("failed to get port");

        let host = if host.to_string() == "localhost" {
            "127.0.0.1".to_string()
        } else {
            host.to_string()
        };

        (container, format!("{host}:{port}"))
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
            .execute(
                format!(
                    "CREATE TABLE IF NOT EXISTS {DATABASE}.{TABLE} (
                        id Int32,
                        name String
                    ) ENGINE = MergeTree() ORDER BY id"
                ),
                None,
            )
            .await
            .expect("failed to create table");
    }

    async fn connect_clickhouse_with_retry(endpoint: &str) -> ArrowClient {
        for attempt in 1..=30 {
            match ClientBuilder::new()
                .with_endpoint(endpoint)
                .with_username(USERNAME)
                .with_password(PASSWORD)
                .build_arrow()
                .await
            {
                Ok(client) => return client,
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
            ClickHouseDestination::new(ClickHouseConfiguration {
                database: DATABASE.to_string(),
                url: self.clickhouse_endpoint.clone(),
                username: USERNAME.to_string(),
                password: Some(PASSWORD.to_string()),
            })
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
        let client = ClientBuilder::new()
            .with_endpoint(&self.clickhouse_endpoint)
            .with_database(DATABASE)
            .with_username(USERNAME)
            .with_password(PASSWORD)
            .build_arrow()
            .await
            .expect("failed to connect");

        let batches: Vec<RecordBatch> = client
            .query(format!("SELECT count() FROM {TABLE}"), None)
            .await
            .expect("query failed")
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to collect");

        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("expected UInt64Array")
            .value(0)
    }
}

async fn run_engine_for(engine: Arc<Engine>, duration: Duration) {
    let engine_handle = engine.clone();
    let task = tokio::spawn(async move {
        engine_handle
            .run(&EngineConfiguration::default())
            .await
            .expect("engine failed");
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
#[serial]
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
#[serial]
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
#[serial]
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
