//! Integration tests for the ETL engine.
//!
//! These tests verify the full message flow: NATS -> Handler -> ClickHouse.
//! They require a Docker-compatible runtime (Docker, Colima, etc).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use arrow::array::{Int32Array, StringArray, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use gkg_server_config::{
    ClickHouseConfiguration, EngineConfiguration, HandlerConfiguration, NatsConfiguration,
};
use gkg_utils::arrow::ArrowUtils;
use indexer::clickhouse::{ArrowClickHouseClient, ClickHouseDestination};
use indexer::dead_letter::{DEAD_LETTER_STREAM, DeadLetterEnvelope};
use indexer::engine::{Engine, EngineBuilder};
use indexer::handler::{Handler, HandlerContext, HandlerError, HandlerRegistry};
use indexer::metrics::EngineMetrics;
use indexer::nats::NatsBroker;
use indexer::types::{Envelope, Event, Subscription};
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

fn test_subscription() -> Subscription {
    Subscription::new(STREAM, SUBJECT).manage_stream(false)
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestEvent {
    id: i32,
    name: String,
}

impl Event for TestEvent {
    fn subscription() -> Subscription {
        test_subscription()
    }
}

struct TestHandler;

#[async_trait]
impl Handler for TestHandler {
    fn name(&self) -> &str {
        "test-handler"
    }

    fn subscription(&self) -> Subscription {
        test_subscription()
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        static CONFIG: HandlerConfiguration = HandlerConfiguration {
            concurrency_group: None,
            max_attempts: None,
            retry_interval_secs: None,
        };
        &CONFIG
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
            let client = ArrowClickHouseClient::new(
                endpoint,
                "default",
                USERNAME,
                Some(PASSWORD),
                &std::collections::HashMap::new(),
            );

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
        self.create_broker_with_config(NatsConfiguration {
            url: self.nats_url.clone(),
            ..Default::default()
        })
        .await
    }

    async fn create_broker_with_config(&self, config: NatsConfiguration) -> Arc<NatsBroker> {
        Arc::new(
            NatsBroker::connect(&config)
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
                    query_settings: std::collections::HashMap::new(),
                    profiling: Default::default(),
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
            .publish(&test_subscription(), &envelope)
            .await
            .expect("failed to publish");
    }

    async fn query_count(&self) -> u64 {
        let client = ArrowClickHouseClient::new(
            &self.clickhouse_endpoint,
            DATABASE,
            USERNAME,
            Some(PASSWORD),
            &std::collections::HashMap::new(),
        );

        let batches = client
            .query_arrow(&format!("SELECT count() as cnt FROM {TABLE}"))
            .await
            .expect("query failed");

        ArrowUtils::get_column_by_name::<UInt64Array>(&batches[0], "cnt")
            .expect("cnt column")
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
    handler: Box<dyn Handler>,
) -> Arc<Engine> {
    let registry = Arc::new(HandlerRegistry::default());
    registry.register_handler(handler);
    let indexing_status = Arc::new(indexer::indexing_status::IndexingStatusStore::new(
        Arc::new(nats_client::KvServicesImpl::new(broker.client().clone())),
    ));
    Arc::new(EngineBuilder::new(broker, registry, destination, indexing_status).build())
}

#[tokio::test]
async fn single_message_flows_through_engine() {
    let context = TestContext::new().await;
    let broker = context.create_broker().await;
    let destination = context.create_destination().await;
    let engine = create_engine(broker.clone(), destination, Box::new(TestHandler));

    context.publish_event(&broker, 1, "alice").await;
    run_engine_for(engine, Duration::from_secs(2)).await;

    assert_eq!(context.query_count().await, 1);
}

#[tokio::test]
async fn multiple_messages_processed() {
    let context = TestContext::new().await;
    let broker = context.create_broker().await;
    let destination = context.create_destination().await;
    let engine = create_engine(broker.clone(), destination, Box::new(TestHandler));

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

    let registry = Arc::new(HandlerRegistry::default());
    registry.register_handler(Box::new(TestHandler));
    registry.register_handler(Box::new(TestHandler));

    let indexing_status = Arc::new(indexer::indexing_status::IndexingStatusStore::new(
        Arc::new(nats_client::KvServicesImpl::new(broker.client().clone())),
    ));
    let engine = Arc::new(
        EngineBuilder::new(broker.clone(), registry, destination, indexing_status).build(),
    );

    context.publish_event(&broker, 100, "shared").await;
    run_engine_for(engine, Duration::from_secs(2)).await;

    assert_eq!(context.query_count().await, 2);
}

const PANIC_STREAM: &str = "panic_test_stream";
const PANIC_SUBJECT: &str = "panic.events";

fn panic_subscription() -> Subscription {
    Subscription::new(PANIC_STREAM, PANIC_SUBJECT)
}

struct PanickingHandler {
    should_panic: Arc<AtomicBool>,
}

#[async_trait]
impl Handler for PanickingHandler {
    fn name(&self) -> &str {
        "panicking-handler"
    }

    fn subscription(&self) -> Subscription {
        panic_subscription()
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        static CONFIG: HandlerConfiguration = HandlerConfiguration {
            concurrency_group: None,
            max_attempts: None,
            retry_interval_secs: None,
        };
        &CONFIG
    }

    async fn handle(&self, context: HandlerContext, message: Envelope) -> Result<(), HandlerError> {
        if self.should_panic.load(Ordering::SeqCst) {
            panic!("intentional panic in handler");
        }

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

#[tokio::test]
async fn subject_is_unblocked_after_handler_panic() {
    let context = TestContext::new().await;

    let nats_config = NatsConfiguration {
        url: context.nats_url.clone(),
        max_deliver: Some(1),
        consumer_name: Some("panic-test-consumer".into()),
        ..Default::default()
    };

    let should_panic = Arc::new(AtomicBool::new(true));
    let destination = context.create_destination().await;

    // Phase 1: publish a message that will cause the handler to panic.
    // The engine should catch the panic and term-ack the message, freeing the subject slot.
    {
        let broker = context.create_broker_with_config(nats_config.clone()).await;

        broker
            .ensure_streams(&[panic_subscription()])
            .await
            .expect("stream creation should succeed");

        let envelope = Envelope::new(&TestEvent {
            id: 1,
            name: "will-panic".into(),
        })
        .expect("envelope");
        broker
            .publish(&panic_subscription(), &envelope)
            .await
            .expect("first publish should succeed");

        let handler = PanickingHandler {
            should_panic: should_panic.clone(),
        };
        let engine = create_engine(broker.clone(), destination.clone(), Box::new(handler));

        run_engine_for(engine, Duration::from_secs(2)).await;

        let broker = Arc::try_unwrap(broker)
            .ok()
            .expect("broker has no other owners");
        broker.shutdown().await;
    }

    assert_eq!(
        context.query_count().await,
        0,
        "panicked message should not be written"
    );

    // Phase 2: the subject slot is now free. Publish a new message and verify it
    // is processed by a non-panicking handler.
    should_panic.store(false, Ordering::SeqCst);

    let broker = context.create_broker_with_config(nats_config).await;

    let envelope = Envelope::new(&TestEvent {
        id: 2,
        name: "after-panic".into(),
    })
    .expect("envelope");
    broker
        .publish(&panic_subscription(), &envelope)
        .await
        .expect("republish should succeed after term-ack freed the subject slot");

    let handler = PanickingHandler {
        should_panic: should_panic.clone(),
    };
    let engine = create_engine(broker.clone(), destination, Box::new(handler));

    run_engine_for(engine, Duration::from_secs(2)).await;

    assert_eq!(
        context.query_count().await,
        1,
        "second message should be processed"
    );
}

// -- Permanent error test helpers --

use indexer::handler::PermanentAction;

fn permanent_error_subscription(stream: &str, subject: &str) -> Subscription {
    Subscription::new(stream, subject).dead_letter_on_exhaustion(true)
}

struct PermanentErrorHandler {
    stream: String,
    subject: String,
    error_message: String,
    action: PermanentAction,
}

impl PermanentErrorHandler {
    fn new(stream: &str, subject: &str, action: PermanentAction, error_message: &str) -> Self {
        Self {
            stream: stream.into(),
            subject: subject.into(),
            error_message: error_message.into(),
            action,
        }
    }
}

#[async_trait]
impl Handler for PermanentErrorHandler {
    fn name(&self) -> &str {
        "permanent-error-handler"
    }

    fn subscription(&self) -> Subscription {
        permanent_error_subscription(&self.stream, &self.subject)
    }

    fn engine_config(&self) -> &HandlerConfiguration {
        static CONFIG: HandlerConfiguration = HandlerConfiguration {
            concurrency_group: None,
            max_attempts: Some(5),
            retry_interval_secs: Some(1),
        };
        &CONFIG
    }

    async fn handle(
        &self,
        _context: HandlerContext,
        _message: Envelope,
    ) -> Result<(), HandlerError> {
        Err(HandlerError::Permanent {
            message: self.error_message.clone(),
            action: self.action,
        })
    }
}

async fn publish_test_event(broker: &NatsBroker, subscription: &Subscription) {
    let envelope = Envelope::new(&TestEvent {
        id: 42,
        name: "will-fail-permanently".into(),
    })
    .expect("envelope");
    broker
        .publish(subscription, &envelope)
        .await
        .expect("publish should succeed");
}

async fn dlq_message_count(nats_url: &str) -> u64 {
    let nats_client = async_nats::connect(format!("nats://{nats_url}"))
        .await
        .expect("connect to NATS");
    let jetstream = async_nats::jetstream::new(nats_client);
    let mut dlq_stream = jetstream
        .get_stream(DEAD_LETTER_STREAM)
        .await
        .expect("DLQ stream should exist");
    let info = dlq_stream.info().await.expect("DLQ stream info");
    info.state.messages
}

async fn last_dlq_envelope(nats_url: &str) -> DeadLetterEnvelope {
    let nats_client = async_nats::connect(format!("nats://{nats_url}"))
        .await
        .expect("connect to NATS");
    let jetstream = async_nats::jetstream::new(nats_client);
    let mut dlq_stream = jetstream
        .get_stream(DEAD_LETTER_STREAM)
        .await
        .expect("DLQ stream should exist");
    let last_seq = dlq_stream
        .info()
        .await
        .expect("DLQ stream info")
        .state
        .last_sequence;
    let raw = dlq_stream
        .get_raw_message(last_seq)
        .await
        .expect("get DLQ message");
    serde_json::from_slice(&raw.payload).expect("deserialize DLQ envelope")
}

#[tokio::test]
async fn permanent_dlq_error_sends_to_dlq_on_first_attempt() {
    let context = TestContext::new().await;
    let stream = "permanent_dlq_stream";
    let subject = "permanent_dlq.events";
    let subscription = permanent_error_subscription(stream, subject);

    let broker = context
        .create_broker_with_config(NatsConfiguration {
            url: context.nats_url.clone(),
            consumer_name: Some("permanent-dlq-consumer".into()),
            ..Default::default()
        })
        .await;
    let destination = context.create_destination().await;

    broker
        .ensure_streams(std::slice::from_ref(&subscription))
        .await
        .expect("stream creation");

    publish_test_event(&broker, &subscription).await;

    let handler = PermanentErrorHandler::new(
        stream,
        subject,
        PermanentAction::DeadLetter,
        "fatal code indexing pipeline error during parse for main.rs: unsupported",
    );
    let engine = create_engine(broker.clone(), destination, Box::new(handler));
    run_engine_for(engine, Duration::from_secs(3)).await;

    assert_eq!(
        context.query_count().await,
        0,
        "nothing written to ClickHouse"
    );
    assert!(
        dlq_message_count(&context.nats_url).await >= 1,
        "DLQ should have a message"
    );

    let dlq_envelope = last_dlq_envelope(&context.nats_url).await;
    assert_eq!(dlq_envelope.attempts, 1, "should DLQ on first attempt");
    assert!(
        dlq_envelope
            .last_error
            .contains("fatal code indexing pipeline error"),
        "unexpected DLQ error: {}",
        dlq_envelope.last_error
    );
}

#[tokio::test]
async fn permanent_drop_error_drops_without_dlq() {
    let context = TestContext::new().await;
    let stream = "permanent_drop_stream";
    let subject = "permanent_drop.events";
    let subscription = permanent_error_subscription(stream, subject);

    let broker = context
        .create_broker_with_config(NatsConfiguration {
            url: context.nats_url.clone(),
            consumer_name: Some("permanent-drop-consumer".into()),
            ..Default::default()
        })
        .await;
    let destination = context.create_destination().await;

    broker
        .ensure_streams(std::slice::from_ref(&subscription))
        .await
        .expect("stream creation");

    publish_test_event(&broker, &subscription).await;

    let handler = PermanentErrorHandler::new(
        stream,
        subject,
        PermanentAction::Drop,
        "known unrecoverable state",
    );
    let engine = create_engine(broker.clone(), destination, Box::new(handler));
    run_engine_for(engine, Duration::from_secs(3)).await;

    assert_eq!(
        context.query_count().await,
        0,
        "nothing written to ClickHouse"
    );
    assert_eq!(
        dlq_message_count(&context.nats_url).await,
        0,
        "DLQ should be empty"
    );
}
