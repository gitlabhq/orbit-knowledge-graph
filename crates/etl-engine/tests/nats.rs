//! Integration tests for the NATS broker.
//!
//! These tests require a Docker-compatible runtime (Docker, Colima, etc).

use std::sync::Arc;

use etl_engine::metrics::EngineMetrics;
use etl_engine::nats::{NatsBroker, NatsConfiguration};
use etl_engine::types::{Envelope, Event, Topic};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use serial_test::serial;
use testcontainers::ImageExt;
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

const TEST_STREAM: &str = "test_stream";
const TEST_SUBJECT: &str = "test.events";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestEvent {
    id: String,
    value: i32,
}

impl Event for TestEvent {
    fn topic() -> Topic {
        Topic::new(TEST_STREAM, TEST_SUBJECT)
    }
}

async fn start_nats_container() -> (testcontainers::ContainerAsync<Nats>, String) {
    let nats_cmd = NatsServerCmd::default().with_jetstream();
    let container = Nats::default()
        .with_cmd(&nats_cmd)
        .with_tag("2.11-alpine")
        .start()
        .await
        .expect("failed to start NATS container");

    let host = container
        .get_host()
        .await
        .expect("failed to get container host");

    let port = container
        .get_host_port_ipv4(4222)
        .await
        .expect("failed to get NATS port");

    let url = format!("{host}:{port}");
    (container, url)
}

async fn create_test_stream(url: &str) {
    let client = async_nats::connect(format!("nats://{url}"))
        .await
        .expect("failed to connect to NATS");

    let jetstream = async_nats::jetstream::new(client);

    jetstream
        .create_stream(async_nats::jetstream::stream::Config {
            name: TEST_STREAM.to_string(),
            subjects: vec![format!("{TEST_SUBJECT}.>"), TEST_SUBJECT.to_string()],
            ..Default::default()
        })
        .await
        .expect("failed to create stream");
}

#[tokio::test]
#[serial]
async fn connect_to_nats() {
    let (_container, url) = start_nats_container().await;

    let config = NatsConfiguration {
        url,
        ..Default::default()
    };

    let result = NatsBroker::connect(&config).await;
    assert!(result.is_ok(), "should connect to NATS");
}

#[tokio::test]
#[serial]
async fn publish_and_subscribe() {
    let (_container, url) = start_nats_container().await;
    create_test_stream(&url).await;

    let config = NatsConfiguration {
        url,
        ..Default::default()
    };

    let broker = NatsBroker::connect(&config)
        .await
        .expect("failed to connect");

    let topic = Topic::new(TEST_STREAM, TEST_SUBJECT);

    let mut subscription = broker
        .subscribe(&topic, Arc::new(EngineMetrics::new()))
        .await
        .expect("failed to subscribe");

    let event = TestEvent {
        id: "test-1".to_string(),
        value: 42,
    };
    let envelope = Envelope::new(&event).expect("failed to create envelope");

    broker
        .publish(&topic, &envelope)
        .await
        .expect("failed to publish");

    let received = tokio::time::timeout(std::time::Duration::from_secs(5), subscription.next())
        .await
        .expect("timed out waiting for message")
        .expect("subscription ended")
        .expect("failed to receive message");

    let received_event: TestEvent = received.envelope.to_event().expect("failed to deserialize");

    assert_eq!(received_event.id, "test-1");
    assert_eq!(received_event.value, 42);

    received.ack().await.expect("failed to ack");
}

#[tokio::test]
#[serial]
async fn nack_redelivers_message() {
    let (_container, url) = start_nats_container().await;
    create_test_stream(&url).await;

    let config = NatsConfiguration {
        url,
        ..Default::default()
    };

    let broker = NatsBroker::connect(&config)
        .await
        .expect("failed to connect");

    let topic = Topic::new(TEST_STREAM, TEST_SUBJECT);

    let mut subscription = broker
        .subscribe(&topic, Arc::new(EngineMetrics::new()))
        .await
        .expect("failed to subscribe");

    let event = TestEvent {
        id: "nack-test".to_string(),
        value: 99,
    };
    let envelope = Envelope::new(&event).expect("failed to create envelope");
    broker
        .publish(&topic, &envelope)
        .await
        .expect("failed to publish");

    let first = tokio::time::timeout(std::time::Duration::from_secs(5), subscription.next())
        .await
        .expect("timed out")
        .expect("ended")
        .expect("failed");

    first.nack().await.expect("failed to nack");

    let second = tokio::time::timeout(std::time::Duration::from_secs(5), subscription.next())
        .await
        .expect("timed out waiting for redelivery")
        .expect("ended")
        .expect("failed");

    let redelivered: TestEvent = second.envelope.to_event().expect("deserialize failed");
    assert_eq!(redelivered.id, "nack-test");

    second.ack().await.expect("failed to ack");
}

#[tokio::test]
#[serial]
async fn nonexistent_stream() {
    let (_container, url) = start_nats_container().await;

    let config = NatsConfiguration {
        url,
        ..Default::default()
    };

    let broker = NatsBroker::connect(&config)
        .await
        .expect("failed to connect");

    let topic = Topic::new("nonexistent", "subject");
    let result = broker
        .subscribe(&topic, Arc::new(EngineMetrics::new()))
        .await;
    assert!(result.is_err());
}

#[tokio::test]
#[serial]
async fn multiple_streams() {
    let (_container, url) = start_nats_container().await;

    let client = async_nats::connect(format!("nats://{url}"))
        .await
        .expect("connect");
    let js = async_nats::jetstream::new(client);

    js.create_stream(async_nats::jetstream::stream::Config {
        name: "stream_a".to_string(),
        subjects: vec!["a.>".to_string()],
        ..Default::default()
    })
    .await
    .expect("create stream_a");

    js.create_stream(async_nats::jetstream::stream::Config {
        name: "stream_b".to_string(),
        subjects: vec!["b.>".to_string()],
        ..Default::default()
    })
    .await
    .expect("create stream_b");

    let config = NatsConfiguration {
        url,
        ..Default::default()
    };

    let broker = NatsBroker::connect(&config)
        .await
        .expect("failed to connect");

    let topic_a = Topic::new("stream_a", "a.events");
    let topic_b = Topic::new("stream_b", "b.events");

    let mut sub_a = broker
        .subscribe(&topic_a, Arc::new(EngineMetrics::new()))
        .await
        .expect("sub a");
    let mut sub_b = broker
        .subscribe(&topic_b, Arc::new(EngineMetrics::new()))
        .await
        .expect("sub b");

    let event_a = TestEvent {
        id: "from-a".to_string(),
        value: 1,
    };
    let envelope_a = Envelope::new(&event_a).unwrap();
    broker
        .publish(&topic_a, &envelope_a)
        .await
        .expect("publish a");

    let event_b = TestEvent {
        id: "from-b".to_string(),
        value: 2,
    };
    let envelope_b = Envelope::new(&event_b).unwrap();
    broker
        .publish(&topic_b, &envelope_b)
        .await
        .expect("publish b");

    let msg_a = tokio::time::timeout(std::time::Duration::from_secs(5), sub_a.next())
        .await
        .expect("timeout a")
        .expect("end a")
        .expect("err a");

    let msg_b = tokio::time::timeout(std::time::Duration::from_secs(5), sub_b.next())
        .await
        .expect("timeout b")
        .expect("end b")
        .expect("err b");

    let recv_a: TestEvent = msg_a.envelope.to_event().unwrap();
    let recv_b: TestEvent = msg_b.envelope.to_event().unwrap();

    assert_eq!(recv_a.id, "from-a");
    assert_eq!(recv_b.id, "from-b");

    msg_a.ack().await.unwrap();
    msg_b.ack().await.unwrap();
}

#[tokio::test]
#[serial]
async fn auto_creates_stream_with_configured_settings() {
    let (_container, url) = start_nats_container().await;

    let config = NatsConfiguration {
        url: url.clone(),
        auto_create_streams: true,
        stream_replicas: 1,
        stream_max_age_secs: Some(3600),
        ..Default::default()
    };

    let broker = NatsBroker::connect(&config)
        .await
        .expect("failed to connect");

    let topic = Topic::new("auto_created_stream", "auto.events");
    let topics = vec![topic.clone()];

    broker
        .ensure_streams(&topics)
        .await
        .expect("failed to ensure streams");

    let client = async_nats::connect(format!("nats://{url}"))
        .await
        .expect("connect");
    let jetstream = async_nats::jetstream::new(client);

    let mut stream = jetstream
        .get_stream("auto_created_stream")
        .await
        .expect("stream should exist");

    let info = stream.info().await.expect("failed to get stream info");
    assert_eq!(info.config.name, "auto_created_stream");
    assert!(info.config.subjects.contains(&"auto.events".to_string()));
    assert_eq!(info.config.max_age, std::time::Duration::from_secs(3600));
}

#[tokio::test]
#[serial]
async fn skips_creation_when_disabled() {
    let (_container, url) = start_nats_container().await;

    let config = NatsConfiguration {
        url: url.clone(),
        auto_create_streams: false,
        ..Default::default()
    };

    let broker = NatsBroker::connect(&config)
        .await
        .expect("failed to connect");

    let topic = Topic::new("should_not_exist", "skip.events");
    let topics = vec![topic];

    broker
        .ensure_streams(&topics)
        .await
        .expect("ensure_streams should succeed even when disabled");

    let client = async_nats::connect(format!("nats://{url}"))
        .await
        .expect("connect");
    let jetstream = async_nats::jetstream::new(client);

    let result = jetstream.get_stream("should_not_exist").await;
    assert!(
        result.is_err(),
        "stream should not exist when auto-create is disabled"
    );
}

#[tokio::test]
#[serial]
async fn idempotent_when_stream_exists() {
    let (_container, url) = start_nats_container().await;
    create_test_stream(&url).await;

    let config = NatsConfiguration {
        url: url.clone(),
        auto_create_streams: true,
        ..Default::default()
    };

    let broker = NatsBroker::connect(&config)
        .await
        .expect("failed to connect");

    let topic = Topic::new(TEST_STREAM, TEST_SUBJECT);
    let topics = vec![topic];

    let result = broker.ensure_streams(&topics).await;
    assert!(result.is_ok(), "ensure_streams should be idempotent");

    let result2 = broker.ensure_streams(&topics).await;
    assert!(
        result2.is_ok(),
        "ensure_streams should succeed on second call"
    );
}
