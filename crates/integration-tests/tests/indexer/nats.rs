//! Integration tests for the NATS broker.
//!
//! These tests require a Docker-compatible runtime (Docker, Colima, etc).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use gkg_server_config::NatsConfiguration;
use indexer::dead_letter::{DEAD_LETTER_STREAM, DeadLetterEnvelope};
use indexer::metrics::EngineMetrics;
use indexer::nats::NatsBroker;
use indexer::types::{Envelope, Event, Subscription};
use serde::{Deserialize, Serialize};
use testcontainers::ImageExt;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

const TEST_STREAM: &str = "test_stream";
const TEST_SUBJECT: &str = "test.events";
const DLQ_SOURCE_STREAM: &str = "dlq_source_stream";
const DLQ_SOURCE_SUBJECT_FILTER: &str = "code.task.indexing.requested.*.*";
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestEvent {
    id: String,
    value: i32,
}

impl Event for TestEvent {
    fn subscription() -> Subscription {
        Subscription::new(TEST_STREAM, TEST_SUBJECT)
    }
}

async fn start_nats_container() -> (testcontainers::ContainerAsync<Nats>, String) {
    let nats_cmd = NatsServerCmd::default().with_jetstream();
    let container = Nats::default()
        .with_cmd(&nats_cmd)
        .with_tag("2.11-alpine")
        .with_mapped_port(0, ContainerPort::Tcp(4222))
        .with_ready_conditions(vec![WaitFor::seconds(3)])
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

fn default_config(url: &str) -> NatsConfiguration {
    NatsConfiguration {
        url: url.to_string(),
        ..Default::default()
    }
}

async fn connect_broker(config: &NatsConfiguration) -> NatsBroker {
    NatsBroker::connect(config)
        .await
        .expect("failed to connect broker")
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

async fn publish_event(broker: &NatsBroker, subscription: &Subscription, id: &str, value: i32) {
    let event = TestEvent {
        id: id.to_string(),
        value,
    };
    let envelope = Envelope::new(&event).expect("failed to create envelope");
    broker
        .publish(subscription, &envelope)
        .await
        .expect("failed to publish");
}

fn delivered_envelope(subject: &str, id: &str) -> Envelope {
    let event = TestEvent {
        id: id.to_string(),
        value: 1,
    };
    let mut envelope = Envelope::new(&event).expect("failed to create envelope");
    envelope.subject = Arc::from(subject);
    envelope.attempt = 5;
    envelope
}

async fn receive_event(
    subscription: &mut (
             impl StreamExt<Item = Result<indexer::nats::NatsMessage, indexer::nats::NatsError>> + Unpin
         ),
) -> TestEvent {
    let message = tokio::time::timeout(RECEIVE_TIMEOUT, subscription.next())
        .await
        .expect("timed out waiting for message")
        .expect("subscription ended")
        .expect("failed to receive message");

    let event: TestEvent = message.envelope.to_event().expect("failed to deserialize");
    message.ack().await.expect("failed to ack");
    event
}

async fn assert_stream_not_exists(url: &str, stream_name: &str) {
    let jetstream = jetstream_client(url).await;
    let result = jetstream.get_stream(stream_name).await;
    assert!(result.is_err(), "stream '{stream_name}' should not exist");
}

async fn stream_config(url: &str, stream_name: &str) -> async_nats::jetstream::stream::Config {
    let jetstream = jetstream_client(url).await;
    let mut stream = jetstream
        .get_stream(stream_name)
        .await
        .unwrap_or_else(|_| panic!("stream '{stream_name}' should exist"));
    let info = stream.info().await.expect("failed to get stream info");
    info.config.clone()
}

async fn assert_stream_has_subjects(url: &str, stream_name: &str, expected_subjects: &[&str]) {
    let config = stream_config(url, stream_name).await;

    for subject in expected_subjects {
        assert!(
            config.subjects.contains(&subject.to_string()),
            "stream '{stream_name}' should contain subject '{subject}', got {:?}",
            config.subjects
        );
    }
}

async fn get_dead_letter(
    url: &str,
    subject: &str,
) -> async_nats::jetstream::message::StreamMessage {
    let jetstream = jetstream_client(url).await;
    let stream = jetstream
        .get_stream(DEAD_LETTER_STREAM)
        .await
        .expect("dead letter stream should exist");

    stream
        .get_last_raw_message_by_subject(subject)
        .await
        .unwrap_or_else(|_| panic!("dead letter subject '{subject}' should exist"))
}

async fn dead_letter_subject_counts(url: &str) -> BTreeMap<String, usize> {
    let jetstream = jetstream_client(url).await;
    let stream = jetstream
        .get_stream(DEAD_LETTER_STREAM)
        .await
        .expect("dead letter stream should exist");

    let mut subjects = stream
        .info_with_subjects("dlq.dlq_source_stream.>")
        .await
        .expect("failed to fetch dead letter subjects");

    let mut counts = BTreeMap::new();
    while let Some((subject, count)) = subjects
        .try_next()
        .await
        .expect("failed to read dead letter subject")
    {
        counts.insert(subject, count);
    }

    counts
}

async fn jetstream_client(url: &str) -> async_nats::jetstream::Context {
    let client = async_nats::connect(format!("nats://{url}"))
        .await
        .expect("failed to connect to NATS");
    async_nats::jetstream::new(client)
}

#[tokio::test]
async fn dead_letters_use_delivered_subject_for_wildcard_subscriptions() {
    let (_container, url) = start_nats_container().await;
    let broker = connect_broker(&default_config(&url)).await;
    let subscription = Subscription::new(DLQ_SOURCE_STREAM, DLQ_SOURCE_SUBJECT_FILTER)
        .dead_letter_on_exhaustion(true);

    broker
        .ensure_streams(std::slice::from_ref(&subscription))
        .await
        .expect("failed to create streams");

    let first = delivered_envelope("code.task.indexing.requested.278964.bWFzdGVy", "a");
    broker
        .publish_dead_letter(&subscription, &first, "first failure")
        .await
        .expect("failed to publish first dead letter");

    let second = delivered_envelope("code.task.indexing.requested.80602550.bWFpbg", "b");
    broker
        .publish_dead_letter(&subscription, &second, "second failure")
        .await
        .expect("failed to publish second dead letter");

    let first_subject = "dlq.dlq_source_stream.code.task.indexing.requested.278964.bWFzdGVy";
    let first_dead_letter = get_dead_letter(&url, first_subject).await;
    assert_eq!(first_dead_letter.subject.to_string(), first_subject);
    let first_payload: DeadLetterEnvelope =
        serde_json::from_slice(&first_dead_letter.payload).expect("failed to parse dead letter");
    assert_eq!(
        first_payload.original_subject,
        "code.task.indexing.requested.278964.bWFzdGVy"
    );

    let second_subject = "dlq.dlq_source_stream.code.task.indexing.requested.80602550.bWFpbg";
    let second_dead_letter = get_dead_letter(&url, second_subject).await;
    assert_eq!(second_dead_letter.subject.to_string(), second_subject);
    let second_payload: DeadLetterEnvelope =
        serde_json::from_slice(&second_dead_letter.payload).expect("failed to parse dead letter");
    assert_eq!(
        second_payload.original_subject,
        "code.task.indexing.requested.80602550.bWFpbg"
    );

    let wildcard_subject = "dlq.dlq_source_stream.code.task.indexing.requested.*.*";
    let subject_counts = dead_letter_subject_counts(&url).await;
    assert_eq!(
        subject_counts,
        BTreeMap::from([
            (first_subject.to_string(), 1),
            (second_subject.to_string(), 1),
        ])
    );
    assert!(
        !subject_counts.contains_key(wildcard_subject),
        "dead letters should not be stored under the wildcard subscription subject"
    );
}

#[tokio::test]
async fn connect_to_nats() {
    let (_container, url) = start_nats_container().await;
    let config = default_config(&url);

    let result = NatsBroker::connect(&config).await;
    assert!(result.is_ok(), "should connect to NATS");
}

#[tokio::test]
async fn publish_and_subscribe() {
    let (_container, url) = start_nats_container().await;
    create_test_stream(&url).await;

    let broker = connect_broker(&default_config(&url)).await;
    let subscription = Subscription::new(TEST_STREAM, TEST_SUBJECT);

    let mut messages = broker
        .subscribe(&subscription, Arc::new(EngineMetrics::new()))
        .await
        .expect("failed to subscribe");

    publish_event(&broker, &subscription, "test-1", 42).await;

    let event = receive_event(&mut messages).await;
    assert_eq!(event.id, "test-1");
    assert_eq!(event.value, 42);
}

#[tokio::test]
async fn nack_redelivers_message() {
    let (_container, url) = start_nats_container().await;
    create_test_stream(&url).await;

    let broker = connect_broker(&default_config(&url)).await;
    let subscription = Subscription::new(TEST_STREAM, TEST_SUBJECT);

    let mut messages = broker
        .subscribe(&subscription, Arc::new(EngineMetrics::new()))
        .await
        .expect("failed to subscribe");

    publish_event(&broker, &subscription, "nack-test", 99).await;

    let first = tokio::time::timeout(RECEIVE_TIMEOUT, messages.next())
        .await
        .expect("timed out")
        .expect("ended")
        .expect("failed");

    first.nack().await.expect("failed to nack");

    let event = receive_event(&mut messages).await;
    assert_eq!(event.id, "nack-test");
}

#[tokio::test]
async fn nonexistent_stream() {
    let (_container, url) = start_nats_container().await;

    let broker = connect_broker(&default_config(&url)).await;

    let subscription = Subscription::new("nonexistent", "subject");
    let result = broker
        .subscribe(&subscription, Arc::new(EngineMetrics::new()))
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn multiple_streams() {
    let (_container, url) = start_nats_container().await;

    let jetstream = jetstream_client(&url).await;

    jetstream
        .create_stream(async_nats::jetstream::stream::Config {
            name: "stream_a".to_string(),
            subjects: vec!["a.>".to_string()],
            ..Default::default()
        })
        .await
        .expect("create stream_a");

    jetstream
        .create_stream(async_nats::jetstream::stream::Config {
            name: "stream_b".to_string(),
            subjects: vec!["b.>".to_string()],
            ..Default::default()
        })
        .await
        .expect("create stream_b");

    let broker = connect_broker(&default_config(&url)).await;

    let subscription_a = Subscription::new("stream_a", "a.events");
    let subscription_b = Subscription::new("stream_b", "b.events");

    let mut messages_a = broker
        .subscribe(&subscription_a, Arc::new(EngineMetrics::new()))
        .await
        .expect("sub a");
    let mut messages_b = broker
        .subscribe(&subscription_b, Arc::new(EngineMetrics::new()))
        .await
        .expect("sub b");

    publish_event(&broker, &subscription_a, "from-a", 1).await;
    publish_event(&broker, &subscription_b, "from-b", 2).await;

    let event_a = receive_event(&mut messages_a).await;
    let event_b = receive_event(&mut messages_b).await;

    assert_eq!(event_a.id, "from-a");
    assert_eq!(event_b.id, "from-b");
}

#[tokio::test]
async fn auto_creates_stream_with_configured_settings() {
    let (_container, url) = start_nats_container().await;

    let config = NatsConfiguration {
        url: url.clone(),
        auto_create_streams: true,
        stream_replicas: 1,
        stream_max_age_secs: Some(3600),
        ..Default::default()
    };

    let broker = connect_broker(&config).await;

    let subscription = Subscription::new("auto_created_stream", "auto.events");
    broker
        .ensure_streams(&[subscription])
        .await
        .expect("failed to ensure streams");

    assert_stream_has_subjects(&url, "auto_created_stream", &["auto.events"]).await;

    let jetstream = jetstream_client(&url).await;
    let mut stream = jetstream
        .get_stream("auto_created_stream")
        .await
        .expect("stream should exist");
    let info = stream.info().await.expect("failed to get stream info");
    assert_eq!(info.config.max_age, Duration::from_secs(3600));
}

#[tokio::test]
async fn skips_creation_when_disabled() {
    let (_container, url) = start_nats_container().await;

    let config = NatsConfiguration {
        url: url.clone(),
        auto_create_streams: false,
        ..Default::default()
    };

    let broker = connect_broker(&config).await;

    let subscription = Subscription::new("should_not_exist", "skip.events");
    broker
        .ensure_streams(&[subscription])
        .await
        .expect("ensure_streams should succeed even when disabled");

    assert_stream_not_exists(&url, "should_not_exist").await;
}

#[tokio::test]
async fn updates_stream_config_during_rolling_update() {
    // Guards against error 10058 during rolling deploys when stream config changes
    // between versions. See https://gitlab.com/gitlab-org/orbit/knowledge-graph/-/work_items/261
    let (_container, url) = start_nats_container().await;

    let config_v1 = NatsConfiguration {
        url: url.clone(),
        auto_create_streams: true,
        stream_max_age_secs: Some(3600),
        ..Default::default()
    };

    let broker_old = connect_broker(&config_v1).await;
    let subscription_v1 = Subscription::new(TEST_STREAM, TEST_SUBJECT);
    broker_old
        .ensure_streams(std::slice::from_ref(&subscription_v1))
        .await
        .expect("old broker should create stream");

    let mut old_subscription = broker_old
        .subscribe(&subscription_v1, Arc::new(EngineMetrics::new()))
        .await
        .expect("old broker should subscribe");

    let config_v2 = NatsConfiguration {
        url: url.clone(),
        auto_create_streams: true,
        stream_max_age_secs: Some(7200),
        ..Default::default()
    };

    let broker_new = connect_broker(&config_v2).await;
    let subscription_v2_existing = Subscription::new(TEST_STREAM, TEST_SUBJECT);
    let subscription_v2_new = Subscription::new(TEST_STREAM, "test.new_subject");
    broker_new
        .ensure_streams(&[
            subscription_v2_existing.clone(),
            subscription_v2_new.clone(),
        ])
        .await
        .expect("new broker should update stream config while old consumer is active");

    let updated = stream_config(&url, TEST_STREAM).await;
    assert_stream_has_subjects(&url, TEST_STREAM, &[TEST_SUBJECT, "test.new_subject"]).await;
    assert_eq!(
        updated.max_age,
        Duration::from_secs(7200),
        "stream max_age should reflect the v2 config"
    );

    // Old consumer still receives on original subject after config update
    publish_event(&broker_new, &subscription_v2_existing, "after-update", 1).await;
    let event = receive_event(&mut old_subscription).await;
    assert_eq!(event.id, "after-update");

    // New consumer receives on the newly added subject
    let mut new_subscription = broker_new
        .subscribe(&subscription_v2_new, Arc::new(EngineMetrics::new()))
        .await
        .expect("new broker should subscribe to new subject");

    publish_event(&broker_new, &subscription_v2_new, "new-subject", 2).await;
    let new_event = receive_event(&mut new_subscription).await;
    assert_eq!(new_event.id, "new-subject");
}

#[tokio::test]
async fn in_progress_prevents_redelivery() {
    let (_container, url) = start_nats_container().await;
    create_test_stream(&url).await;

    let ack_wait = Duration::from_secs(5);
    let config = NatsConfiguration {
        url,
        ack_wait_secs: ack_wait.as_secs(),
        ..Default::default()
    };

    let broker = NatsBroker::connect(&config)
        .await
        .expect("failed to connect");

    let subscription = Subscription::new(TEST_STREAM, TEST_SUBJECT);

    let mut messages = broker
        .subscribe(&subscription, Arc::new(EngineMetrics::new()))
        .await
        .expect("failed to subscribe");

    let event = TestEvent {
        id: "progress-test".to_string(),
        value: 7,
    };
    let envelope = Envelope::new(&event).expect("failed to create envelope");
    broker
        .publish(&subscription, &envelope)
        .await
        .expect("failed to publish");

    let message = tokio::time::timeout(Duration::from_secs(10), messages.next())
        .await
        .expect("timed out waiting for message")
        .expect("subscription ended")
        .expect("failed to receive message");

    let progress = message.progress_notifier();

    // Send in-progress after 3s (before the 5s ack_wait expires).
    // This resets the deadline to ~8s from message delivery.
    tokio::time::sleep(Duration::from_secs(3)).await;
    progress.notify_in_progress().await;

    // Wait another 3s — now at ~6s total, past the original 5s deadline
    // but safely within the reset window (new deadline ~8s).
    tokio::time::sleep(Duration::from_secs(3)).await;

    // Check for 1s — ends at ~7s, still before the 8s reset deadline.
    let redelivery = tokio::time::timeout(Duration::from_secs(1), messages.next()).await;
    assert!(
        redelivery.is_err(),
        "message should NOT be redelivered after in-progress signal"
    );

    message.ack().await.expect("failed to ack");
}
