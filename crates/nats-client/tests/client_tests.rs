//! Integration tests for `NatsClient` KV operations against a real NATS server
//! (testcontainers; requires Docker).

use std::time::Duration;

use bytes::Bytes;
use gkg_server_config::NatsConfiguration;
use nats_client::NatsClient;
use nats_client::kv_types::{KvBucketConfig, KvPutOptions, KvPutResult};
use testcontainers::ImageExt;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

const BUCKET: &str = "test_locks";
const NATS_TAG: &str = "2.11-alpine";

async fn start_nats() -> (testcontainers::ContainerAsync<Nats>, String) {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = Nats::default()
        .with_cmd(&cmd)
        .with_tag(NATS_TAG)
        .with_mapped_port(0, ContainerPort::Tcp(4222))
        .with_ready_conditions(vec![WaitFor::seconds(3)])
        .start()
        .await
        .expect("failed to start NATS container");

    let host = container.get_host().await.expect("host");
    let port = container.get_host_port_ipv4(4222).await.expect("port");
    (container, format!("{host}:{port}"))
}

fn config(url: &str) -> NatsConfiguration {
    NatsConfiguration {
        url: url.to_string(),
        ..Default::default()
    }
}

#[tokio::test]
async fn kv_put_errors_when_bucket_not_registered() {
    let (_container, url) = start_nats().await;

    let client = NatsClient::connect(&config(&url)).await.expect("connect");

    let result = client
        .kv_put(
            "never_ensured",
            "k",
            Bytes::new(),
            KvPutOptions::create_only(),
        )
        .await;

    let err = result.expect_err("kv_put must not target an unregistered bucket");
    let msg = format!("{err}");
    assert!(
        msg.contains("ensure_kv_bucket_exists"),
        "error must point operators at the explicit-create path, got: {msg}",
    );
}

#[tokio::test]
async fn kv_create_only_returns_already_exists_on_live_key() {
    let (_container, url) = start_nats().await;
    let client = NatsClient::connect(&config(&url)).await.expect("connect");
    client
        .ensure_kv_bucket_exists(BUCKET, KvBucketConfig::default())
        .await
        .expect("ensure");

    client
        .kv_put(
            BUCKET,
            "k",
            Bytes::from_static(b"v1"),
            KvPutOptions::create_only(),
        )
        .await
        .expect("first create");

    let result = client
        .kv_put(
            BUCKET,
            "k",
            Bytes::from_static(b"v2"),
            KvPutOptions::create_only(),
        )
        .await
        .expect("second create");
    assert_eq!(result, KvPutResult::AlreadyExists);
}

#[tokio::test]
async fn kv_update_revision_cas_succeeds_only_on_matching_revision() {
    let (_container, url) = start_nats().await;
    let client = NatsClient::connect(&config(&url)).await.expect("connect");
    client
        .ensure_kv_bucket_exists(BUCKET, KvBucketConfig::default())
        .await
        .expect("ensure");

    let initial = client
        .kv_put(
            BUCKET,
            "k",
            Bytes::from_static(b"v1"),
            KvPutOptions::create_only(),
        )
        .await
        .expect("create");
    let KvPutResult::Success(rev) = initial else {
        panic!("expected Success, got {initial:?}");
    };

    let stale = client
        .kv_put(
            BUCKET,
            "k",
            Bytes::from_static(b"v2"),
            KvPutOptions::update_revision(rev + 99),
        )
        .await
        .expect("stale cas");
    assert_eq!(stale, KvPutResult::RevisionMismatch);

    let fresh = client
        .kv_put(
            BUCKET,
            "k",
            Bytes::from_static(b"v3"),
            KvPutOptions::update_revision(rev),
        )
        .await
        .expect("fresh cas");
    assert!(matches!(fresh, KvPutResult::Success(_)));
}

#[tokio::test]
async fn create_or_update_stream_max_age_override_isolates_dlq() {
    let (_container, url) = start_nats().await;

    let mut cfg = config(&url);
    cfg.stream_max_age_secs = Some(60);
    let client = NatsClient::connect(&cfg).await.expect("connect");

    client
        .create_or_update_stream("test_workqueue", vec!["wq.>".to_string()], None)
        .await
        .expect("workqueue stream create");

    client
        .create_or_update_stream("test_dlq", vec!["dlq.>".to_string()], Some(Duration::ZERO))
        .await
        .expect("dlq stream create");

    let async_client = async_nats::connect(format!("nats://{url}"))
        .await
        .expect("nats connect");
    let js = async_nats::jetstream::new(async_client);

    let mut wq = js
        .get_stream("test_workqueue")
        .await
        .expect("workqueue stream exists");
    let wq_max_age = wq.info().await.expect("workqueue info").config.max_age;
    assert_eq!(
        wq_max_age,
        Duration::from_secs(60),
        "workqueue stream must inherit configured stream_max_age",
    );

    let mut dlq = js.get_stream("test_dlq").await.expect("dlq stream exists");
    let dlq_max_age = dlq.info().await.expect("dlq info").config.max_age;
    assert_eq!(
        dlq_max_age,
        Duration::ZERO,
        "dlq stream must pin max_age=0 regardless of configured stream_max_age",
    );
}
