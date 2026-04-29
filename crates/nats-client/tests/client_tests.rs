//! Integration tests for `NatsClient::ensure_kv_bucket_exists` against a real
//! NATS server (testcontainers; requires Docker).
//!
//! Regression coverage: when a bucket already exists with a different config,
//! `ensure_kv_bucket_exists` must migrate the existing bucket in place rather
//! than erroring with `STREAM_NAME_IN_USE`.

use std::time::Duration;

use bytes::Bytes;
use gkg_server_config::NatsConfiguration;
use nats_client::NatsClient;
use nats_client::kv_types::{KvBucketConfig, KvPutOptions};
use testcontainers::ImageExt;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers_modules::nats::{Nats, NatsServerCmd};

const BUCKET: &str = "test_locks";

// Pin to a NATS 2.11+ tag because per-message TTL is a 2.11 server feature.
// testcontainers-modules' default tag is older and cannot host the bucket.
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

async fn stream_allow_message_ttl(url: &str, bucket: &str) -> bool {
    let client = async_nats::connect(format!("nats://{url}"))
        .await
        .expect("nats connect");
    let js = async_nats::jetstream::new(client);
    let mut stream = js
        .get_stream(format!("KV_{bucket}"))
        .await
        .expect("KV stream should exist");
    stream
        .info()
        .await
        .expect("stream info")
        .config
        .allow_message_ttl
}

/// Simulates the production state: a prior release created the bucket without
/// per-message TTL, then a new release boots and calls `ensure_kv_bucket_exists`
/// with `with_per_message_ttl()`. The fix uses `create_or_update_key_value`
/// under the hood, which sends `STREAM.UPDATE` and migrates the existing
/// bucket. Without the fix this errors with `STREAM_NAME_IN_USE` and the
/// bucket stays in its original config forever, so per-key TTL never works.
#[tokio::test]
async fn ensure_kv_bucket_exists_migrates_existing_bucket_to_enable_per_key_ttl() {
    let (_container, url) = start_nats().await;

    // Step 1: create bucket WITHOUT per-message TTL (old release).
    let client_old = NatsClient::connect(&config(&url)).await.expect("connect");
    client_old
        .ensure_kv_bucket_exists(BUCKET, KvBucketConfig::default())
        .await
        .expect("initial bucket create should succeed");
    assert!(
        !stream_allow_message_ttl(&url, BUCKET).await,
        "precondition: existing bucket should not have allow_message_ttl",
    );

    // Step 2: new release reconnects and calls ensure with TTL config.
    let client_new = NatsClient::connect(&config(&url)).await.expect("connect");
    client_new
        .ensure_kv_bucket_exists(BUCKET, KvBucketConfig::with_per_message_ttl())
        .await
        .expect("ensure_kv_bucket_exists must migrate existing bucket, not error");

    assert!(
        stream_allow_message_ttl(&url, BUCKET).await,
        "after re-ensure with TTL config, stream must advertise allow_message_ttl",
    );

    // Step 3: per-key TTL must actually work end-to-end on the migrated bucket.
    let ttl = Duration::from_secs(2);
    client_new
        .kv_put(
            BUCKET,
            "expiring_key",
            Bytes::new(),
            KvPutOptions::create_with_ttl(ttl),
        )
        .await
        .expect("kv_put with TTL");

    tokio::time::sleep(ttl + Duration::from_secs(2)).await;

    let key = client_new
        .kv_get(BUCKET, "expiring_key")
        .await
        .expect("kv_get");
    assert!(
        key.is_none(),
        "key with per-key TTL must expire on the migrated bucket",
    );
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
