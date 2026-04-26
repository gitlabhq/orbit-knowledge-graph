//! Reproduction harness for the per-key TTL bug on the `indexing_locks` bucket.
//!
//! Tracks two scenarios:
//! 1. `repro_per_key_ttl_against_2_11`: confirms the current code path works on a
//!    NATS 2.11 server. If this passes, the production bug is environmental
//!    (server version), not a code defect.
//! 2. `repro_per_key_ttl_against_2_10`: confirms the failure mode against
//!    NATS 2.10 (production). The bucket is created without `allow_msg_ttl`
//!    and keys never expire.
//! 3. `repro_pre_existing_bucket_drift`: confirms that once a bucket exists
//!    without `allow_msg_ttl`, calling `create_key_value` again with
//!    `limit_markers: Some(...)` does NOT migrate the existing stream — it
//!    surfaces a `STREAM_NAME_IN_USE` error that the current
//!    `ensure_kv_bucket_exists` swallows generically.
//!
//! These tests require Docker.

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

async fn start_nats(tag: &str) -> (testcontainers::ContainerAsync<Nats>, String) {
    let cmd = NatsServerCmd::default().with_jetstream();
    let container = Nats::default()
        .with_cmd(&cmd)
        .with_tag(tag)
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

async fn dump_stream_config(url: &str, bucket: &str) -> async_nats::jetstream::stream::Config {
    let client = async_nats::connect(format!("nats://{url}"))
        .await
        .expect("nats connect");
    let js = async_nats::jetstream::new(client);
    let mut stream = js
        .get_stream(format!("KV_{bucket}"))
        .await
        .expect("KV stream should exist");
    stream.info().await.expect("stream info").config.clone()
}

/// Drives the same flow the indexer uses on startup, plus a put-with-ttl and
/// a delayed get. Returns whether the key was still present after `ttl + slack`.
async fn put_then_assert_present_after_ttl(
    client: &NatsClient,
    ttl: Duration,
) -> (bool, async_nats::jetstream::stream::Config) {
    client
        .ensure_kv_bucket_exists(BUCKET, KvBucketConfig::with_per_message_ttl())
        .await
        .expect("ensure_kv_bucket_exists");

    let put_result = client
        .kv_put(
            BUCKET,
            "key1",
            Bytes::new(),
            KvPutOptions::create_with_ttl(ttl),
        )
        .await
        .expect("kv_put");
    assert!(
        put_result.is_success(),
        "create_with_ttl should succeed: {:?}",
        put_result
    );

    tokio::time::sleep(ttl + Duration::from_secs(2)).await;

    let still_present = client
        .kv_get(BUCKET, "key1")
        .await
        .expect("kv_get")
        .is_some();

    let url = client.config().url.clone();
    let stream_config = dump_stream_config(&url, BUCKET).await;
    (still_present, stream_config)
}

#[tokio::test]
async fn repro_per_key_ttl_against_2_11() {
    let (_c, url) = start_nats("2.11-alpine").await;
    let client = NatsClient::connect(&config(&url)).await.expect("connect");

    let (still_present, cfg) =
        put_then_assert_present_after_ttl(&client, Duration::from_secs(2)).await;

    eprintln!("[2.11] allow_message_ttl = {}", cfg.allow_message_ttl);
    eprintln!(
        "[2.11] subject_delete_marker_ttl = {:?}",
        cfg.subject_delete_marker_ttl
    );
    eprintln!("[2.11] still_present_after_ttl = {}", still_present);

    assert!(
        cfg.allow_message_ttl,
        "stream should advertise allow_msg_ttl on 2.11"
    );
    assert!(!still_present, "key should expire after TTL on 2.11");
}

/// On 2.10 we expect either:
/// - `ensure_kv_bucket_exists` to fail, OR
/// - the bucket is created without `allow_msg_ttl` and the key never expires.
///
/// Either way, the test documents what production sees.
#[tokio::test]
async fn repro_per_key_ttl_against_2_10() {
    let (_c, url) = start_nats("2.10.19-alpine").await;
    let client = NatsClient::connect(&config(&url)).await.expect("connect");

    let ensured = client
        .ensure_kv_bucket_exists(BUCKET, KvBucketConfig::with_per_message_ttl())
        .await;
    eprintln!(
        "[2.10] ensure_kv_bucket_exists -> {:?}",
        ensured.as_ref().map(|_| "Ok")
    );

    if ensured.is_err() {
        // Server level < 1, returned as KvToStreamConfigError::LimitMarkersNotSupported;
        // confirms 2.10 cannot host per-message-TTL buckets.
        return;
    }

    let put_result = client
        .kv_put(
            BUCKET,
            "key1",
            Bytes::new(),
            KvPutOptions::create_with_ttl(Duration::from_secs(2)),
        )
        .await
        .expect("kv_put");
    assert!(put_result.is_success());

    tokio::time::sleep(Duration::from_secs(4)).await;
    let still_present = client
        .kv_get(BUCKET, "key1")
        .await
        .expect("kv_get")
        .is_some();

    let cfg = dump_stream_config(&url, BUCKET).await;
    eprintln!("[2.10] allow_message_ttl = {}", cfg.allow_message_ttl);
    eprintln!("[2.10] still_present_after_ttl = {}", still_present);

    assert!(
        still_present,
        "on 2.10, keys with per-key TTL should NOT expire — this is the production symptom",
    );
}

/// Drift case: bucket pre-exists from an old release that didn't request
/// per-message TTL. The new release calls `ensure_kv_bucket_exists` with
/// `with_per_message_ttl()`, but `create_key_value` calls `create_stream`,
/// which errors with `STREAM_NAME_IN_USE` when the stream already exists.
/// The current `ensure_kv_bucket_exists` returns this as `NatsError::KvBucket`.
#[tokio::test]
async fn repro_pre_existing_bucket_drift() {
    let (_c, url) = start_nats("2.11-alpine").await;
    let client = NatsClient::connect(&config(&url)).await.expect("connect");

    // First: create bucket WITHOUT per-message TTL (simulates old release).
    client
        .ensure_kv_bucket_exists(BUCKET, KvBucketConfig::default())
        .await
        .expect("initial create");

    let cfg_before = dump_stream_config(&url, BUCKET).await;
    eprintln!(
        "[drift] before: allow_message_ttl = {}",
        cfg_before.allow_message_ttl
    );
    assert!(!cfg_before.allow_message_ttl);

    // Second: simulate new release attempting to enable per-message TTL on the
    // existing bucket. Use a fresh client so the in-memory cache from the first
    // call doesn't short-circuit.
    let client2 = NatsClient::connect(&config(&url)).await.expect("connect");
    let result = client2
        .ensure_kv_bucket_exists(BUCKET, KvBucketConfig::with_per_message_ttl())
        .await;
    eprintln!(
        "[drift] re-ensure with TTL config -> {:?}",
        result.as_ref().err().map(|e| e.to_string())
    );

    let cfg_after = dump_stream_config(&url, BUCKET).await;
    eprintln!(
        "[drift] after: allow_message_ttl = {}",
        cfg_after.allow_message_ttl
    );

    // After the fix (use create_or_update_key_value), re-ensuring with the
    // TTL-enabled config must migrate the existing stream in place.
    assert!(
        result.is_ok(),
        "ensure_kv_bucket_exists should not error when the bucket already exists",
    );
    assert!(
        cfg_after.allow_message_ttl,
        "ensure_kv_bucket_exists should migrate an existing bucket to allow_msg_ttl",
    );

    // And per-key TTL must now actually work end to end on the migrated bucket.
    client2
        .kv_put(
            BUCKET,
            "drift_key",
            Bytes::new(),
            KvPutOptions::create_with_ttl(Duration::from_secs(2)),
        )
        .await
        .expect("kv_put after migration");
    tokio::time::sleep(Duration::from_secs(4)).await;
    let still_present = client2
        .kv_get(BUCKET, "drift_key")
        .await
        .expect("kv_get")
        .is_some();
    assert!(!still_present, "key should expire on migrated bucket");
}
