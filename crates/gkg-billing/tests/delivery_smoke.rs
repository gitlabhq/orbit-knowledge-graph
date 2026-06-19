// Smoke test for SnowplowBillingTracker + BillingObserver against a live
// Snowplow Micro instance. Requires Snowplow Micro on localhost:9090.
//
// Run:
//   cargo test -p gkg-billing --test delivery_smoke -- --nocapture

use std::sync::{Arc, Mutex};
use std::time::Duration;

use gkg_billing::{BillingInputs, BillingObserver, SnowplowBillingTracker};
use gkg_server_config::{BillingConfig, QuotaConfig};
use query_engine::pipeline::PipelineObserver;
use tracing_subscriber::fmt::MakeWriter;

const MICRO_URL: &str = "http://localhost:9090";

// Writer that captures tracing output into a shared buffer while also echoing
// to stderr so `--nocapture` shows it live.
#[derive(Clone)]
struct CapturingWriter(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for CapturingWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        eprint!("{}", String::from_utf8_lossy(buf));
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for CapturingWriter {
    type Writer = CapturingWriter;
    fn make_writer(&'a self) -> Self::Writer {
        self.clone()
    }
}

fn billing_config() -> BillingConfig {
    BillingConfig {
        enabled: true,
        collector_url: MICRO_URL.to_string(),
        quota: QuotaConfig {
            enabled: false,
            ..Default::default()
        },
    }
}

fn test_inputs() -> BillingInputs {
    BillingInputs {
        user_id: 99001,
        source_type: "mcp".into(),
        organization_id: Some(1),
        instance_id: Some("smoke-instance".into()),
        unique_instance_id: Some("smoke-uid".into()),
        instance_version: Some("18.0.0".into()),
        global_user_id: Some("smoke-global-user".into()),
        host_name: Some("gitlab.example.com".into()),
        root_namespace_id: Some(1),
        deployment_type: Some(".com".into()),
        realm: Some("SaaS".into()),
    }
}

#[tokio::test]
async fn snowplow_billing_delivery_smoke() {
    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let _ = tracing_subscriber::fmt()
        .with_writer(CapturingWriter(buf.clone()))
        .with_max_level(tracing::Level::INFO)
        .with_ansi(false)
        .try_init();

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let client = reqwest::Client::new();

    let before: serde_json::Value = client
        .get(format!("{MICRO_URL}/micro/all"))
        .send()
        .await
        .expect("Snowplow Micro must be running on localhost:9090")
        .json()
        .await
        .unwrap();
    let good_before = before["good"].as_u64().unwrap_or(0);
    eprintln!("[smoke] Micro before: good={good_before}");

    {
        let config = billing_config();
        let tracker = Arc::new(
            SnowplowBillingTracker::from_config(&config).expect("tracker init must succeed"),
        );
        let mut obs = BillingObserver::new(Some(tracker), test_inputs());
        obs.set_query_type("traversal");
        obs.finish(42, 3);
    }

    // Give labkit's background delivery loop time to flush the batch.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let after: serde_json::Value = client
        .get(format!("{MICRO_URL}/micro/all"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let good_after = after["good"].as_u64().unwrap_or(0);
    eprintln!("[smoke] Micro after:  good={good_after}");

    let logs = String::from_utf8_lossy(&buf.lock().unwrap()).to_string();

    // Event must have reached Snowplow Micro.
    assert!(
        good_after > good_before,
        "expected Snowplow Micro to receive at least one new event \
         (before={good_before}, after={good_after})\nLogs captured:\n{logs}"
    );

    // The enqueue log must include a non-nil event_id UUID.
    assert!(
        logs.contains("billing event enqueued for delivery"),
        "missing enqueue log line\nLogs captured:\n{logs}"
    );
    assert!(
        logs.contains("event_id"),
        "event_id field missing from enqueue log\nLogs captured:\n{logs}"
    );
    // UUID in Display format looks like xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx;
    // nil UUID is all zeroes — verify we got a real one.
    assert!(
        !logs.contains("00000000-0000-0000-0000-000000000000"),
        "event_id must not be the nil UUID\nLogs captured:\n{logs}"
    );

    // The on_success callback log must include the events count and ids array.
    assert!(
        logs.contains("billing event delivery: success"),
        "on_success callback log missing — delivery may have failed\nLogs captured:\n{logs}"
    );
    assert!(
        logs.contains("events=1"),
        "on_success log must show events=1 (one event per batch)\nLogs captured:\n{logs}"
    );
    assert!(
        logs.contains("event_ids="),
        "on_success log must include event_ids array\nLogs captured:\n{logs}"
    );
}
