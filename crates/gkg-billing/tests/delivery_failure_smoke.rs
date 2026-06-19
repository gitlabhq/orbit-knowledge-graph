// Smoke test for the on_failure delivery callback path.
// Spins up a local HTTP server that always returns 400 so labkit fires
// DeliveryFailure::NonRetriableStatus immediately, then asserts all expected
// log fields appear.
//
// Run:
//   cargo test -p gkg-billing --test delivery_failure_smoke -- --nocapture

use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::http::StatusCode;
use axum::routing::post;
use gkg_billing::{BillingInputs, BillingObserver, SnowplowBillingTracker};
use gkg_server_config::{BillingConfig, QuotaConfig};
use query_engine::pipeline::PipelineObserver;
use tokio::net::TcpListener;
use tracing_subscriber::fmt::MakeWriter;

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

fn test_inputs() -> BillingInputs {
    BillingInputs {
        user_id: 99002,
        source_type: "mcp".into(),
        organization_id: Some(1),
        instance_id: Some("failure-smoke-instance".into()),
        unique_instance_id: Some("failure-smoke-uid".into()),
        instance_version: Some("18.0.0".into()),
        global_user_id: Some("failure-smoke-global-user".into()),
        host_name: Some("gitlab.example.com".into()),
        root_namespace_id: Some(1),
        deployment_type: Some(".com".into()),
        realm: Some("SaaS".into()),
    }
}

#[tokio::test]
async fn on_failure_callback_logs_all_fields() {
    let buf = Arc::new(Mutex::new(Vec::<u8>::new()));
    let _ = tracing_subscriber::fmt()
        .with_writer(CapturingWriter(buf.clone()))
        .with_max_level(tracing::Level::WARN)
        .with_ansi(false)
        .try_init();

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    // Server that immediately returns 400 → NonRetriableStatus, no retries.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = axum::Router::new()
        .route(
            "/com.snowplowanalytics.snowplow.auth/tp2",
            post(|| async { StatusCode::BAD_REQUEST }),
        )
        .fallback(|| async { StatusCode::NOT_FOUND });
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

    {
        let config = BillingConfig {
            enabled: true,
            collector_url: format!("http://{addr}"),
            quota: QuotaConfig {
                enabled: false,
                ..Default::default()
            },
        };
        let tracker = Arc::new(
            SnowplowBillingTracker::from_config(&config).expect("tracker init must succeed"),
        );
        let mut obs = BillingObserver::new(Some(tracker), test_inputs());
        obs.set_query_type("traversal");
        obs.finish(42, 3);
    }

    // 400 is non-retriable so the callback should fire almost immediately.
    tokio::time::sleep(Duration::from_secs(2)).await;

    let logs = String::from_utf8_lossy(&buf.lock().unwrap()).to_string();

    assert!(
        logs.contains("billing event delivery: failed"),
        "on_failure log line missing\nLogs:\n{logs}"
    );
    assert!(
        logs.contains("events=1"),
        "events count missing from on_failure log\nLogs:\n{logs}"
    );
    assert!(
        logs.contains("event_ids="),
        "event_ids array missing from on_failure log\nLogs:\n{logs}"
    );
    assert!(
        logs.contains("reason=\"non_retriable_status\""),
        "reason field missing or wrong\nLogs:\n{logs}"
    );
    assert!(
        logs.contains("status=Some(400)"),
        "status field missing or wrong\nLogs:\n{logs}"
    );
    // Verify the event_ids UUID is non-nil.
    assert!(
        !logs.contains("00000000-0000-0000-0000-000000000000"),
        "event_id must not be the nil UUID\nLogs:\n{logs}"
    );
}
