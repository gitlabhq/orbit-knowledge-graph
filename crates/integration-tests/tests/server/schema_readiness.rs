use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use clickhouse_client::ArrowClickHouseClient;
use gkg_server::schema_watcher::{SchemaState, SchemaWatcher};
use gkg_server::webserver::create_router;
use indexer::schema::version::{ensure_version_table, write_schema_version};
use integration_testkit::TestContext;
use tokio::time::{sleep, timeout};
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

const POLL: Duration = Duration::from_millis(50);
const WAIT_LIMIT: Duration = Duration::from_secs(5);

fn ready_request() -> Request<Body> {
    Request::get("/ready").body(Body::empty()).unwrap()
}

async fn parse_response(response: axum::response::Response) -> (StatusCode, serde_json::Value) {
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    (status, json)
}

fn unhealthy_components(json: &serde_json::Value) -> Vec<String> {
    json["unhealthy_components"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect()
}

fn dummy_clickhouse() -> ArrowClickHouseClient {
    ArrowClickHouseClient::new(
        "http://127.0.0.1:1",
        "default",
        "x",
        None,
        &std::collections::HashMap::new(),
    )
}

async fn await_state(watcher: &Arc<SchemaWatcher>, target: SchemaState) {
    timeout(WAIT_LIMIT, async {
        while watcher.current() != target {
            sleep(POLL).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("watcher never reached {target:?}"));
}

#[tokio::test]
async fn ready_returns_503_when_schema_pending() {
    let watcher = SchemaWatcher::for_state(SchemaState::Pending, 1);
    let router = create_router(dummy_clickhouse(), None, watcher);

    let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(unhealthy_components(&json).contains(&"schema_pending".to_string()));
}

#[tokio::test]
async fn ready_returns_503_when_schema_outdated() {
    let watcher = SchemaWatcher::for_state(SchemaState::Outdated, 1);
    let router = create_router(dummy_clickhouse(), None, watcher);

    let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert!(unhealthy_components(&json).contains(&"schema_outdated".to_string()));
}

#[tokio::test]
async fn watcher_transitions_to_ready_when_active_version_matches() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();

    let shutdown = CancellationToken::new();
    let watcher = SchemaWatcher::spawn(client.clone(), 1, POLL, shutdown.clone());
    assert_eq!(watcher.current(), SchemaState::Pending);

    write_schema_version(&client, 1).await.unwrap();
    await_state(&watcher, SchemaState::Ready).await;

    shutdown.cancel();
}

#[tokio::test]
async fn watcher_transitions_to_outdated_and_cancels_shutdown() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();
    write_schema_version(&client, 5).await.unwrap();

    let shutdown = CancellationToken::new();
    let watcher = SchemaWatcher::spawn(client, 1, POLL, shutdown.clone());

    timeout(WAIT_LIMIT, shutdown.cancelled())
        .await
        .expect("shutdown token should fire when binary is too old");
    assert_eq!(watcher.current(), SchemaState::Outdated);
}

#[tokio::test]
async fn watcher_stays_pending_when_active_version_lower_than_binary() {
    let ctx = TestContext::new(&[]).await;
    let client = ctx.create_client();
    ensure_version_table(&client).await.unwrap();
    write_schema_version(&client, 1).await.unwrap();

    let shutdown = CancellationToken::new();
    let watcher = SchemaWatcher::spawn(client, 5, POLL, shutdown.clone());

    sleep(POLL * 5).await;
    assert_eq!(watcher.current(), SchemaState::Pending);
    assert!(!shutdown.is_cancelled());

    shutdown.cancel();
}
