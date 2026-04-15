use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use clickhouse_client::ArrowClickHouseClient;
use gitlab_client::GitlabClient;
use gkg_server::webserver::create_router;
use gkg_server_config::GitlabClientConfiguration;
use integration_testkit::{GRAPH_SCHEMA_SQL, TestContext};
use tower::ServiceExt;

fn live_request() -> Request<Body> {
    Request::get("/live").body(Body::empty()).unwrap()
}

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

#[tokio::test]
async fn live_returns_ok() {
    let client = ArrowClickHouseClient::new(
        "http://127.0.0.1:1",
        "default",
        "x",
        None,
        &std::collections::HashMap::new(),
    );
    let router = create_router(client, None);

    let (status, json) = parse_response(router.oneshot(live_request()).await.unwrap()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "ok");
    assert!(json["version"].is_string());
}

#[tokio::test]
async fn ready_returns_ok_when_clickhouse_healthy() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    let router = create_router(ctx.create_client(), None);

    let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "ok");
    assert!(
        json.get("unhealthy_components")
            .and_then(|v| v.as_array())
            .is_none_or(|a| a.is_empty())
    );
}

#[tokio::test]
async fn ready_returns_503_when_clickhouse_unreachable() {
    let client = ArrowClickHouseClient::new(
        "http://127.0.0.1:1",
        "default",
        "x",
        None,
        &std::collections::HashMap::new(),
    );
    let router = create_router(client, None);

    let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json["status"], "unhealthy");

    let components: Vec<String> = json["unhealthy_components"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    assert!(components.contains(&"clickhouse_graph".to_string()));
}

// ─────────────────────────────────────────────────────────────────────────────
// GitLab readiness probe
// ─────────────────────────────────────────────────────────────────────────────

fn unreachable_gitlab_client() -> Arc<GitlabClient> {
    Arc::new(
        GitlabClient::new(GitlabClientConfiguration {
            base_url: "http://127.0.0.1:1".to_string(),
            signing_key: BASE64.encode(b"test-secret-that-is-long-enough!"),
            resolve_host: None,
        })
        .unwrap(),
    )
}

#[tokio::test]
async fn ready_skips_gitlab_probe_when_no_client() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    let router = create_router(ctx.create_client(), None);

    let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;

    assert_eq!(status, StatusCode::OK);
    let components = json
        .get("unhealthy_components")
        .and_then(|v| v.as_array())
        .cloned()
        .unwrap_or_default();
    assert!(
        !components.iter().any(|v| v.as_str() == Some("gitlab")),
        "gitlab should not appear when no client configured"
    );
}

#[tokio::test]
async fn ready_reports_gitlab_unhealthy_when_unreachable() {
    let ctx = TestContext::new(&[*GRAPH_SCHEMA_SQL]).await;
    let router = create_router(ctx.create_client(), Some(unreachable_gitlab_client()));

    let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let components: Vec<String> = json["unhealthy_components"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|v| v.as_str().map(String::from))
        .collect();
    assert!(
        components.contains(&"gitlab".to_string()),
        "gitlab should be unhealthy when unreachable"
    );
}
