use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use gkg_server::schema_watcher::{SchemaState, SchemaWatcher};
use gkg_server::webserver::create_router;
use tower::ServiceExt;

fn ready_watcher() -> Arc<SchemaWatcher> {
    SchemaWatcher::for_state(SchemaState::Ready)
}

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
    let router = create_router(ready_watcher());

    let (status, json) = parse_response(router.oneshot(live_request()).await.unwrap()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "ok");
    assert!(json["version"].is_string());
}

#[tokio::test]
async fn ready_returns_ok_when_schema_is_ready() {
    let router = create_router(ready_watcher());

    let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "ok");
    assert!(
        json.get("unhealthy_components")
            .and_then(|v| v.as_array())
            .is_none_or(|a| a.is_empty())
    );
}
