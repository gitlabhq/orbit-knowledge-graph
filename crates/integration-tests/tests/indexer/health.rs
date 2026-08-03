use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use indexer::health::create_health_router;
use tower::ServiceExt;

fn ready_request() -> Request<Body> {
    Request::get("/ready").body(Body::empty()).unwrap()
}

async fn parse_response(response: axum::response::Response) -> (StatusCode, serde_json::Value) {
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), 4096)
        .await
        .unwrap();
    let json = serde_json::from_slice(&body).unwrap();
    (status, json)
}

#[tokio::test]
async fn readiness_probe_is_ok_when_serving() {
    let router = create_health_router(Arc::new(AtomicBool::new(true)));

    let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "ok");
    assert!(json.get("unhealthy_components").is_none());
}

#[tokio::test]
async fn readiness_probe_is_unavailable_until_serving() {
    let router = create_health_router(Arc::new(AtomicBool::new(false)));

    let (status, json) = parse_response(router.oneshot(ready_request()).await.unwrap()).await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(json["status"], "starting");
    assert_eq!(
        json["unhealthy_components"],
        serde_json::json!(["schema_gate"])
    );
}
