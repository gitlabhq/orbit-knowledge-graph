use axum::body::Body;
use axum::http::{Request, StatusCode};
use clickhouse_client::ArrowClickHouseClient;
use gkg_server::webserver::create_router;
use integration_testkit::TestContext;
use tower::ServiceExt;

const GRAPH_SCHEMA_SQL: &str = include_str!("../../../../config/graph.sql");

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
    let client = ArrowClickHouseClient::new("http://127.0.0.1:1", "default", "x", None);
    let router = create_router(client);

    let (status, json) = parse_response(router.oneshot(live_request()).await.unwrap()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(json["status"], "ok");
    assert!(json["version"].is_string());
}

#[tokio::test]
async fn ready_returns_ok_when_clickhouse_healthy() {
    let ctx = TestContext::new(&[GRAPH_SCHEMA_SQL]).await;
    let router = create_router(ctx.create_client());

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
    let client = ArrowClickHouseClient::new("http://127.0.0.1:1", "default", "x", None);
    let router = create_router(client);

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
