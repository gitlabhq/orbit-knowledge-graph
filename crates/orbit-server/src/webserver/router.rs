use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Json, Router, routing::get};
use labkit::http::{CorrelationLayer, GitlabTraceLayer, HttpMetricsLayer};
use serde::Serialize;

use crate::schema_watcher::{SchemaState, SchemaWatcher};

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    unhealthy_components: Vec<&'static str>,
}

fn version() -> &'static str {
    orbit_utils::version::get()
}

async fn live() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: version(),
        unhealthy_components: Vec::new(),
    })
}

async fn ready(State(schema_watcher): State<Arc<SchemaWatcher>>) -> impl IntoResponse {
    let mut unhealthy_components = Vec::new();

    match schema_watcher.current() {
        SchemaState::Ready => {}
        SchemaState::Pending => unhealthy_components.push("schema_pending"),
        SchemaState::Outdated => unhealthy_components.push("schema_outdated"),
        SchemaState::Migrating => unhealthy_components.push("schema_migrating"),
    }

    let healthy = unhealthy_components.is_empty();
    let status_code = if healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let label = if healthy {
        "ok"
    } else if unhealthy_components == ["schema_migrating"] {
        "migrating"
    } else {
        "unhealthy"
    };

    (
        status_code,
        Json(HealthResponse {
            status: label,
            version: version(),
            unhealthy_components,
        }),
    )
}

pub fn create_router(schema_watcher: Arc<SchemaWatcher>) -> Router {
    Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .with_state(schema_watcher)
        .layer(HttpMetricsLayer::new())
        .layer(GitlabTraceLayer::new())
        .layer(CorrelationLayer::new())
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;

    fn ready_watcher() -> Arc<SchemaWatcher> {
        SchemaWatcher::for_state(SchemaState::Ready)
    }

    fn request(path: &str) -> Request<Body> {
        Request::get(path).body(Body::empty()).unwrap()
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
    async fn live_returns_ok() {
        let router = create_router(ready_watcher());

        let (status, json) = parse_response(router.oneshot(request("/live")).await.unwrap()).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "ok");
        assert!(json["version"].is_string());
    }

    #[tokio::test]
    async fn ready_returns_ok_when_schema_is_ready() {
        let router = create_router(ready_watcher());

        let (status, json) = parse_response(router.oneshot(request("/ready")).await.unwrap()).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "ok");
        assert!(json.get("unhealthy_components").is_none());
    }
}
