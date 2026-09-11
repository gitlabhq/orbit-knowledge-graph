use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Json, Router, routing::get};
use labkit::http::{CorrelationLayer, GitlabTraceLayer, HttpMetricsLayer};
use serde::Serialize;

use crate::active_schema::ActiveSchema;

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

async fn ready(State(active_schema): State<Arc<ActiveSchema>>) -> impl IntoResponse {
    let healthy = active_schema.snapshot().is_ok();
    let (status_code, label, unhealthy_components) = if healthy {
        (StatusCode::OK, "ok", Vec::new())
    } else {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            "unhealthy",
            vec!["schema_pending"],
        )
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

pub fn create_router(active_schema: Arc<ActiveSchema>) -> Router {
    Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .with_state(active_schema)
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

    fn pinned_schema() -> Arc<ActiveSchema> {
        ActiveSchema::pinned(Arc::new(ontology::Ontology::load_embedded().unwrap()))
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
        let router = create_router(pinned_schema());

        let (status, json) = parse_response(router.oneshot(request("/live")).await.unwrap()).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "ok");
        assert!(json["version"].is_string());
    }

    #[tokio::test]
    async fn ready_returns_ok_when_schema_is_ready() {
        let router = create_router(pinned_schema());

        let (status, json) = parse_response(router.oneshot(request("/ready")).await.unwrap()).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(json["status"], "ok");
        assert!(json.get("unhealthy_components").is_none());
    }
}
