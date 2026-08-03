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
    gkg_utils::version::get()
}

async fn live() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: version(),
        unhealthy_components: Vec::new(),
    })
}

async fn ready(State(schema_watcher): State<std::sync::Arc<SchemaWatcher>>) -> impl IntoResponse {
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

pub fn create_router(schema_watcher: std::sync::Arc<SchemaWatcher>) -> Router {
    Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .with_state(schema_watcher)
        .layer(HttpMetricsLayer::new())
        .layer(GitlabTraceLayer::new())
        .layer(CorrelationLayer::new())
}
