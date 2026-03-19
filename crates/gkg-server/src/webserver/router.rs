use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Json, Router, routing::get};
use clickhouse_client::ArrowClickHouseClient;
use labkit::http::{CorrelationLayer, GitlabTraceLayer, HttpMetricsLayer};
use serde::Serialize;
use tokio::time::timeout;

const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct AppState {
    pub graph_client: ArrowClickHouseClient,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    unhealthy_components: Vec<&'static str>,
}

fn version() -> &'static str {
    match option_env!("GKG_VERSION") {
        Some(v) => v,
        None => env!("CARGO_PKG_VERSION"),
    }
}

async fn live() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: version(),
        unhealthy_components: Vec::new(),
    })
}

async fn ready(State(state): State<AppState>) -> impl IntoResponse {
    let graph_healthy = timeout(HEALTH_CHECK_TIMEOUT, state.graph_client.execute("SELECT 1"))
        .await
        .is_ok_and(|r| r.is_ok());

    let mut unhealthy_components = Vec::new();
    if !graph_healthy {
        unhealthy_components.push("clickhouse_graph");
    }

    let healthy = unhealthy_components.is_empty();
    let status_code = if healthy {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    let label = if healthy { "ok" } else { "unhealthy" };

    (
        status_code,
        Json(HealthResponse {
            status: label,
            version: version(),
            unhealthy_components,
        }),
    )
}

pub fn create_router(graph_client: ArrowClickHouseClient) -> Router {
    let state = AppState { graph_client };

    Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .with_state(state)
        .layer(HttpMetricsLayer::new())
        .layer(GitlabTraceLayer::new())
        .layer(CorrelationLayer::new())
}
