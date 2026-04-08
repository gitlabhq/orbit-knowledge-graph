use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::{Json, Router, routing::get};
use clickhouse_client::ArrowClickHouseClient;
use gitlab_client::GitlabClient;
use labkit::http::{CorrelationLayer, GitlabTraceLayer, HttpMetricsLayer};
use serde::Serialize;
use tokio::time::timeout;

const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct AppState {
    pub graph_client: ArrowClickHouseClient,
    pub gitlab_client: Option<Arc<GitlabClient>>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    unhealthy_components: Vec<&'static str>,
}

fn version() -> &'static str {
    use std::sync::OnceLock;
    static VERSION: OnceLock<String> = OnceLock::new();
    VERSION
        .get_or_init(|| {
            std::env::var("GKG_VERSION")
                .ok()
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| env!("CARGO_PKG_VERSION").to_string())
        })
        .as_str()
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

    // Checks both connectivity AND auth. A 401 means the JWT secret is
    // wrong or expired -- that's unhealthy, not just "unreachable".
    // Only Ok and NotFound count as healthy (matches indexer behavior).
    let gitlab_healthy = match &state.gitlab_client {
        Some(client) => timeout(HEALTH_CHECK_TIMEOUT, client.project_info(1))
            .await
            .is_ok_and(|r| {
                matches!(
                    r,
                    Ok(_) | Err(gitlab_client::GitlabClientError::NotFound(_))
                )
            }),
        None => true,
    };

    let mut unhealthy_components = Vec::new();
    if !graph_healthy {
        unhealthy_components.push("clickhouse_graph");
    }
    if !gitlab_healthy {
        unhealthy_components.push("gitlab");
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

pub fn create_router(
    graph_client: ArrowClickHouseClient,
    gitlab_client: Option<Arc<GitlabClient>>,
) -> Router {
    let state = AppState {
        graph_client,
        gitlab_client,
    };

    Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .with_state(state)
        .layer(HttpMetricsLayer::new())
        .layer(GitlabTraceLayer::new())
        .layer(CorrelationLayer::new())
}
