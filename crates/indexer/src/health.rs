use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};

use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use serde::Serialize;
use tokio::net::TcpListener;
use tracing::info;

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

async fn ready(State(serving): State<std::sync::Arc<AtomicBool>>) -> impl IntoResponse {
    if !serving.load(Ordering::Relaxed) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(HealthResponse {
                status: "starting",
                version: version(),
                unhealthy_components: vec!["schema_gate"],
            }),
        );
    }

    (
        StatusCode::OK,
        Json(HealthResponse {
            status: "ok",
            version: version(),
            unhealthy_components: Vec::new(),
        }),
    )
}

pub fn create_health_router(serving: std::sync::Arc<AtomicBool>) -> Router {
    Router::new()
        .route("/live", get(live))
        .route("/ready", get(ready))
        .with_state(serving)
}

pub async fn run_health_server(
    bind_address: SocketAddr,
    serving: std::sync::Arc<AtomicBool>,
) -> Result<(), std::io::Error> {
    let app = create_health_router(serving);

    let listener = TcpListener::bind(bind_address).await?;

    info!(%bind_address, "indexer health server listening");

    axum::serve(listener, app).await
}
