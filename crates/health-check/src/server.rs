use std::net::SocketAddr;
use std::sync::Arc;

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use tokio::net::TcpListener;
use tracing::info;

use crate::checker::HealthChecker;
use crate::error::Error;
use crate::types::HealthStatus;

async fn health(State(checker): State<Arc<HealthChecker>>) -> Json<HealthStatus> {
    Json(checker.check().await)
}

async fn queue_depth(State(checker): State<Arc<HealthChecker>>) -> impl IntoResponse {
    match checker.queue_depth().await {
        Ok(depth) => (StatusCode::OK, Json(depth)).into_response(),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(serde_json::json!({ "error": error })),
        )
            .into_response(),
    }
}

pub async fn run_server(bind_address: SocketAddr, checker: HealthChecker) -> Result<(), Error> {
    let checker = Arc::new(checker);

    let app = Router::new()
        .route("/health", get(health))
        .route("/queue-depth", get(queue_depth))
        .with_state(checker);

    let listener = TcpListener::bind(bind_address)
        .await
        .map_err(|e| Error::Config(format!("Failed to bind to {}: {}", bind_address, e)))?;

    info!(%bind_address, "Health check server listening");

    axum::serve(listener, app)
        .await
        .map_err(|e| Error::Config(format!("Server error: {}", e)))
}
