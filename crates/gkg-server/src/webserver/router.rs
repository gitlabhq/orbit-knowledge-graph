use axum::{Json, Router, routing::get};
use labkit_rs::correlation::http::{CorrelationIdLayer, PropagateCorrelationIdLayer};
use serde::Serialize;
use tower_http::trace::TraceLayer;

use crate::webserver::JwtValidator;

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}

pub fn create_router(_validator: JwtValidator) -> Router {
    Router::new()
        .route("/health", get(health))
        .layer(CorrelationIdLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(PropagateCorrelationIdLayer::new())
}
