use axum::{Json, Router, routing::get};
use labkit_rs::correlation::http::{CorrelationIdLayer, PropagateCorrelationIdLayer};
use labkit_rs::metrics::http::HttpMetricsLayer;
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
        .layer(HttpMetricsLayer::new())
        .layer(CorrelationIdLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(PropagateCorrelationIdLayer::new())
}
