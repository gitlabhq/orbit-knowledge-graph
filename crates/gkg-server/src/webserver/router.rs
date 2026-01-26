use axum::{Json, Router, routing::get};
use labkit_rs::correlation::http::{CorrelationIdLayer, PropagateCorrelationIdLayer};
use serde::Serialize;
use tower_http::trace::TraceLayer;

use crate::auth::JwtValidator;
use crate::cli::Mode;

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

pub fn create_router(mode: Mode, _validator: JwtValidator) -> Router {
    let router = Router::new().route("/health", get(health));

    let router = match mode {
        Mode::Webserver => router,
        Mode::Indexer => router,
    };

    router
        .layer(CorrelationIdLayer::new())
        .layer(TraceLayer::new_for_http())
        .layer(PropagateCorrelationIdLayer::new())
}
