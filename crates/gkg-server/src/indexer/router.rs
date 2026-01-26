use std::sync::Arc;

use axum::{Router, middleware, routing::get};
use tower_http::trace::TraceLayer;

use super::metrics::metrics;
use crate::webserver::{
    JwtValidator, ToolRegistry, auth_middleware,
    handlers::{AppState, health, list_tools},
};

pub fn create_router(registry: Arc<ToolRegistry>, jwt_validator: JwtValidator) -> Router {
    let state = AppState { registry };

    let api = Router::new()
        .route("/tools", get(list_tools))
        .with_state(state)
        .layer(middleware::from_fn_with_state(
            jwt_validator,
            auth_middleware,
        ));

    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .nest("/api/v1", api)
        .layer(TraceLayer::new_for_http())
}
