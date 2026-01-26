use std::sync::Arc;

use axum::{
    Router, middleware,
    routing::{get, post},
};
use tower_http::trace::TraceLayer;

use super::auth::{JwtValidator, auth_middleware};
use super::handlers::{AppState, call_tool, health, list_tools};
use super::tools::ToolRegistry;

pub fn create_router(registry: Arc<ToolRegistry>, jwt_validator: JwtValidator) -> Router {
    let state = AppState { registry };

    let api = Router::new()
        .route("/tools", get(list_tools))
        .route("/tools/{name}/call", post(call_tool))
        .with_state(state)
        .layer(middleware::from_fn_with_state(
            jwt_validator,
            auth_middleware,
        ));

    Router::new()
        .route("/health", get(health))
        .nest("/api/v1", api)
        .layer(TraceLayer::new_for_http())
}
