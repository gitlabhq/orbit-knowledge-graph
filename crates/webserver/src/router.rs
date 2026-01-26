use std::sync::Arc;

use axum::{
    Router,
    middleware,
    routing::{get, post},
};
use tower_http::trace::TraceLayer;

use crate::auth::{JwtValidator, auth_middleware};
use crate::handlers::{ToolsState, call_tool, health_check, list_tools};
use crate::tools::ToolRegistry;

pub fn create_router(registry: Arc<ToolRegistry>, jwt_validator: JwtValidator) -> Router {
    let tools_state = ToolsState { registry };

    let api_routes = Router::new()
        .route("/tools", get(list_tools))
        .route("/tools/{name}/call", post(call_tool))
        .with_state(tools_state)
        .layer(middleware::from_fn_with_state(
            jwt_validator,
            auth_middleware,
        ));

    Router::new()
        .route("/health", get(health_check))
        .nest("/api/v1", api_routes)
        .layer(TraceLayer::new_for_http())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::WebserverConfig;

    fn create_test_config() -> WebserverConfig {
        WebserverConfig {
            bind_address: "0.0.0.0:8080".to_string(),
            jwt_secret: "test-secret".to_string(),
            jwt_issuer: "gitlab".to_string(),
            jwt_audience: "gitlab-knowledge-graph".to_string(),
            jwt_clock_skew_secs: 60,
        }
    }

    #[test]
    fn test_create_router() {
        let config = create_test_config();
        let registry = Arc::new(ToolRegistry::new());
        let validator = JwtValidator::new(&config).unwrap();

        let _router = create_router(registry, validator);
    }
}
