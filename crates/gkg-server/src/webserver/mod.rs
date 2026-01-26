mod auth;
pub mod handlers;
mod router;
mod tools;

pub use auth::{AuthenticatedUser, Claims, JwtValidator, auth_middleware};
pub use router::create_router;
pub use tools::{KnowledgeGraphTool, ToolError, ToolRegistry, ToolResult};

use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

use crate::config::ServerConfig;
use crate::error::ServerError;

pub struct WebServer {
    listener: TcpListener,
    router: axum::Router,
}

impl WebServer {
    pub async fn new(config: &ServerConfig, registry: ToolRegistry) -> Result<Self, ServerError> {
        let jwt_validator = JwtValidator::new(config)?;
        let listener = TcpListener::bind(&config.bind_address).await?;
        let router = create_router(Arc::new(registry), jwt_validator);
        Ok(Self { listener, router })
    }

    pub async fn run(self) -> Result<(), ServerError> {
        let addr = self.listener.local_addr()?;
        info!("Webserver listening on {}", addr);
        axum::serve(self.listener, self.router)
            .await
            .map_err(|e| ServerError::Server(e.to_string()))
    }

    pub async fn run_until_stopped(
        self,
        shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), ServerError> {
        let addr = self.listener.local_addr()?;
        info!("Webserver listening on {}", addr);
        axum::serve(self.listener, self.router)
            .with_graceful_shutdown(shutdown)
            .await
            .map_err(|e| ServerError::Server(e.to_string()))
    }
}
