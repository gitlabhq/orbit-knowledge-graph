use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::info;

use crate::auth::JwtValidator;
use crate::config::WebserverConfig;
use crate::error::WebserverError;
use crate::router::create_router;
use crate::tools::ToolRegistry;

pub struct Server {
    listener: TcpListener,
    router: axum::Router,
}

impl Server {
    pub async fn run(self) -> Result<(), WebserverError> {
        let addr = self.listener.local_addr().map_err(WebserverError::Io)?;
        info!("Server listening on {}", addr);

        axum::serve(self.listener, self.router)
            .await
            .map_err(|e| WebserverError::Server(e.to_string()))?;

        Ok(())
    }

    pub async fn run_until_stopped(
        self,
        shutdown_signal: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), WebserverError> {
        let addr = self.listener.local_addr().map_err(WebserverError::Io)?;
        info!("Server listening on {}", addr);

        axum::serve(self.listener, self.router)
            .with_graceful_shutdown(shutdown_signal)
            .await
            .map_err(|e| WebserverError::Server(e.to_string()))?;

        info!("Server shutdown complete");
        Ok(())
    }

    pub fn local_addr(&self) -> Result<SocketAddr, WebserverError> {
        self.listener.local_addr().map_err(WebserverError::Io)
    }
}

pub struct ServerBuilder {
    config: WebserverConfig,
    registry: ToolRegistry,
}

impl ServerBuilder {
    pub fn new(config: WebserverConfig) -> Self {
        Self {
            config,
            registry: ToolRegistry::new(),
        }
    }

    pub fn with_registry(mut self, registry: ToolRegistry) -> Self {
        self.registry = registry;
        self
    }

    pub async fn build(self) -> Result<Server, WebserverError> {
        let jwt_validator = JwtValidator::new(&self.config)?;

        let listener = TcpListener::bind(&self.config.bind_address)
            .await
            .map_err(WebserverError::Io)?;

        let router = create_router(Arc::new(self.registry), jwt_validator);

        Ok(Server { listener, router })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> WebserverConfig {
        WebserverConfig {
            bind_address: "127.0.0.1:0".to_string(),
            jwt_secret: "test-secret".to_string(),
            jwt_issuer: "gitlab".to_string(),
            jwt_audience: "gitlab-knowledge-graph".to_string(),
            jwt_clock_skew_secs: 60,
        }
    }

    #[tokio::test]
    async fn test_server_builder() {
        let config = create_test_config();
        let result = ServerBuilder::new(config).build().await;

        assert!(result.is_ok());
        let server = result.unwrap();
        let addr = server.local_addr().unwrap();
        assert!(addr.port() > 0);
    }

    #[tokio::test]
    async fn test_server_builder_with_registry() {
        let config = create_test_config();
        let registry = ToolRegistry::new();

        let result = ServerBuilder::new(config)
            .with_registry(registry)
            .build()
            .await;

        assert!(result.is_ok());
    }
}
