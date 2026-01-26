mod metrics;
mod router;

pub use router::create_router;

use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::info;

use crate::config::ServerConfig;
use crate::error::ServerError;
use crate::webserver::ToolRegistry;

pub struct IndexerServer {
    listener: TcpListener,
    router: axum::Router,
}

impl IndexerServer {
    pub async fn new(config: &ServerConfig, registry: ToolRegistry) -> Result<Self, ServerError> {
        let jwt_validator = crate::webserver::JwtValidator::new(config)?;
        let listener = TcpListener::bind(&config.bind_address).await?;
        let router = create_router(Arc::new(registry), jwt_validator);
        Ok(Self { listener, router })
    }

    pub async fn run_until_stopped(
        self,
        shutdown: impl std::future::Future<Output = ()> + Send + 'static,
    ) -> Result<(), ServerError> {
        let addr = self.listener.local_addr()?;
        info!("Indexer server listening on {}", addr);
        axum::serve(self.listener, self.router)
            .with_graceful_shutdown(shutdown)
            .await
            .map_err(|e| ServerError::Server(e.to_string()))
    }
}
