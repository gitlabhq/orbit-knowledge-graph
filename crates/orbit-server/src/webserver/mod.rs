mod health_client;
mod router;

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tracing::info;

use crate::schema_watcher::SchemaWatcher;

pub use health_client::InfrastructureHealthClient;
pub use router::create_router;

pub struct Server {
    listener: TcpListener,
    router: axum::Router,
}

impl Server {
    pub async fn bind(
        addr: SocketAddr,
        schema_watcher: Arc<SchemaWatcher>,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let router = create_router(schema_watcher);
        Ok(Self { listener, router })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn run(self) -> std::io::Result<()> {
        info!("listening on {}", self.listener.local_addr()?);
        axum::serve(self.listener, self.router).await
    }
}
