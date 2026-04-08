mod health_client;
mod router;

use std::net::SocketAddr;
use std::sync::Arc;

use clickhouse_client::ArrowClickHouseClient;
use gitlab_client::GitlabClient;
use tokio::net::TcpListener;
use tracing::info;

pub use health_client::InfrastructureHealthClient;
pub use router::create_router;

pub struct Server {
    listener: TcpListener,
    router: axum::Router,
}

impl Server {
    pub async fn bind(
        addr: SocketAddr,
        graph_client: ArrowClickHouseClient,
        gitlab_client: Option<Arc<GitlabClient>>,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let router = create_router(graph_client, gitlab_client);
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
