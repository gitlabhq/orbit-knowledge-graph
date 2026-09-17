mod health_client;
mod router;

use std::net::{SocketAddr, TcpListener};
use std::sync::Arc;

use labkit::tls::ServerTls;

use crate::active_schema::ActiveSchema;

pub use health_client::InfrastructureHealthClient;
pub use router::create_router;

pub struct Server {
    listener: TcpListener,
    router: axum::Router,
    tls: Option<ServerTls>,
}

impl Server {
    pub fn bind(
        addr: SocketAddr,
        active_schema: Arc<ActiveSchema>,
        tls: Option<ServerTls>,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        let router = create_router(active_schema);
        Ok(Self {
            listener,
            router,
            tls,
        })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.listener.local_addr()
    }

    pub async fn run(self) -> std::io::Result<()> {
        labkit::server::serve(self.listener, self.router, self.tls).await
    }
}
