//! gRPC server setup for the Knowledge Graph service.

use std::net::SocketAddr;
use std::sync::Arc;

use tonic::transport::Server as TonicServer;
use tracing::info;

use crate::auth::JwtValidator;
use crate::proto::knowledge_graph_service_server::KnowledgeGraphServiceServer;

use super::service::KnowledgeGraphServiceImpl;

/// gRPC server for the Knowledge Graph service
pub struct Server {
    addr: SocketAddr,
    service: KnowledgeGraphServiceServer<KnowledgeGraphServiceImpl>,
}

impl Server {
    /// Create a new gRPC server bound to the given address
    pub fn new(addr: SocketAddr, validator: Arc<JwtValidator>) -> Self {
        let service = KnowledgeGraphServiceImpl::new(validator);
        Self {
            addr,
            service: KnowledgeGraphServiceServer::new(service),
        }
    }

    /// Get the address the server will bind to
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Run the gRPC server
    pub async fn run(self) -> Result<(), tonic::transport::Error> {
        info!(addr = %self.addr, "Starting gRPC server");

        TonicServer::builder()
            .add_service(self.service)
            .serve(self.addr)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn test_server_creation() {
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50051);
        // Secret must be at least 32 bytes
        let validator =
            Arc::new(JwtValidator::new("test-secret-that-is-at-least-32-bytes-long", 0).unwrap());

        let server = Server::new(addr, validator);
        assert_eq!(server.addr(), addr);
    }
}
