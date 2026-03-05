use std::net::SocketAddr;
use std::sync::Arc;

use clickhouse_client::ClickHouseConfiguration;
use labkit_rs::correlation::grpc::server_interceptor;
use tonic::transport::Server as TonicServer;
use tracing::info;

use crate::auth::JwtValidator;
use crate::cluster_health::ClusterHealthChecker;
use crate::proto::knowledge_graph_service_server::KnowledgeGraphServiceServer;

use super::service::KnowledgeGraphServiceImpl;

type Interceptor = fn(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>;
type ServiceWithInterceptor = tonic::service::interceptor::InterceptedService<
    KnowledgeGraphServiceServer<KnowledgeGraphServiceImpl>,
    Interceptor,
>;

pub struct GrpcServer {
    addr: SocketAddr,
    service: ServiceWithInterceptor,
}

impl GrpcServer {
    pub fn new(
        addr: SocketAddr,
        validator: Arc<JwtValidator>,
        clickhouse_config: &ClickHouseConfiguration,
        cluster_health: Arc<ClusterHealthChecker>,
    ) -> Self {
        let service = KnowledgeGraphServiceImpl::new(validator, clickhouse_config, cluster_health);
        Self {
            addr,
            service: KnowledgeGraphServiceServer::with_interceptor(service, server_interceptor),
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

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
        let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 50054);
        let validator =
            Arc::new(JwtValidator::new("test-secret-that-is-at-least-32-bytes-long", 0).unwrap());
        let clickhouse_config = ClickHouseConfiguration::default();

        let cluster_health = ClusterHealthChecker::default().into_arc();
        let server = GrpcServer::new(addr, validator, &clickhouse_config, cluster_health);
        assert_eq!(server.addr(), addr);
    }
}
