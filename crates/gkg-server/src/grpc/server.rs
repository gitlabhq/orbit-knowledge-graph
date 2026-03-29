use std::net::SocketAddr;
use std::sync::Arc;

use clickhouse_client::ClickHouseConfiguration;
use gitlab_client::GitlabClient;
use ontology::Ontology;
use tonic::transport::Server as TonicServer;
use tonic::transport::server::ServerTlsConfig;
use tracing::info;

use crate::auth::JwtValidator;
use crate::cluster_health::ClusterHealthChecker;
use crate::proto::knowledge_graph_service_server::KnowledgeGraphServiceServer;

use super::service::KnowledgeGraphServiceImpl;

pub struct GrpcServer {
    addr: SocketAddr,
    service: KnowledgeGraphServiceServer<KnowledgeGraphServiceImpl>,
    tls_config: Option<ServerTlsConfig>,
}

impl GrpcServer {
    pub fn new(
        addr: SocketAddr,
        validator: Arc<JwtValidator>,
        ontology: Arc<Ontology>,
        clickhouse_config: &ClickHouseConfiguration,
        cluster_health: Arc<ClusterHealthChecker>,
        tls_config: Option<ServerTlsConfig>,
        gitlab_client: Option<Arc<GitlabClient>>,
    ) -> Self {
        let service = KnowledgeGraphServiceImpl::new(
            validator,
            ontology,
            clickhouse_config,
            cluster_health,
            gitlab_client,
        );
        Self {
            addr,
            service: KnowledgeGraphServiceServer::new(service),
            tls_config,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn run(self) -> Result<(), tonic::transport::Error> {
        let tls_enabled = self.tls_config.is_some();
        info!(addr = %self.addr, tls = tls_enabled, "Starting gRPC server");

        let mut builder = TonicServer::builder();
        if let Some(tls) = self.tls_config {
            builder = builder.tls_config(tls)?;
        }

        builder
            .layer(labkit::grpc::GrpcMetricsLayer::new())
            .layer(labkit::grpc::GrpcTraceLayer::new())
            .layer(labkit::grpc::GrpcCorrelationLayer::new())
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
        let ontology = Arc::new(Ontology::load_embedded().expect("ontology must load"));
        let clickhouse_config = ClickHouseConfiguration::default();
        let cluster_health = ClusterHealthChecker::default().into_arc();
        let server = GrpcServer::new(
            addr,
            validator,
            ontology,
            &clickhouse_config,
            cluster_health,
            None,
            None,
        );
        assert_eq!(server.addr(), addr);
    }
}
