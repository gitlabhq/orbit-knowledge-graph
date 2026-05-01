use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use gkg_server_config::{ClickHouseConfiguration, GrpcConfig};
use ontology::Ontology;
use query_engine::shared::content::ColumnResolverRegistry;
use tonic::transport::Server as TonicServer;
use tonic::transport::server::ServerTlsConfig;
use tracing::info;

use crate::auth::JwtValidator;
use crate::cluster_health::ClusterHealthChecker;
use crate::proto::knowledge_graph_service_server::KnowledgeGraphServiceServer;
use gkg_billing::BillingTracker;

use super::service::KnowledgeGraphServiceImpl;

pub struct GrpcServer {
    addr: SocketAddr,
    service: KnowledgeGraphServiceImpl,
    tls_config: Option<ServerTlsConfig>,
    grpc_config: GrpcConfig,
}

impl GrpcServer {
    pub fn new(
        addr: SocketAddr,
        validator: Arc<JwtValidator>,
        ontology: Arc<Ontology>,
        clickhouse_config: &ClickHouseConfiguration,
        cluster_health: Arc<ClusterHealthChecker>,
        tls_config: Option<ServerTlsConfig>,
        grpc_config: GrpcConfig,
    ) -> Self {
        let service = KnowledgeGraphServiceImpl::new(
            validator,
            ontology,
            clickhouse_config,
            cluster_health,
            grpc_config.stream_timeout_secs,
        );
        Self {
            addr,
            service,
            tls_config,
            grpc_config,
        }
    }

    pub fn with_resolver_registry(mut self, registry: Arc<ColumnResolverRegistry>) -> Self {
        self.service = self.service.with_resolver_registry(registry);
        self
    }

    pub fn with_cache_broker(mut self, broker: Arc<nats_client::NatsClient>) -> Self {
        self.service = self.service.with_cache_broker(broker);
        self
    }

    pub fn with_billing(mut self, tracker: Arc<dyn BillingTracker>) -> Self {
        self.service = self.service.with_billing(tracker);
        self
    }

    pub fn with_indexing_status(
        mut self,
        store: indexer::indexing_status::IndexingStatusStore,
    ) -> Self {
        self.service = self.service.with_indexing_status(store);
        self
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn run(self) -> Result<(), tonic::transport::Error> {
        let tls_enabled = self.tls_config.is_some();
        info!(addr = %self.addr, tls = tls_enabled, "Starting gRPC server");

        let gc = &self.grpc_config;
        let mut builder = TonicServer::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(gc.keepalive_interval_secs)))
            .http2_keepalive_timeout(Some(Duration::from_secs(gc.keepalive_timeout_secs)))
            .tcp_keepalive(Some(Duration::from_secs(gc.tcp_keepalive_secs)))
            .initial_connection_window_size(gc.connection_window_size)
            .initial_stream_window_size(gc.stream_window_size)
            .concurrency_limit_per_connection(gc.concurrency_limit)
            .max_connection_age(Duration::from_secs(gc.max_connection_age_secs))
            .max_connection_age_grace(Duration::from_secs(gc.max_connection_age_grace_secs));
        if let Some(tls) = self.tls_config {
            builder = builder.tls_config(tls)?;
        }

        builder
            .layer(labkit::grpc::GrpcMetricsLayer::new())
            .layer(labkit::grpc::GrpcTraceLayer::new())
            .layer(labkit::grpc::GrpcCorrelationLayer::new())
            .add_service(KnowledgeGraphServiceServer::new(self.service))
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
            GrpcConfig::default(),
        );
        assert_eq!(server.addr(), addr);
    }
}
