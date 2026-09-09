use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use orbit_server_config::GrpcConfig;
use tonic::transport::Server as TonicServer;
use tonic::transport::server::ServerTlsConfig;
use tracing::info;

use crate::proto::orbit_service_server::OrbitServiceServer;

use super::service::OrbitServiceImpl;

pub struct GrpcServer {
    addr: SocketAddr,
    service: OrbitServiceImpl,
    tls_config: Option<ServerTlsConfig>,
    grpc_config: GrpcConfig,
}

impl GrpcServer {
    pub fn new(
        addr: SocketAddr,
        service: OrbitServiceImpl,
        tls_config: Option<ServerTlsConfig>,
        grpc_config: GrpcConfig,
    ) -> Self {
        Self {
            addr,
            service,
            tls_config,
            grpc_config,
        }
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn run(self) -> Result<(), tonic::transport::Error> {
        let tls_enabled = self.tls_config.is_some();
        info!(addr = %self.addr, tls = tls_enabled, "Starting gRPC server");

        let service = Arc::new(self.service);
        let gc = &self.grpc_config;
        let mut builder = TonicServer::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(gc.keepalive_interval_secs)))
            .http2_keepalive_timeout(Some(Duration::from_secs(gc.keepalive_timeout_secs)))
            .tcp_keepalive(Some(Duration::from_secs(gc.tcp_keepalive_secs)))
            .initial_connection_window_size(gc.connection_window_size)
            .initial_stream_window_size(gc.stream_window_size)
            .concurrency_limit_per_connection(gc.concurrency_limit)
            .max_connection_age(Duration::from_secs(gc.max_connection_age_secs))
            .max_connection_age_grace(Duration::from_secs(gc.max_connection_age_grace_secs))
            .http2_max_header_list_size(Some(gc.max_header_list_size_bytes));
        if let Some(tls) = self.tls_config {
            builder = builder.tls_config(tls)?;
        }

        builder
            .layer(labkit::grpc::GrpcMetricsLayer::with_duration_buckets(
                [
                    labkit::otel::DEFAULT_DURATION_BUCKETS_SECONDS,
                    &[15.0, 30.0, 60.0],
                ]
                .concat(),
            ))
            .layer(labkit::grpc::GrpcTraceLayer::new())
            .layer(labkit::grpc::GrpcCorrelationLayer::new())
            .add_service(OrbitServiceServer::from_arc(Arc::clone(&service)))
            .add_service(super::legacy::LegacyGkgService::new(
                OrbitServiceServer::from_arc(service),
            ))
            .serve(self.addr)
            .await
    }
}
