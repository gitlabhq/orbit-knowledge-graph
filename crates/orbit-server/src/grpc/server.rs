use std::sync::Arc;
use std::time::Duration;

use clickhouse_client::ArrowClickHouseClient;
use orbit_log_fields::info;
use orbit_server_config::{AnalyticsConfig, GrpcConfig};
use query_engine::shared::content::ColumnResolverRegistry;
use tokio::net::TcpListener;
use tonic::codec::CompressionEncoding;
use tonic::transport::Server as TonicServer;
use tonic::transport::server::{ServerTlsConfig, TcpIncoming};

use crate::active_schema::ActiveSchema;
use crate::analytics::AnalyticsTracker;
use crate::auth::JwtValidator;
use crate::cluster_health::ClusterHealthChecker;
use crate::proto::orbit_service_server::OrbitServiceServer;
use orbit_billing::{BillingTracker, QuotaService};

use super::service::OrbitServiceImpl;

/// The client's `grpc-accept-encoding` order picks among the encodings tonic is
/// compiled with (gzip, zstd); `send_compressed` only switches response
/// compression on. C-core clients list gzip as the only one they decode.
fn with_compression<T>(service: OrbitServiceServer<T>) -> OrbitServiceServer<T> {
    service
        .send_compressed(CompressionEncoding::Gzip)
        .send_compressed(CompressionEncoding::Zstd)
        .accept_compressed(CompressionEncoding::Gzip)
        .accept_compressed(CompressionEncoding::Zstd)
}

pub struct GrpcServer {
    service: OrbitServiceImpl,
    tls_config: Option<ServerTlsConfig>,
    grpc_config: GrpcConfig,
}

impl GrpcServer {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        validator: Arc<JwtValidator>,
        active_schema: Arc<ActiveSchema>,
        client: ArrowClickHouseClient,
        cluster_health: Arc<ClusterHealthChecker>,
        tls_config: Option<ServerTlsConfig>,
        grpc_config: GrpcConfig,
        analytics_config: Arc<AnalyticsConfig>,
    ) -> Self {
        let service = OrbitServiceImpl::new(
            validator,
            active_schema,
            client,
            cluster_health,
            &grpc_config,
            analytics_config,
        );
        Self {
            service,
            tls_config,
            grpc_config,
        }
    }

    pub fn with_resolver_registry(mut self, registry: Arc<ColumnResolverRegistry>) -> Self {
        self.service = self.service.with_resolver_registry(registry);
        self
    }

    pub fn with_billing(mut self, tracker: Arc<dyn BillingTracker>) -> Self {
        self.service = self.service.with_billing(tracker);
        self
    }

    pub fn with_quota(mut self, quota: Arc<QuotaService>) -> Self {
        self.service = self.service.with_quota(quota);
        self
    }

    pub fn with_analytics(mut self, tracker: Arc<dyn AnalyticsTracker>) -> Self {
        self.service = self.service.with_analytics(tracker);
        self
    }

    pub async fn run(self, listener: TcpListener) -> Result<(), tonic::transport::Error> {
        let tls_enabled = self.tls_config.is_some();
        info!(safe(TLS_ENABLED) = tls_enabled, "Starting gRPC server");

        let service = Arc::new(self.service);
        let gc = &self.grpc_config;
        // Tonic ignores the builder's TCP settings for a pre-bound listener.
        let incoming = TcpIncoming::from(listener)
            .with_nodelay(Some(true))
            .with_keepalive(Some(Duration::from_secs(gc.tcp_keepalive_secs)));
        let mut builder = TonicServer::builder()
            .http2_keepalive_interval(Some(Duration::from_secs(gc.keepalive_interval_secs)))
            .http2_keepalive_timeout(Some(Duration::from_secs(gc.keepalive_timeout_secs)))
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
            .layer(labkit::grpc::GrpcVerboseLogsLayer::new(
                crate::expanded_logs::VERBOSE_LOGS.clone(),
            ))
            .layer(tower::util::MapFutureLayer::new(|future| {
                tracing::Instrument::instrument(future, crate::auth::request_span())
            }))
            .add_service(with_compression(OrbitServiceServer::from_arc(Arc::clone(
                &service,
            ))))
            .add_service(super::legacy::LegacyGkgService::new(with_compression(
                OrbitServiceServer::from_arc(service),
            )))
            .serve_with_incoming(incoming)
            .await
    }
}
