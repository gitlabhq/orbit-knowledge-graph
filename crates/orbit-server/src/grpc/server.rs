use std::sync::Arc;
use std::time::Duration;

use clickhouse_client::ArrowClickHouseClient;
use futures::{Stream, StreamExt};
use orbit_log_fields::{info, warn};
use orbit_server_config::{AnalyticsConfig, GrpcConfig};
use query_engine::shared::content::ColumnResolverRegistry;
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;
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
    shutdown: CancellationToken,
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
            shutdown: CancellationToken::new(),
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

    /// Without a shutdown token the server runs until the process exits.
    pub fn with_shutdown(mut self, shutdown: CancellationToken) -> Self {
        self.shutdown = shutdown;
        self
    }

    /// Serves until the shutdown token is cancelled, then drains: the listener
    /// closes, every connection gets a GOAWAY, and in-flight streams run to
    /// completion. After `grpc.shutdown_drain_timeout_secs` this stops
    /// waiting and returns; the remaining streams end when the process exits.
    ///
    /// The Kubernetes `terminationGracePeriodSeconds` must exceed the drain
    /// timeout, or the kubelet sends SIGKILL mid-drain.
    pub async fn run(self, listener: TcpListener) -> Result<(), tonic::transport::Error> {
        let shutdown = self.shutdown;
        let tls_enabled = self.tls_config.is_some();
        info!(safe(TLS_ENABLED) = tls_enabled, "Starting gRPC server");

        let service = Arc::new(self.service);
        let gc = &self.grpc_config;
        // Tonic ignores the builder's TCP settings for a pre-bound listener.
        let incoming = TcpIncoming::from(listener)
            .with_nodelay(Some(true))
            .with_keepalive(Some(Duration::from_secs(gc.tcp_keepalive_secs)));
        let (incoming, listener_closed) = close_on_shutdown(incoming, shutdown.clone());
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

        let drain_timeout = Duration::from_secs(gc.shutdown_drain_timeout_secs);
        let serve = builder
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
            .serve_with_incoming_shutdown(incoming, listener_closed);
        tokio::pin!(serve);

        tokio::select! {
            result = &mut serve => return result,
            () = shutdown.cancelled() => {}
        }
        info!(
            safe(TIMEOUT_S) = drain_timeout.as_secs(),
            "Draining gRPC server"
        );
        match tokio::time::timeout(drain_timeout, serve).await {
            Ok(result) => {
                info!("gRPC server drained");
                result
            }
            Err(_) => {
                warn!(
                    safe(TIMEOUT_S) = drain_timeout.as_secs(),
                    "gRPC drain timed out; exiting with streams still open"
                );
                Ok(())
            }
        }
    }
}

/// Ends `incoming` when `shutdown` is cancelled and drops it, which closes the
/// listener so new connections are refused rather than left in the backlog.
/// The returned future resolves once the listener is gone; tonic uses it as
/// the signal to send GOAWAY and wait for in-flight streams.
fn close_on_shutdown<S>(
    incoming: S,
    shutdown: CancellationToken,
) -> (impl Stream<Item = S::Item>, impl Future<Output = ()>)
where
    S: Stream + Unpin,
{
    // Dropping the sender together with `incoming` resolves the receiver.
    let (closed_tx, closed_rx) = oneshot::channel::<()>();
    let stream = futures::stream::unfold(
        (incoming, shutdown, closed_tx),
        |(mut incoming, shutdown, closed_tx)| async move {
            tokio::select! {
                biased;
                () = shutdown.cancelled() => None,
                item = incoming.next() => item.map(|item| (item, (incoming, shutdown, closed_tx))),
            }
        },
    );
    (stream, async move {
        let _ = closed_rx.await;
    })
}
