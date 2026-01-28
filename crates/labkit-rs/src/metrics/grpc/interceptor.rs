use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use futures_core::Stream;
use opentelemetry::global;
use opentelemetry::{KeyValue, metrics::Meter};
use opentelemetry_semantic_conventions::attribute::{
    RPC_GRPC_STATUS_CODE, RPC_METHOD, RPC_SERVICE,
};
use pin_project::pin_project;
use tonic::Status;

use crate::metrics::instruments::GrpcServerInstruments;

/// Wrapper for recording gRPC server metrics.
///
/// Supports both unary and streaming RPCs.
///
/// # Unary Example
///
/// ```rust,ignore
/// use labkit_rs::metrics::grpc::GrpcMetrics;
/// use std::sync::LazyLock;
///
/// static METRICS: LazyLock<GrpcMetrics> = LazyLock::new(GrpcMetrics::new);
///
/// #[tonic::async_trait]
/// impl MyService for MyServiceImpl {
///     async fn get_user(&self, req: Request<GetUserRequest>) -> Result<Response<User>, Status> {
///         METRICS.record("mypackage.MyService", "GetUser", || async {
///             // handler logic
///             Ok(Response::new(User { ... }))
///         }).await
///     }
/// }
/// ```
///
/// # Streaming Example
///
/// ```rust,ignore
/// async fn list_users(
///     &self,
///     req: Request<ListUsersRequest>,
/// ) -> Result<Response<Self::ListUsersStream>, Status> {
///     let stream = async_stream::stream! { ... };
///     Ok(Response::new(METRICS.record_stream("mypackage.MyService", "ListUsers", stream)))
/// }
/// ```
#[derive(Clone)]
pub struct GrpcMetrics {
    instruments: GrpcServerInstruments,
}

impl GrpcMetrics {
    #[must_use]
    pub fn new() -> Self {
        let meter = global::meter("labkit_rs");
        Self::with_meter(&meter)
    }

    #[must_use]
    pub fn with_meter(meter: &Meter) -> Self {
        Self {
            instruments: GrpcServerInstruments::new(meter),
        }
    }

    /// Record metrics for a unary RPC handler.
    ///
    /// Service should be the full service name (e.g., "mypackage.MyService").
    /// Method should be the RPC method name (e.g., "GetUser").
    pub async fn record<T, F, Fut>(
        &self,
        service: &str,
        method: &str,
        handler: F,
    ) -> Result<T, Status>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, Status>>,
    {
        let start = Instant::now();
        let result = handler().await;
        let duration = start.elapsed().as_secs_f64();

        let status_code = match &result {
            Ok(_) => 0i64,
            Err(status) => status.code() as i64,
        };

        self.record_duration(service, method, status_code, duration);
        result
    }

    /// Wrap a response stream to record metrics when it completes.
    ///
    /// Duration is measured from stream creation until the stream ends.
    pub fn record_stream<S, T>(&self, service: &str, method: &str, stream: S) -> MeteredStream<S>
    where
        S: Stream<Item = Result<T, Status>>,
    {
        MeteredStream {
            inner: stream,
            instruments: self.instruments.clone(),
            service: service.to_owned(),
            method: method.to_owned(),
            start: Instant::now(),
            last_status: 0,
        }
    }

    fn record_duration(&self, service: &str, method: &str, status_code: i64, duration: f64) {
        let attributes = [
            KeyValue::new(RPC_SERVICE, service.to_owned()),
            KeyValue::new(RPC_METHOD, method.to_owned()),
            KeyValue::new(RPC_GRPC_STATUS_CODE, status_code),
        ];
        self.instruments.call_duration.record(duration, &attributes);
    }
}

impl Default for GrpcMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// A stream wrapper that records gRPC metrics when the stream completes.
#[pin_project(PinnedDrop)]
pub struct MeteredStream<S> {
    #[pin]
    inner: S,
    instruments: GrpcServerInstruments,
    service: String,
    method: String,
    start: Instant,
    last_status: i64,
}

impl<S, T> Stream for MeteredStream<S>
where
    S: Stream<Item = Result<T, Status>>,
{
    type Item = Result<T, Status>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match this.inner.poll_next(cx) {
            Poll::Ready(Some(Err(status))) => {
                *this.last_status = status.code() as i64;
                Poll::Ready(Some(Err(status)))
            }
            other => other,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

#[pin_project::pinned_drop]
impl<S> PinnedDrop for MeteredStream<S> {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        let duration = this.start.elapsed().as_secs_f64();

        let attributes = [
            KeyValue::new(RPC_SERVICE, this.service.clone()),
            KeyValue::new(RPC_METHOD, this.method.clone()),
            KeyValue::new(RPC_GRPC_STATUS_CODE, *this.last_status),
        ];

        this.instruments.call_duration.record(duration, &attributes);
    }
}
