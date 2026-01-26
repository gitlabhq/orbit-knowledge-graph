//! Server-side gRPC interceptor for correlation ID extraction.
//!
//! Extracts correlation IDs from incoming gRPC metadata or generates new ones.
//!
//! # Primitives
//!
//! - [`server_interceptor`] - Extracts correlation ID and stores in request extensions
//! - [`with_correlation`] - Wraps a handler to set up task-local context (for automatic outgoing propagation)
//! - [`with_correlation_stream`] - Same but for streaming handlers

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;
use tonic::{Request, Status};

use crate::correlation::context::{self, CorrelationIdExt};
use crate::correlation::id::{
    CorrelationId, GRPC_METADATA_CORRELATION_ID, LOG_FIELD_CORRELATION_ID,
};

/// Server interceptor that extracts correlation ID from incoming gRPC requests.
///
/// Extracts from the `x-gitlab-correlation-id` metadata key. If not present,
/// generates a new ULID-based correlation ID. The correlation ID is stored
/// in request extensions and recorded on the current tracing span.
///
/// # Example
///
/// ```rust,ignore
/// use labkit_rs::correlation::grpc::server_interceptor;
/// use tonic::transport::Server;
///
/// Server::builder()
///     .add_service(MyServiceServer::with_interceptor(my_service, server_interceptor))
///     .serve(addr)
///     .await?;
/// ```
pub fn server_interceptor(mut request: Request<()>) -> Result<Request<()>, Status> {
    let correlation_id = extract_correlation_id(&request);

    // Record on tracing span
    tracing::Span::current().record(LOG_FIELD_CORRELATION_ID, correlation_id.as_str());

    // Store in request extensions for handler access
    request
        .extensions_mut()
        .insert(CorrelationIdExt(correlation_id));

    Ok(request)
}

/// Extract correlation ID from gRPC request metadata.
///
/// Returns the correlation ID from `x-gitlab-correlation-id` metadata if present,
/// otherwise generates a new one.
fn extract_correlation_id<T>(request: &Request<T>) -> CorrelationId {
    request
        .metadata()
        .get(GRPC_METADATA_CORRELATION_ID)
        .and_then(|v| v.to_str().ok())
        .filter(|s| !s.is_empty())
        .map(CorrelationId::from_string)
        .unwrap_or_else(CorrelationId::generate)
}

/// Extract correlation ID from gRPC request extensions.
///
/// Returns `None` if no correlation ID was set (e.g., if the interceptor wasn't applied).
#[must_use]
pub fn extract_from_request<T>(request: &Request<T>) -> Option<CorrelationId> {
    request
        .extensions()
        .get::<CorrelationIdExt>()
        .map(|ext| ext.0.clone())
}

/// Configuration for the server interceptor.
#[derive(Clone, Debug, Default)]
pub struct ServerConfig {
    /// Whether to propagate the correlation ID back in response headers.
    pub reverse_propagation: bool,
}

/// Create a configurable server interceptor.
///
/// For most use cases, the simple [`server_interceptor`] function is sufficient.
pub fn create_server_interceptor(
    _config: ServerConfig,
) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |request| server_interceptor(request)
}

/// Execute a gRPC handler with the correlation ID in task-local context.
///
/// This enables automatic propagation of correlation IDs to outgoing HTTP and
/// gRPC requests made within the handler.
///
/// # Example
///
/// ```rust,ignore
/// use labkit_rs::correlation::grpc::with_correlation;
/// use tonic::{Request, Response, Status};
///
/// async fn my_handler(request: Request<MyMessage>) -> Result<Response<MyReply>, Status> {
///     with_correlation(&request, async {
///         // Outgoing requests automatically get the correlation ID
///         let data = http_client.get("/api").await?;
///         Ok(Response::new(MyReply { data }))
///     }).await
/// }
/// ```
pub async fn with_correlation<T, F, R>(request: &Request<T>, handler: F) -> R
where
    F: Future<Output = R>,
{
    let id = extract_from_request(request).unwrap_or_else(CorrelationId::generate);
    context::scope(id, handler).await
}

/// Wrap a stream to execute within a correlation ID context.
///
/// Use this for streaming gRPC handlers (server streaming, client streaming,
/// or bidirectional streaming) to ensure the correlation ID is available
/// throughout stream processing.
///
/// # Example
///
/// ```rust,ignore
/// use labkit_rs::correlation::grpc::{with_correlation_stream, extract_from_request};
/// use tonic::{Request, Response, Status, Streaming};
/// use futures::Stream;
///
/// async fn my_bidi_stream(
///     request: Request<Streaming<MyMessage>>,
/// ) -> Result<Response<impl Stream<Item = Result<MyReply, Status>>>, Status> {
///     let correlation_id = extract_from_request(&request);
///     let input_stream = request.into_inner();
///
///     let output_stream = async_stream::stream! {
///         while let Some(msg) = input_stream.message().await? {
///             // Process message and yield responses
///             yield Ok(MyReply { ... });
///         }
///     };
///
///     // Wrap the output stream to run within correlation context
///     Ok(Response::new(with_correlation_stream(correlation_id, output_stream)))
/// }
/// ```
pub fn with_correlation_stream<S>(
    correlation_id: Option<CorrelationId>,
    stream: S,
) -> CorrelationStream<S> {
    CorrelationStream {
        inner: stream,
        correlation_id: correlation_id.unwrap_or_else(CorrelationId::generate),
    }
}

pin_project! {
    /// A stream wrapper that provides correlation ID context during polling.
    ///
    /// Created by [`with_correlation_stream`].
    pub struct CorrelationStream<S> {
        #[pin]
        inner: S,
        correlation_id: CorrelationId,
    }
}

impl<S> futures_core::Stream for CorrelationStream<S>
where
    S: futures_core::Stream,
{
    type Item = S::Item;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        context::sync_scope(this.correlation_id.clone(), || this.inner.poll_next(cx))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
