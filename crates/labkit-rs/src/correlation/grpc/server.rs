//! Server-side gRPC interceptor for correlation ID extraction.

use std::future::Future;

use opentelemetry::Context as OtelContext;
use opentelemetry::trace::{FutureExt, WithContext};
use tonic::{Request, Status};

use crate::correlation::context::{CorrelationIdExt, OtelContextExt, with_correlation_id};
use crate::correlation::id::CorrelationId;
use crate::correlation::propagator::{ensure_correlation_id, extract_from_grpc_metadata};

/// Server interceptor that extracts correlation ID from incoming gRPC requests.
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
    let id = extract_from_grpc_metadata(request.metadata());
    let cx = if let Some(id) = id {
        crate::correlation::propagator::context_with_id(id)
    } else {
        OtelContext::current()
    };
    let (cx, id) = ensure_correlation_id(cx);

    request.extensions_mut().insert(OtelContextExt(cx.clone()));
    request.extensions_mut().insert(CorrelationIdExt(id));

    Ok(request)
}

pub fn extract_from_request<T>(request: &Request<T>) -> Option<CorrelationId> {
    request
        .extensions()
        .get::<CorrelationIdExt>()
        .map(|ext| ext.0.clone())
}

pub fn context_from_request<T>(request: &Request<T>) -> OtelContext {
    request
        .extensions()
        .get::<OtelContextExt>()
        .map(|ext| ext.0.clone())
        .unwrap_or_else(OtelContext::current)
}

#[derive(Clone, Debug, Default)]
pub struct ServerConfig {
    pub reverse_propagation: bool,
}

pub fn create_server_interceptor(
    _config: ServerConfig,
) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |request| server_interceptor(request)
}

/// Execute a gRPC handler with the OpenTelemetry context from the request.
///
/// # Example
///
/// ```rust,ignore
/// use labkit_rs::correlation::grpc::with_correlation;
/// use tonic::{Request, Response, Status};
///
/// async fn my_handler(request: Request<MyMessage>) -> Result<Response<MyReply>, Status> {
///     with_correlation(&request, async {
///         // Correlation ID available via context::current()
///         // Outgoing requests automatically get the correlation ID
///         Ok(Response::new(MyReply { ... }))
///     }).await
/// }
/// ```
pub async fn with_correlation<T, F, R>(request: &Request<T>, handler: F) -> R
where
    F: Future<Output = R>,
{
    let cx = request
        .extensions()
        .get::<OtelContextExt>()
        .map(|ext| ext.0.clone())
        .unwrap_or_else(|| {
            let id = CorrelationId::generate();
            with_correlation_id(id)
        });

    handler.with_context(cx).await
}

/// Wrap a stream to execute within an OpenTelemetry context.
///
/// # Example
///
/// ```rust,ignore
/// use labkit_rs::correlation::grpc::{with_correlation_stream, context_from_request};
///
/// async fn my_stream(request: Request<Streaming<Msg>>) -> Result<Response<impl Stream<...>>, Status> {
///     let context = context_from_request(&request);
///     let stream = async_stream::stream! { ... };
///     Ok(Response::new(with_correlation_stream(context, stream)))
/// }
/// ```
pub fn with_correlation_stream<S>(context: OtelContext, stream: S) -> WithContext<S> {
    stream.with_context(context)
}

pub fn with_correlation_id_stream<S>(
    correlation_id: Option<CorrelationId>,
    stream: S,
) -> WithContext<S> {
    let context = correlation_id
        .map(with_correlation_id)
        .unwrap_or_else(|| with_correlation_id(CorrelationId::generate()));

    stream.with_context(context)
}
