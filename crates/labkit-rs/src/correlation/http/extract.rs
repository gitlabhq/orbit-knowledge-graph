//! Extract correlation ID from incoming HTTP requests.
//!
//! Provides a Tower layer that extracts the correlation ID from the `X-Request-Id`
//! header or generates a new one if not present.

use std::task::{Context, Poll};

use http::{Request, Response, header::HeaderName};
use pin_project_lite::pin_project;
use tower::{Layer, Service};

use crate::correlation::context::{CorrelationIdExt, sync_scope};
use crate::correlation::id::{CorrelationId, HTTP_HEADER_CORRELATION_ID};

/// Tower layer that extracts or generates correlation IDs for incoming HTTP requests.
///
/// This layer:
/// 1. Checks for an existing `X-Request-Id` header
/// 2. If not present, generates a new ULID-based correlation ID
/// 3. Stores the correlation ID in request extensions (accessible via [`CorrelationIdExt`])
/// 4. Sets the correlation ID in task-local context for the request duration
///
/// The correlation ID is automatically included in logs when using `labkit_rs::logging::init()`.
///
/// # Example
///
/// ```rust,ignore
/// use axum::Router;
/// use labkit_rs::correlation::http::CorrelationIdLayer;
///
/// let app = Router::new()
///     .route("/", get(handler))
///     .layer(CorrelationIdLayer::new());
/// ```
#[derive(Clone, Debug)]
pub struct CorrelationIdLayer {
    header_name: HeaderName,
}

impl CorrelationIdLayer {
    /// Create a new layer that extracts correlation IDs from the default header.
    #[must_use]
    pub fn new() -> Self {
        Self {
            header_name: HeaderName::from_static(HTTP_HEADER_CORRELATION_ID),
        }
    }

    /// Create a layer with a custom header name.
    #[must_use]
    pub fn with_header(header_name: HeaderName) -> Self {
        Self { header_name }
    }
}

impl Default for CorrelationIdLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for CorrelationIdLayer {
    type Service = CorrelationIdService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CorrelationIdService {
            inner,
            header_name: self.header_name.clone(),
        }
    }
}

/// Service that extracts or generates correlation IDs.
#[derive(Clone, Debug)]
pub struct CorrelationIdService<S> {
    inner: S,
    header_name: HeaderName,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for CorrelationIdService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = CorrelationIdFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<ReqBody>) -> Self::Future {
        // Extract correlation ID from header or generate a new one
        let correlation_id = request
            .headers()
            .get(&self.header_name)
            .and_then(|v| v.to_str().ok())
            .filter(|s| !s.is_empty())
            .map(CorrelationId::from_string)
            .unwrap_or_else(CorrelationId::generate);

        request
            .extensions_mut()
            .insert(CorrelationIdExt(correlation_id.clone()));

        CorrelationIdFuture {
            inner: self.inner.call(request),
            correlation_id,
        }
    }
}

pin_project! {
    /// Future for the correlation ID service.
    ///
    /// This future wraps the inner handler future and ensures the correlation ID
    /// is available in the task-local context during execution.
    pub struct CorrelationIdFuture<F> {
        #[pin]
        inner: F,
        correlation_id: CorrelationId,
    }
}

impl<F, ResBody, E> std::future::Future for CorrelationIdFuture<F>
where
    F: std::future::Future<Output = Result<Response<ResBody>, E>>,
{
    type Output = F::Output;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        // Run the inner poll within the task-local scope so that
        // context::current() returns this correlation ID
        sync_scope(this.correlation_id.clone(), || this.inner.poll(cx))
    }
}

/// Extract correlation ID from request extensions.
///
/// Returns `None` if no correlation ID was set (e.g., if the layer wasn't applied).
#[must_use]
pub fn extract_from_request<B>(request: &Request<B>) -> Option<CorrelationId> {
    request
        .extensions()
        .get::<CorrelationIdExt>()
        .map(|ext| ext.0.clone())
}
