//! Extract correlation ID from incoming HTTP requests.

use std::task::Poll;

use http::{Request, Response};
use opentelemetry::Context as OtelContext;
use opentelemetry::trace::{FutureExt, WithContext};
use tower::{Layer, Service};

use crate::correlation::context::CorrelationIdExt;
use crate::correlation::propagator::{ensure_correlation_id, extract_from_http_headers};

/// Tower layer that extracts or generates correlation IDs for incoming HTTP requests.
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
#[derive(Clone, Debug, Default)]
pub struct CorrelationIdLayer;

impl CorrelationIdLayer {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl<S> Layer<S> for CorrelationIdLayer {
    type Service = CorrelationIdService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        CorrelationIdService { inner }
    }
}

#[derive(Clone, Debug)]
pub struct CorrelationIdService<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for CorrelationIdService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = WithContext<S::Future>;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<ReqBody>) -> Self::Future {
        let id = extract_from_http_headers(request.headers());
        let cx = if let Some(id) = id {
            crate::correlation::propagator::context_with_id(id)
        } else {
            OtelContext::current()
        };
        let (cx, id) = ensure_correlation_id(cx);

        request.extensions_mut().insert(CorrelationIdExt(id));

        self.inner.call(request).with_context(cx)
    }
}

pub fn extract_from_request<B>(request: &Request<B>) -> Option<crate::correlation::CorrelationId> {
    request
        .extensions()
        .get::<CorrelationIdExt>()
        .map(|ext| ext.0.clone())
}

pub fn context_from_request<B>(request: &Request<B>) -> OtelContext {
    extract_from_request(request)
        .map(crate::correlation::context::with_correlation_id)
        .unwrap_or_else(OtelContext::current)
}
