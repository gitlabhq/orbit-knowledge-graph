//! Inject correlation ID into outgoing HTTP requests and responses.

use std::task::Poll;

use futures_util::TryFutureExt;
use http::{Request, Response, header::HeaderName, header::HeaderValue};
use opentelemetry::Context as OtelContext;
use tower::{Layer, Service};

use crate::correlation::context::CorrelationIdExt;
use crate::correlation::id::HTTP_HEADER_CORRELATION_ID;
use crate::correlation::propagator::{
    ensure_correlation_id, inject_client_name_to_http_headers, inject_to_http_headers,
};

/// Tower layer that injects correlation ID into outgoing HTTP requests.
///
/// # Example
///
/// ```rust,ignore
/// use labkit_rs::correlation::http::InjectCorrelationIdLayer;
/// use tower::ServiceBuilder;
///
/// let service = ServiceBuilder::new()
///     .layer(InjectCorrelationIdLayer::new())
///     .service(http_client);
/// ```
#[derive(Clone, Debug, Default)]
pub struct InjectCorrelationIdLayer {
    client_name: Option<String>,
}

impl InjectCorrelationIdLayer {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_client_name(mut self, name: impl Into<String>) -> Self {
        self.client_name = Some(name.into());
        self
    }
}

impl<S> Layer<S> for InjectCorrelationIdLayer {
    type Service = InjectCorrelationIdService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        InjectCorrelationIdService {
            inner,
            client_name: self.client_name.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct InjectCorrelationIdService<S> {
    inner: S,
    client_name: Option<String>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for InjectCorrelationIdService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<ReqBody>) -> Self::Future {
        let (_, id) = ensure_correlation_id(OtelContext::current());
        inject_to_http_headers(request.headers_mut(), &id);
        if let Some(ref name) = self.client_name {
            inject_client_name_to_http_headers(request.headers_mut(), name);
        }
        self.inner.call(request)
    }
}

/// Tower layer that propagates correlation ID to response headers.
#[derive(Clone, Debug)]
pub struct PropagateCorrelationIdLayer {
    header_name: HeaderName,
}

impl PropagateCorrelationIdLayer {
    #[must_use]
    pub fn new() -> Self {
        Self {
            header_name: HeaderName::from_static(HTTP_HEADER_CORRELATION_ID),
        }
    }
}

impl Default for PropagateCorrelationIdLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for PropagateCorrelationIdLayer {
    type Service = PropagateCorrelationIdService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        PropagateCorrelationIdService {
            inner,
            header_name: self.header_name.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PropagateCorrelationIdService<S> {
    inner: S,
    header_name: HeaderName,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for PropagateCorrelationIdService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures_util::future::MapOk<
        S::Future,
        Box<dyn FnOnce(Response<ResBody>) -> Response<ResBody> + Send>,
    >;

    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let correlation_id = request.extensions().get::<CorrelationIdExt>().cloned();
        let header_name = self.header_name.clone();

        self.inner
            .call(request)
            .map_ok(Box::new(move |mut response| {
                if let Some(ext) = correlation_id
                    && let Ok(value) = HeaderValue::from_str(ext.0.as_str())
                {
                    response.headers_mut().insert(header_name, value);
                }
                response
            }))
    }
}
