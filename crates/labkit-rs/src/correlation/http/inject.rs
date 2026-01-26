//! Inject correlation ID into outgoing HTTP requests and responses.
//!
//! Provides Tower layers for:
//! - Injecting correlation ID into outgoing HTTP client requests
//! - Propagating correlation ID to response headers

use std::task::{Context, Poll};

use http::{Request, Response, header::HeaderName, header::HeaderValue};
use pin_project_lite::pin_project;
use tower::{Layer, Service};

use crate::correlation::context::{CorrelationIdExt, current_or_generate};
use crate::correlation::id::{CorrelationId, HTTP_HEADER_CLIENT_NAME, HTTP_HEADER_CORRELATION_ID};

/// Tower layer that injects correlation ID into outgoing HTTP requests.
///
/// For use with HTTP clients (reqwest, hyper, etc.) to propagate correlation
/// IDs to downstream services.
///
/// # Example
///
/// ```rust,ignore
/// use labkit_rs::correlation::http::InjectCorrelationIdLayer;
///
/// let client = reqwest::Client::builder()
///     .build()
///     .unwrap();
/// // Apply layer to client requests...
/// ```
#[derive(Clone, Debug)]
pub struct InjectCorrelationIdLayer {
    header_name: HeaderName,
    client_name: Option<String>,
}

impl InjectCorrelationIdLayer {
    /// Create a new layer with default settings.
    #[must_use]
    pub fn new() -> Self {
        Self {
            header_name: HeaderName::from_static(HTTP_HEADER_CORRELATION_ID),
            client_name: None,
        }
    }

    /// Set a client name to include in outgoing requests.
    ///
    /// This will be sent in the `X-GitLab-Client-Name` header.
    #[must_use]
    pub fn with_client_name(mut self, name: impl Into<String>) -> Self {
        self.client_name = Some(name.into());
        self
    }
}

impl Default for InjectCorrelationIdLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for InjectCorrelationIdLayer {
    type Service = InjectCorrelationIdService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        InjectCorrelationIdService {
            inner,
            header_name: self.header_name.clone(),
            client_name: self.client_name.clone(),
        }
    }
}

/// Service that injects correlation ID into outgoing requests.
#[derive(Clone, Debug)]
pub struct InjectCorrelationIdService<S> {
    inner: S,
    header_name: HeaderName,
    client_name: Option<String>,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for InjectCorrelationIdService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: Request<ReqBody>) -> Self::Future {
        // Get correlation ID from task-local context or generate new one
        let correlation_id = current_or_generate();

        // Inject correlation ID header
        if let Ok(value) = HeaderValue::from_str(correlation_id.as_str()) {
            request
                .headers_mut()
                .insert(self.header_name.clone(), value);
        }

        // Inject client name if configured
        if let Some(ref name) = self.client_name
            && let Ok(value) = HeaderValue::from_str(name)
        {
            request
                .headers_mut()
                .insert(HeaderName::from_static(HTTP_HEADER_CLIENT_NAME), value);
        }

        self.inner.call(request)
    }
}

/// Tower layer that propagates correlation ID to response headers.
///
/// Copies the correlation ID from request extensions to the response
/// `X-Request-Id` header, enabling clients to correlate responses with requests.
#[derive(Clone, Debug)]
pub struct PropagateCorrelationIdLayer {
    header_name: HeaderName,
}

impl PropagateCorrelationIdLayer {
    /// Create a new layer with default settings.
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

/// Service that propagates correlation ID to responses.
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
    type Future = PropagateCorrelationIdFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        // Extract correlation ID from request extensions
        let correlation_id = request
            .extensions()
            .get::<CorrelationIdExt>()
            .map(|ext| ext.0.clone());

        PropagateCorrelationIdFuture {
            inner: self.inner.call(request),
            header_name: self.header_name.clone(),
            correlation_id,
        }
    }
}

pin_project! {
    /// Future for the propagate correlation ID service.
    pub struct PropagateCorrelationIdFuture<F> {
        #[pin]
        inner: F,
        header_name: HeaderName,
        correlation_id: Option<CorrelationId>,
    }
}

impl<F, ResBody, E> std::future::Future for PropagateCorrelationIdFuture<F>
where
    F: std::future::Future<Output = Result<Response<ResBody>, E>>,
{
    type Output = F::Output;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        match this.inner.poll(cx) {
            Poll::Ready(Ok(mut response)) => {
                // Add correlation ID to response headers
                if let Some(id) = this.correlation_id
                    && let Ok(value) = HeaderValue::from_str(id.as_str())
                {
                    response
                        .headers_mut()
                        .insert(this.header_name.clone(), value);
                }
                Poll::Ready(Ok(response))
            }
            Poll::Ready(Err(e)) => Poll::Ready(Err(e)),
            Poll::Pending => Poll::Pending,
        }
    }
}
