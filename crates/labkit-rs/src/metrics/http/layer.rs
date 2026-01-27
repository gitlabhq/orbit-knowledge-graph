use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use http::{Request, Response};
use opentelemetry::global;
use opentelemetry::{KeyValue, metrics::Meter};
#[cfg(feature = "metrics-axum")]
use opentelemetry_semantic_conventions::attribute::HTTP_ROUTE;
use opentelemetry_semantic_conventions::attribute::{
    HTTP_REQUEST_METHOD, HTTP_RESPONSE_STATUS_CODE,
};
use pin_project::pin_project;
use tower::{Layer, Service};

use crate::metrics::instruments::HttpServerInstruments;

/// Tower layer that records HTTP server metrics.
///
/// Records `http.server.request.duration`, `http.server.active_requests`,
/// and optionally body size histograms.
///
/// # Example
///
/// ```rust,ignore
/// use axum::Router;
/// use labkit_rs::metrics::http::HttpMetricsLayer;
///
/// let app = Router::new()
///     .route("/", get(handler))
///     .layer(HttpMetricsLayer::new());
/// ```
#[derive(Clone)]
pub struct HttpMetricsLayer {
    instruments: HttpServerInstruments,
}

impl HttpMetricsLayer {
    #[must_use]
    pub fn new() -> Self {
        let meter = global::meter("labkit_rs");
        Self::with_meter(&meter, false)
    }

    #[must_use]
    pub fn with_body_size_recording() -> Self {
        let meter = global::meter("labkit_rs");
        Self::with_meter(&meter, true)
    }

    #[must_use]
    pub fn with_meter(meter: &Meter, record_body_size: bool) -> Self {
        Self {
            instruments: HttpServerInstruments::new(meter, record_body_size),
        }
    }
}

impl Default for HttpMetricsLayer {
    fn default() -> Self {
        Self::new()
    }
}

impl<S> Layer<S> for HttpMetricsLayer {
    type Service = HttpMetricsService<S>;

    fn layer(&self, inner: S) -> Self::Service {
        HttpMetricsService {
            inner,
            instruments: self.instruments.clone(),
        }
    }
}

#[derive(Clone)]
pub struct HttpMetricsService<S> {
    inner: S,
    instruments: HttpServerInstruments,
}

impl<S, ReqBody, ResBody> Service<Request<ReqBody>> for HttpMetricsService<S>
where
    S: Service<Request<ReqBody>, Response = Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = HttpMetricsFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<ReqBody>) -> Self::Future {
        let method = request.method().as_str().to_owned();

        #[cfg(feature = "metrics-axum")]
        let route = request
            .extensions()
            .get::<axum::extract::MatchedPath>()
            .map(|p| p.as_str().to_owned());

        #[cfg(not(feature = "metrics-axum"))]
        let route: Option<String> = None;

        self.instruments.active_requests.add(1, &[]);

        HttpMetricsFuture {
            inner: self.inner.call(request),
            instruments: self.instruments.clone(),
            method,
            route,
            start: Instant::now(),
        }
    }
}

#[pin_project]
pub struct HttpMetricsFuture<F> {
    #[pin]
    inner: F,
    instruments: HttpServerInstruments,
    method: String,
    route: Option<String>,
    start: Instant,
}

impl<F, ResBody, E> Future for HttpMetricsFuture<F>
where
    F: Future<Output = Result<Response<ResBody>, E>>,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();

        match this.inner.poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                let duration = this.start.elapsed().as_secs_f64();
                this.instruments.active_requests.add(-1, &[]);

                let mut attributes = vec![KeyValue::new(HTTP_REQUEST_METHOD, this.method.clone())];

                #[cfg(feature = "metrics-axum")]
                if let Some(route) = this.route.take() {
                    attributes.push(KeyValue::new(HTTP_ROUTE, route));
                }

                #[cfg(not(feature = "metrics-axum"))]
                let _ = this.route.take();

                if let Ok(ref response) = result {
                    attributes.push(KeyValue::new(
                        HTTP_RESPONSE_STATUS_CODE,
                        i64::from(response.status().as_u16()),
                    ));
                }

                this.instruments
                    .request_duration
                    .record(duration, &attributes);

                Poll::Ready(result)
            }
        }
    }
}
