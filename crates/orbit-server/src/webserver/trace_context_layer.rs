use std::collections::HashMap;
use std::task::{Context, Poll};

use axum::http::Request;
use opentelemetry::propagation::Extractor;
use tower::{Layer, Service};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// W3C Trace Context + Baggage headers forwarded from upstream callers (CLI, Rails, Workhorse).
const PROPAGATED_HEADERS: &[&str] = &["traceparent", "tracestate", "baggage"];

#[derive(Clone, Copy)]
pub struct InboundTraceContextLayer;

impl<S> Layer<S> for InboundTraceContextLayer {
    type Service = InboundTraceContext<S>;

    fn layer(&self, inner: S) -> Self::Service {
        InboundTraceContext { inner }
    }
}

#[derive(Clone)]
pub struct InboundTraceContext<S> {
    inner: S,
}

impl<S, B> Service<Request<B>> for InboundTraceContext<S>
where
    S: Service<Request<B>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: Request<B>) -> Self::Future {
        let headers: HashMap<String, String> = PROPAGATED_HEADERS
            .iter()
            .filter_map(|name| {
                let value = request.headers().get(*name)?.to_str().ok()?;
                Some(((*name).to_owned(), value.to_owned()))
            })
            .collect();

        if !headers.is_empty() {
            let carrier = OwnedHeaderCarrier(headers);
            let parent_context = opentelemetry::global::get_text_map_propagator(|propagator| {
                propagator.extract(&carrier)
            });
            if let Err(err) = tracing::Span::current().set_parent(parent_context) {
                tracing::debug!(?err, "could not link inbound trace context to span");
            }
        }

        self.inner.call(request)
    }
}

struct OwnedHeaderCarrier(HashMap<String, String>);

impl Extractor for OwnedHeaderCarrier {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}
