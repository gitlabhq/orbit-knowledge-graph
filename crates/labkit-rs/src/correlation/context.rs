//! Context storage for correlation ID propagation using OpenTelemetry.

use opentelemetry::{Context, KeyValue, baggage::BaggageExt};

use crate::correlation::id::CorrelationId;

pub const BAGGAGE_CORRELATION_ID: &str = "correlation_id";

#[derive(Clone, Debug)]
pub struct CorrelationIdExt(pub CorrelationId);

#[derive(Clone)]
pub struct OtelContextExt(pub Context);

/// Get the current correlation ID from OpenTelemetry context.
#[must_use]
pub fn current() -> Option<CorrelationId> {
    get_from_context(&Context::current())
}

#[must_use]
pub fn get_from_context(cx: &Context) -> Option<CorrelationId> {
    cx.baggage()
        .get(BAGGAGE_CORRELATION_ID)
        .map(|v| CorrelationId::from_string(v.to_string()))
}

#[must_use]
pub fn current_or_generate() -> CorrelationId {
    current().unwrap_or_else(CorrelationId::generate)
}

#[must_use]
pub fn with_correlation_id(id: CorrelationId) -> Context {
    Context::current().with_baggage(vec![KeyValue::new(
        BAGGAGE_CORRELATION_ID,
        id.into_string(),
    )])
}

/// Run a future with a correlation ID in the OpenTelemetry context.
///
/// # Example
///
/// ```ignore
/// use labkit_rs::correlation::{CorrelationId, context};
///
/// async fn handler() {
///     let id = CorrelationId::generate();
///     context::scope(id, async {
///         let current = context::current(); // returns Some(id)
///     }).await;
/// }
/// ```
pub async fn scope<F, T>(id: CorrelationId, f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    use opentelemetry::trace::FutureExt;
    let cx = with_correlation_id(id);
    f.with_context(cx).await
}

pub fn sync_scope<F, T>(id: CorrelationId, f: F) -> T
where
    F: FnOnce() -> T,
{
    let cx = with_correlation_id(id);
    let _guard = cx.attach();
    f()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn scope_provides_correlation_id() {
        let id = CorrelationId::from_string("test-id");
        let result = scope(id.clone(), async { current() }).await;
        assert_eq!(result, Some(id));
    }

    #[tokio::test]
    async fn current_returns_none_outside_scope() {
        let result = current();
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn current_or_generate_returns_existing() {
        let id = CorrelationId::from_string("existing-id");
        let result = scope(id.clone(), async { current_or_generate() }).await;
        assert_eq!(result, id);
    }

    #[tokio::test]
    async fn current_or_generate_creates_new_outside_scope() {
        let result = current_or_generate();
        assert_eq!(result.as_str().len(), 26);
    }

    #[test]
    fn sync_scope_provides_correlation_id() {
        let id = CorrelationId::from_string("sync-test-id");
        let result = sync_scope(id.clone(), current);
        assert_eq!(result, Some(id));
    }

    #[test]
    fn with_correlation_id_creates_context() {
        let id = CorrelationId::from_string("context-test");
        let cx = with_correlation_id(id.clone());
        let extracted = get_from_context(&cx);
        assert_eq!(extracted, Some(id));
    }
}
