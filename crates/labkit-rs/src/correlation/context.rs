//! Context storage for correlation ID propagation.
//!
//! Provides mechanisms to store and retrieve correlation IDs in async contexts.

use crate::correlation::id::CorrelationId;

tokio::task_local! {
    /// Task-local storage for correlation ID.
    pub static CORRELATION_ID: CorrelationId;
}

/// Extension type for storing correlation ID in request extensions.
///
/// Used with HTTP request extensions and gRPC request extensions.
#[derive(Clone, Debug)]
pub struct CorrelationIdExt(pub CorrelationId);

/// Run a future with a correlation ID in the task-local context.
///
/// The correlation ID will be available via [`current`] for the duration
/// of the future execution.
///
/// # Example
///
/// ```ignore
/// use labkit_rs::correlation::{CorrelationId, context};
///
/// async fn handler() {
///     let id = CorrelationId::generate();
///     context::scope(id, async {
///         // correlation ID is available here
///         let current = context::current();
///     }).await;
/// }
/// ```
pub async fn scope<F, T>(id: CorrelationId, f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    CORRELATION_ID.scope(id, f).await
}

/// Run synchronous code with a correlation ID in the task-local context.
///
/// This is useful for running code in poll methods where async isn't available.
pub fn sync_scope<F, T>(id: CorrelationId, f: F) -> T
where
    F: FnOnce() -> T,
{
    CORRELATION_ID.sync_scope(id, f)
}

/// Get the current correlation ID from the task-local context.
///
/// Returns `None` if no correlation ID has been set in the current context.
#[must_use]
pub fn current() -> Option<CorrelationId> {
    CORRELATION_ID.try_with(|id| id.clone()).ok()
}

/// Get the current correlation ID or generate a new one.
///
/// If a correlation ID exists in the task-local context, returns it.
/// Otherwise, generates and returns a new ULID-based correlation ID.
#[must_use]
pub fn current_or_generate() -> CorrelationId {
    current().unwrap_or_else(CorrelationId::generate)
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
        // Should return a valid ULID (26 chars)
        assert_eq!(result.as_str().len(), 26);
    }
}
