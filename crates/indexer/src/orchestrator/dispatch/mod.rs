//! Trigger-agnostic dispatch operations shared by the orchestrator's triggers.
//!
//! Code in here knows nothing about cron, CDC, or migrations; it only knows how
//! to enumerate namespaces/projects and publish indexing work to NATS.

pub mod code_backfill;
pub mod namespace_indexing;

pub use code_backfill::CodeBackfill;
pub use namespace_indexing::NamespaceIndexingDispatch;

/// Result of a dispatch pass: how many requests were published versus skipped
/// (already in-flight on the NATS work queue).
pub struct DispatchOutcome {
    pub dispatched: u64,
    pub skipped: u64,
}
