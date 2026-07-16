//! Code in here knows nothing about cron, CDC, or migrations; it only knows how
//! to enumerate namespaces/projects and publish indexing work to NATS.

pub mod code_backfill;
pub mod namespace_indexing;

pub use code_backfill::CodeBackfill;
pub use namespace_indexing::{NamespaceDispatchRequest, NamespaceIndexingDispatch};

#[derive(Default)]
pub struct DispatchOutcome {
    pub dispatched: u64,
    pub skipped: u64,
    pub drained_paths: Vec<String>,
}
