//! Every scheduled task the orchestrator runs: producers that dispatch indexing
//! work, plus the maintenance tasks that keep the graph store healthy.

pub mod code_indexing_task;
pub mod global;
pub mod migration_completion;
pub mod namespace;
pub mod namespace_backfill;
pub mod namespace_deletion;
pub mod stale_edge_reconciliation;
pub mod table_cleanup;

pub use code_indexing_task::SiphonCodeIndexingTaskDispatcher;
pub use global::GlobalDispatcher;
pub use migration_completion::MigrationCompletionChecker;
pub use namespace::NamespaceDispatcher;
pub use namespace_backfill::NamespaceCodeBackfillDispatcher;
pub use namespace_deletion::NamespaceDeletionScheduler;
pub use stale_edge_reconciliation::StaleEdgeReconciliation;
pub use table_cleanup::TableCleanup;
