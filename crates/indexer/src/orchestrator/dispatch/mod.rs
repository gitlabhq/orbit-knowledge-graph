//! Trigger-agnostic dispatch operations shared by the orchestrator's triggers.
//!
//! Code in here knows nothing about cron, CDC, or migrations; it only knows how
//! to enumerate namespaces/projects and publish code-indexing work to NATS.

pub mod code_backfill;

pub use code_backfill::CodeBackfill;
