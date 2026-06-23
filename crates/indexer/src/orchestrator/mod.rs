//! The orchestrator drives indexing: it owns the clock, decides what to index,
//! reacts to Siphon CDC, and dispatches work requests for the indexer to execute.

pub mod scheduler;
pub mod siphon;
pub mod tasks;
