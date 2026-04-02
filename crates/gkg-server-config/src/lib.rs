//! Shared configuration types used across the GKG service.
//!
//! This crate is intentionally thin: it defines config structs and a global
//! accessor so that the compiler, server, and other subsystems can read
//! query settings without heavy dependencies or constructor drilling.

pub mod query;

pub use query::QueryConfig;
pub use query::QuerySettings;
