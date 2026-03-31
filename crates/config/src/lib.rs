//! Shared configuration types used across the GKG service.
//!
//! This crate is intentionally thin — it defines config structs that need
//! to be shared between the server, compiler, and other subsystems without
//! introducing heavy dependencies.

pub mod query;

pub use query::QueryConfig;
