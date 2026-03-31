//! Shared configuration types used across the GKG service.
//!
//! This crate is intentionally thin — it defines config structs and global
//! constants that need to be shared between the server, compiler, and other
//! subsystems without introducing heavy dependencies.

pub mod global;
pub mod query;

pub use query::QueryConfig;
