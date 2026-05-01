//! V2 compiler pipeline: skeleton-first lowering.
//!
//! Edges drive, nodes are lazy lookups. Zero CTEs for the common case.
//! Replaces `lower` + `optimize` + `deduplicate` with a single `lower` pass.

pub mod lower;
