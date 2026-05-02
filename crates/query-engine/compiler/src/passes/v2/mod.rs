//! V2 compiler pipeline: skeleton-first lowering.
//!
//! Edges drive, nodes are lazy lookups. Zero CTEs for the common case.
//! Replaces `lower` + `optimize` + `deduplicate` with a single `lower` pass.
//!
//! ```text
//! lower/
//! ├── mod.rs           — dispatch by query type
//! ├── types.rs         — Skeleton struct, hydration, edge chain, helpers
//! ├── traversal.rs     — skeleton + edge SELECT + ORDER BY
//! └── aggregation.rs   — skeleton + GROUP BY + agg functions
//! ```

pub mod enforce;
pub mod lower;
