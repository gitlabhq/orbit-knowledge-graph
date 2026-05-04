//! Edge-chain-first lowering: edge chain drives, nodes are lazy.
//!
//! Edges drive, nodes are lazy lookups. Zero CTEs for the common case.
//! Replaces `lower` + `optimize` + `deduplicate` with a single `lower` pass.
//!
//! ```text
//! lower/
//! ├── mod.rs           — dispatch by query type
//! ├── plan.rs          — EdgeChainPlan, plan types, builder
//! ├── emit.rs          — EmitOutput, SQL AST emission
//! ├── traversal.rs     — plan + edge SELECT + ORDER BY
//! └── aggregation.rs   — plan + GROUP BY + agg functions
//! ```

pub mod lower;
