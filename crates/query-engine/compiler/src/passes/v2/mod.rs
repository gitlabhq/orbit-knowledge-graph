//! Edge-chain-first lowering: edge chain drives, nodes are lazy.
//!
//! Edges drive, nodes are lazy lookups. Zero CTEs for the common case.
//! Replaces `lower` + `optimize` + `deduplicate` with a single `lower` pass.
//!
//! ```text
//! plan/
//! ├── mod.rs           — QueryPlan enum, plan() dispatch
//! ├── edge_chain.rs    — EdgeChainPlan types + builders
//! ├── neighbors.rs     — NeighborsPlan + plan_neighbors
//! ├── pathfinding.rs   — PathFindingPlan + plan_pathfinding
//! └── hydration.rs     — HydrationPlan + plan_hydration
//!
//! lower/
//! ├── mod.rs           — emit() dispatch
//! ├── emit.rs          — EmitOutput, SQL AST emission
//! ├── traversal.rs     — emit_traversal
//! ├── aggregation.rs   — emit_aggregation
//! ├── neighbors.rs     — emit_neighbors
//! ├── pathfinding.rs   — emit_pathfinding
//! └── hydration.rs     — emit_hydration
//!
//! shared.rs            — filter/predicate/column helpers
//! ```

pub mod lower;
pub mod plan;
pub mod shared;
