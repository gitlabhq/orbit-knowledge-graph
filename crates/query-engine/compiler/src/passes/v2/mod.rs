//! Edge-chain-first lowering: edge chain drives, nodes are lazy.
//!
//! One Plan struct with a PlanBody enum. Common fields (nodes, hops,
//! limit, etc.) live on Plan; query-type-specific data in the body.
//!
//! ```text
//! plan/
//! ├── mod.rs           — Plan, PlanBody, plan() dispatch
//! ├── edge_chain.rs    — NodePlan, Hop, Strategy + builders
//! ├── neighbors.rs     — plan_neighbors → Plan
//! ├── pathfinding.rs   — plan_pathfinding → Plan
//! └── hydration.rs     — plan_hydration → Plan
//!
//! lower/
//! ├── mod.rs           — emit() dispatch, EmitOutput, Plan::emit_edge_chain()
//! ├── flat_chain.rs    — emit_flat_chain (flat edge chain)
//! ├── fk_star.rs       — emit_fk_star (FK star joins)
//! ├── single_node.rs   — emit_single_node (no edges)
//! ├── helpers.rs       — shared emit helpers (dedup, predicates, node joins)
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
