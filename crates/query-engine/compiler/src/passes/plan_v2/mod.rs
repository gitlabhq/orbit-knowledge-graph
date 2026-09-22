//! Physical planning: query `Input` → `PhysOp`, a relational operator tree
//! over ClickHouse tables.
//!
//! ```text
//!   Input ──plan()──▶ PhysOp ──optimize──▶ PhysOp ──lower_v2──▶ SQL AST
//! ```
//!
//! One shape per query type builds the naive tree:
//!
//! | query type   | file            | optimized |
//! |--------------|-----------------|-----------|
//! | traversal    | `chain.rs`      | yes       |
//! | aggregation  | `chain.rs`      | yes       |
//! | neighbors    | `neighbors.rs`  | no        |
//! | pathfinding  | `pathfinding.rs`| no        |
//! | hydration    | `hydration.rs`  | no        |
//!
//! The vocabulary those shapes are written in:
//!
//! - `op.rs`: the operators (`Scan`, `Filter`, `Join`, ...) and builders.
//! - `expr.rs`: scalar expressions with fully qualified columns, and the
//!   typed builders that carry user data (`id_in`, `rel_kind`, ...).
//! - `parse.rs`: `pe!("f.depth = 1")`, so a plan reads as the SQL it makes.
//! - `ctx.rs`: scans and predicates every shape needs (`node_scan`, ...).
//! - `hops.rs`: an `e1 -> e2 -> ... -> eN` edge chain, for variable-length
//!   hops and pathfinding frontiers.
//! - `join_graph.rs`: what the planner needs to know from the ontology.
//! - `sexpr.rs`: the plan as an S-expression, for fixtures and devtools.

mod chain;
mod ctx;
mod expr;
mod hops;
mod hydration;
mod join_graph;
mod neighbors;
mod op;
pub mod parse;
mod pathfinding;
mod sexpr;

pub(crate) use ctx::PlanCtx;
pub use expr::*;
pub use join_graph::JoinGraph;
pub use op::*;

/// Everything a shape file needs.
pub(crate) mod prelude {
    pub(crate) use super::PlanCtx;
    pub use super::hops::{hop_chain, kind_col, path_nodes};
    pub use super::parse::on;
    pub use super::*;
    pub use crate::constants::*;
    pub use crate::input::*;
    pub use crate::{pe, pn};
    pub use ontology::constants::*;
    pub use std::collections::{HashMap, HashSet};
}

use ontology::Ontology;
use prelude::*;

// ── Entry point ─────────────────────────────────────────────────────────────

#[derive(Default)]
pub struct PlanMetadata {
    pub node_edge_mappings: HashMap<String, (String, String)>,
    pub hop_count: usize,
    pub phys_op: Option<PhysOp>,
}

pub fn plan(
    input: &mut Input,
    ontology: &Ontology,
) -> crate::error::Result<(PlanMetadata, PhysOp)> {
    if input.compiler.table_sort_keys.is_empty() {
        for node in ontology.nodes() {
            input
                .compiler
                .table_sort_keys
                .insert(node.destination_table.clone(), node.sort_key.clone());
        }
    }
    let graph = JoinGraph::build(ontology);
    let ctx = PlanCtx {
        input,
        graph: &graph,
    };
    let limit = input.fetch_limit();

    let op = match input.query_type {
        QueryType::Traversal | QueryType::Aggregation => ctx.plan_chain_query(limit),
        QueryType::Neighbors => ctx.plan_neighbors(limit),
        QueryType::PathFinding => ctx.plan_pathfinding(limit),
        QueryType::Hydration => ctx.plan_hydration(limit),
    };

    let meta = PlanMetadata {
        node_edge_mappings: ctx.node_edge_mappings(&op),
        hop_count: input.relationships.len(),
        phys_op: None,
    };
    // A scope anchor the optimizer elided has no representation in the
    // query; later passes must not expect it in the result.
    if input.query_type == QueryType::Aggregation {
        let kept: HashSet<&String> = meta.node_edge_mappings.keys().collect();
        input.nodes.retain(|n| kept.contains(&n.id));
        input
            .relationships
            .retain(|r| kept.contains(&r.from) && kept.contains(&r.to));
    }
    Ok((meta, op))
}
