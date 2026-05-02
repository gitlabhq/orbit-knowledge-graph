//! V2 query lowerer: skeleton-first, edge chain drives, nodes are lazy.

pub mod aggregation;
pub mod shared;
pub mod traversal;
pub mod types;

use crate::ast::Node;
use crate::error::Result;
use crate::input::*;

pub fn lower_v2(input: &mut Input) -> Result<Node> {
    match input.query_type {
        QueryType::Traversal => traversal::lower_traversal(input),
        QueryType::Aggregation => aggregation::lower_aggregation(input),
        // PathFinding and Neighbors fall back to v1 for now.
        _ => crate::passes::lower::lower(input),
    }
}
