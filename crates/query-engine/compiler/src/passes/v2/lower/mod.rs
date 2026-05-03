//! Query lowerer: skeleton-first, edge chain drives, nodes are lazy.

pub mod aggregation;
pub mod neighbors;
pub mod pathfinding;
pub mod shared;
pub mod traversal;
pub mod types;

use crate::ast::Node;
use crate::error::{QueryError, Result};
use crate::input::*;

pub fn lower(input: &mut Input) -> Result<Node> {
    match input.query_type {
        QueryType::Traversal => traversal::lower_traversal(input),
        QueryType::Aggregation => aggregation::lower_aggregation(input),
        QueryType::Neighbors => neighbors::lower_neighbors(input),
        QueryType::PathFinding => pathfinding::lower_pathfinding(input),
        _ => Err(QueryError::Validation(format!(
            "unsupported query type '{}'",
            input.query_type
        ))),
    }
}
