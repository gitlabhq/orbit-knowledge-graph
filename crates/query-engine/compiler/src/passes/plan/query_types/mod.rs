//! One planner per query type. Traversal and aggregation share the pattern planner; hydration has
//! its own shape.

pub mod aggregation;
pub mod hydration;
pub mod traversal;
