//! Raw query execution module for the Knowledge Graph.
//!
//! This module handles raw JSON DSL queries from the playground,
//! bypassing the tool abstraction but using the same redaction flow.

mod executor;

pub use executor::{QueryError, QueryExecutor, QueryResult};
