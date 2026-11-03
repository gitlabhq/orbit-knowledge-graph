//! Routing and connection handling for GitLab Observability data.
//!
//! Observability data is not indexed into the graph: it is high cardinality,
//! lives in per-tenant ClickHouse databases, and expires on its own retention.
//! The resolver that reads it lands separately.

pub mod client_pool;
pub mod host_map;
