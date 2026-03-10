//! Simulator-wide constants.
//!
//! Centralizes magic strings and numeric defaults used across modules.
//! Ontology-derived constants live in `ontology::constants`; these are
//! simulator-specific conventions and config defaults.

// --- Config defaults (validated against the ontology at startup) ---

/// Default entity type that defines the namespace hierarchy.
pub const DEFAULT_NAMESPACE_ENTITY: &str = "Group";

/// Default path to the fake data YAML file.
pub const DEFAULT_FAKE_DATA_PATH: &str = "fake_data.yaml";

// --- Edge directionality naming convention ---

/// Edge type that is always parent-to-child (exact match).
pub const PARENT_TO_CHILD_EDGE: &str = "CONTAINS";

/// Edge type prefix for parent-to-child edges (prefix match).
pub const PARENT_TO_CHILD_PREFIX: &str = "HAS_";

// --- Config table pattern aliases ---

/// Config shorthand for "all node tables".
pub const TABLE_PATTERN_ALL_NODES: &str = "*";

/// Config shorthand for the edge table.
pub const TABLE_PATTERN_EDGES: &str = "edges";

// --- Generator internals ---

/// Traversal path sentinel for association edges (cross-namespace).
///
/// Association edges don't belong to a specific namespace, so they use
/// this sentinel value instead of a real traversal path.
pub const ASSOCIATION_TRAVERSAL_PATH: &str = "0/";

// --- ClickHouse infrastructure ---

/// Default native protocol port for `clickhouse client` CLI.
pub const CLICKHOUSE_NATIVE_PORT: &str = "9000";

/// Default flush threshold for streaming edge Parquet writes (rows per batch).
pub const DEFAULT_EDGE_FLUSH_THRESHOLD: usize = 1_000_000;
