//! Ontology-derived constants live in `ontology::constants`; these are
//! synth-specific conventions and config defaults.

/// Validated against the ontology at startup, so it must name a real node type.
pub const DEFAULT_NAMESPACE_ENTITY: &str = "Group";

/// Relative to the xtask crate root.
pub const DEFAULT_FAKE_DATA_PATH: &str = "fake_data.yaml";

/// Parent-to-child edge, exact match.
pub const PARENT_TO_CHILD_EDGE: &str = "CONTAINS";

/// Parent-to-child edge prefix match.
pub const PARENT_TO_CHILD_PREFIX: &str = "HAS_";

/// Config shorthand for "all node tables".
pub const TABLE_PATTERN_ALL_NODES: &str = "*";

pub const TABLE_PATTERN_EDGES: &str = "edges";

pub const CLICKHOUSE_NATIVE_PORT: &str = "9000";

/// Rows per batch.
pub const DEFAULT_EDGE_FLUSH_THRESHOLD: usize = 1_000_000;
