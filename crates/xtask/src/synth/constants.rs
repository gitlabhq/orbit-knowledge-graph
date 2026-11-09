//! Synth-specific conventions and config defaults; shared column names come
//! from `query_data_model`.

use query_data_model::TRAVERSAL_PATH_COLUMN;

/// Validated against the data model at startup, so it must name a real node type.
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

/// Fixed "now" for seeded runs (2026-01-01T00:00:00Z) so timestamps are reproducible.
pub const SEEDED_BASE_TIME_MILLIS: i64 = 1_767_225_600_000;

pub const RELATIONSHIP_KIND_COLUMN: &str = "relationship_kind";

pub const SOURCE_ID_COLUMN: &str = "source_id";

pub const SOURCE_KIND_COLUMN: &str = "source_kind";

pub const TARGET_ID_COLUMN: &str = "target_id";

pub const TARGET_KIND_COLUMN: &str = "target_kind";

/// Column order of the edge files and the synthetic edge table.
pub const EDGE_COLUMNS: [&str; 6] = [
    TRAVERSAL_PATH_COLUMN,
    RELATIONSHIP_KIND_COLUMN,
    SOURCE_ID_COLUMN,
    SOURCE_KIND_COLUMN,
    TARGET_ID_COLUMN,
    TARGET_KIND_COLUMN,
];
