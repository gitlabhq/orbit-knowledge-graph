//! Centralized constants for the ontology crate.

use const_format::concatcp;

/// Primary key field name used by default.
pub const DEFAULT_PRIMARY_KEY: &str = "id";

/// Reserved columns that exist on all nodes.
pub const NODE_RESERVED_COLUMNS: &[&str] = &["id"];

/// Reserved columns on the edge table (matches EdgeEntity schema).
pub const EDGE_RESERVED_COLUMNS: &[&str] = &[
    "traversal_path",
    "relationship_kind",
    "source_id",
    "source_kind",
    "target_id",
    "target_kind",
];

/// Prefix for all ClickHouse graph tables (e.g., `gl_user`, `gl_project`).
pub const GL_TABLE_PREFIX: &str = "gl_";

/// Edge table name in ClickHouse.
pub const EDGE_TABLE: &str = concatcp!(GL_TABLE_PREFIX, "edge");

/// Version column name in datalake tables.
pub const VERSION_COLUMN: &str = "_version";

/// Deleted flag column name in datalake tables.
pub const DELETED_COLUMN: &str = "_deleted";

/// Traversal path column name for namespace scoping.
pub const TRAVERSAL_PATH_COLUMN: &str = "traversal_path";
