//! DDL Abstract Syntax Tree for `CREATE TABLE` statements.
//!
//! Database-agnostic where possible. Storage-engine details (codecs,
//! projections, engine type, settings) are represented as data, not
//! hardcoded SQL. The codegen layer handles dialect-specific emission.

/// A complete `CREATE TABLE IF NOT EXISTS` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub indexes: Vec<IndexDef>,
    pub projections: Vec<ProjectionDef>,
    pub engine: Engine,
    pub order_by: Vec<String>,
    /// When absent, PRIMARY KEY defaults to ORDER BY.
    pub primary_key: Option<Vec<String>>,
    pub settings: Vec<TableSetting>,
}

/// A column definition: `name type [DEFAULT expr] [codec]`.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: ColumnType,
    pub default: Option<String>,
    /// Storage-level compression hints. Dialect-specific (e.g. ClickHouse CODECs).
    /// Backends that don't support codecs ignore this field.
    pub codec: Option<Vec<Codec>>,
}

/// Column data types. Uses names that map naturally to both ClickHouse and
/// DuckDB. The codegen layer emits the dialect-appropriate spelling.
#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Int64,
    UInt64,
    UInt32,
    Bool,
    String,
    Date32,
    /// Plain `DateTime` (second precision, no timezone).
    DateTime,
    /// Timestamp with sub-second precision and optional timezone.
    /// Precision must be 0–9 for ClickHouse (`DateTime64`).
    Timestamp {
        precision: u8,
        timezone: Option<String>,
    },
    /// ClickHouse `Enum8('label' = N, ...)`.
    Enum8(Vec<(std::string::String, i8)>),
    /// Wraps an inner type as nullable.
    Nullable(Box<ColumnType>),
    /// Dictionary-encoded / low-cardinality wrapper.
    LowCardinality(Box<ColumnType>),
    /// ClickHouse `Array(T)`.
    Array(Box<ColumnType>),
}

/// Compression codec applied to a column's on-disk representation.
/// Dialect-specific; backends that don't support codecs skip emission.
#[derive(Debug, Clone, PartialEq)]
pub enum Codec {
    ZSTD(u8),
    Delta(u8),
    LZ4,
}

/// A secondary index definition on a column or expression.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexDef {
    pub name: String,
    pub expression: String,
    pub index_type: IndexType,
    pub granularity: u32,
}

/// Index algorithm types.
#[derive(Debug, Clone, PartialEq)]
pub enum IndexType {
    MinMax,
    Set(u32),
    BloomFilter(f64),
    /// ClickHouse 26.2+ full-text index with inverted posting lists.
    /// Stores the raw parameter string (e.g. `tokenizer = splitByNonAlpha`).
    Text(String),
    /// N-gram bloom filter for substring matching (`LIKE '%pattern%'`).
    /// Stores the raw parameter string (e.g. `3, 256, 2, 0`).
    NgramBF(String),
    /// Token bloom filter for token matching.
    /// Stores the raw parameter string (e.g. `256, 2, 0`).
    TokenBF(String),
}

/// A materialized projection over table data.
#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionDef {
    /// Re-sorted copy of the data for alternative access patterns.
    /// Emits: `SELECT * ORDER BY (cols...)`.
    Reorder { name: String, order_by: Vec<String> },
    /// Lightweight projection: stores only key columns + `_part_offset`,
    /// acting as a secondary index without duplicating full rows.
    /// Emits: `SELECT col1, col2 ORDER BY (col1, col2)`.
    /// Requires ClickHouse 26.1+.
    Lightweight { name: String, order_by: Vec<String> },
    /// Pre-aggregated rollup.
    /// `select` contains raw column refs and aggregate expressions (e.g. `count()`, `uniq(col)`).
    /// `group_by` contains the grouping columns.
    Aggregate {
        name: String,
        select: Vec<String>,
        group_by: Vec<String>,
    },
}

/// Table engine with arguments.
///
/// Generic enough for any engine that takes positional args:
/// `ReplacingMergeTree(_version, _deleted)`, `MergeTree()`, etc.
#[derive(Debug, Clone, PartialEq)]
pub struct Engine {
    pub name: String,
    pub args: Vec<String>,
}

/// A key-value setting applied at the table level.
#[derive(Debug, Clone, PartialEq)]
pub struct TableSetting {
    pub key: String,
    pub value: String,
}

// ─────────────────────────────────────────────────────────────────────────────
// Builder helpers
// ─────────────────────────────────────────────────────────────────────────────

impl ColumnDef {
    pub fn new(name: impl Into<String>, data_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            data_type,
            default: None,
            codec: None,
        }
    }

    pub fn with_default(mut self, default: impl Into<String>) -> Self {
        self.default = Some(default.into());
        self
    }

    pub fn with_codec(mut self, codec: Vec<Codec>) -> Self {
        self.codec = Some(codec);
        self
    }
}

impl CreateTable {
    pub fn new(name: impl Into<String>, engine: Engine) -> Self {
        Self {
            name: name.into(),
            columns: vec![],
            indexes: vec![],
            projections: vec![],
            engine,
            order_by: vec![],
            primary_key: None,
            settings: vec![],
        }
    }

    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.name = format!("{prefix}{}", self.name);
        assert!(
            self.name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "table name must be a valid identifier: {}",
            self.name
        );
        self
    }
}

impl Engine {
    pub fn replacing_merge_tree(version_col: &str, deleted_col: &str) -> Self {
        Self {
            name: "ReplacingMergeTree".into(),
            args: vec![version_col.into(), deleted_col.into()],
        }
    }

    pub fn replacing_merge_tree_version_only(version_col: &str) -> Self {
        Self {
            name: "ReplacingMergeTree".into(),
            args: vec![version_col.into()],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_table_with_prefix() {
        let table = CreateTable::new(
            "gl_project",
            Engine::replacing_merge_tree("_version", "_deleted"),
        )
        .with_prefix("v1_");
        assert_eq!(table.name, "v1_gl_project");
    }

    #[test]
    fn create_table_empty_prefix() {
        let table = CreateTable::new(
            "gl_project",
            Engine::replacing_merge_tree("_version", "_deleted"),
        )
        .with_prefix("");
        assert_eq!(table.name, "gl_project");
    }

    #[test]
    fn column_def_builder() {
        let col = ColumnDef::new("id", ColumnType::Int64)
            .with_codec(vec![Codec::Delta(8), Codec::ZSTD(1)]);
        assert_eq!(col.name, "id");
        assert_eq!(col.codec, Some(vec![Codec::Delta(8), Codec::ZSTD(1)]));
        assert_eq!(col.default, None);

        let col2 = ColumnDef::new("_deleted", ColumnType::Bool).with_default("false");
        assert_eq!(col2.default, Some("false".into()));
    }
}
