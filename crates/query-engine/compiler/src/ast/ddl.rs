//! DDL Abstract Syntax Tree for `CREATE TABLE` and `CREATE MATERIALIZED VIEW`
//! statements.
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

#[derive(Debug, Clone, PartialEq)]
pub struct CreateDictionary {
    pub name: String,
    pub source_table: String,
    /// PRIMARY KEY columns. Single-element for simple key, multiple for composite.
    pub keys: Vec<String>,
    pub attributes: Vec<ColumnDef>,
    pub layout: DictLayout,
    pub lifetime_min: u32,
    pub lifetime_max: u32,
    /// When set, used as the SOURCE(CLICKHOUSE(QUERY ...)) instead of the
    /// auto-generated argMax dedup query over `source_table`. Table references
    /// use `{table_name}` placeholders for schema-version prefixing.
    pub source_query: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DictLayout {
    pub kind: String,
    pub size_in_cells: Option<u64>,
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
    /// Opaque type string emitted verbatim (e.g. `AggregateFunction(uniq, Int64)`).
    Raw(std::string::String),
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

/// A `CREATE MATERIALIZED VIEW IF NOT EXISTS` statement.
///
/// ClickHouse materialized views act as insert triggers: every batch inserted
/// into the source table is transformed by the `AS SELECT` query and written
/// to the destination. Two storage modes are supported:
///
/// - **Explicit target** (`to_table` is `Some`): the view writes into a
///   pre-existing table. The engine/order_by on this struct are ignored.
/// - **Implicit storage** (`to_table` is `None`): ClickHouse creates a
///   hidden table using the supplied `engine` and `order_by`.
#[derive(Debug, Clone, PartialEq)]
pub struct CreateMaterializedView {
    pub name: String,
    /// Target table for the `TO` clause. When present the view inserts into
    /// this table instead of creating implicit backing storage.
    pub to_table: Option<String>,
    /// The `AS SELECT ...` query. Table references use `{table_name}` template
    /// syntax (e.g. `{gl_edge}`) so that schema-version prefixes can be
    /// resolved at generation time.
    pub select_query: String,
    /// Engine for implicit storage (ignored when `to_table` is set).
    pub engine: Option<Engine>,
    /// ORDER BY for implicit storage (ignored when `to_table` is set).
    pub order_by: Vec<String>,
    /// When true, emit `POPULATE` to backfill the view with existing data.
    pub populate: bool,
}

impl CreateMaterializedView {
    /// Applies a schema-version prefix to the view name, the optional
    /// `to_table`, and every `{table_name}` placeholder in the SELECT query.
    pub fn with_prefix(mut self, prefix: &str, known_tables: &[String]) -> Self {
        self.name = format!("{prefix}{}", self.name);
        assert!(
            self.name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "materialized view name must be a valid identifier: {}",
            self.name
        );
        if let Some(ref mut to) = self.to_table {
            *to = format!("{prefix}{to}");
        }
        for table in known_tables {
            let placeholder = format!("{{{table}}}");
            let replacement = format!("{prefix}{table}");
            self.select_query = self.select_query.replace(&placeholder, &replacement);
        }
        self
    }
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
        assert_valid_ident(&self.name, "table name");
        self
    }
}

impl CreateDictionary {
    pub fn with_prefix(mut self, prefix: &str) -> Self {
        self.name = format!("{prefix}{}", self.name);
        self.source_table = format!("{prefix}{}", self.source_table);
        if let Some(ref mut query) = self.source_query {
            // Resolve {table_name} placeholders in the source query.
            // We only know the source_table here; the caller should ensure
            // all referenced tables are prefixed.
            let old_table = self.source_table.trim_start_matches(prefix);
            let placeholder = format!("{{{old_table}}}");
            *query = query.replace(&placeholder, &self.source_table);
        }
        assert_valid_ident(&self.name, "dictionary name");
        assert_valid_ident(&self.source_table, "dictionary source_table");
        self
    }
}

fn assert_valid_ident(s: &str, what: &str) {
    assert!(
        s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'),
        "{what} must be a valid identifier: {s}"
    );
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
    fn materialized_view_with_prefix() {
        let mv = CreateMaterializedView {
            name: "mv_summary".into(),
            to_table: Some("gl_summary".into()),
            select_query: "SELECT count() FROM {gl_edge} WHERE relationship_kind = 'CONTAINS'"
                .into(),
            engine: None,
            order_by: vec![],
            populate: false,
        };
        let prefixed = mv.with_prefix(
            "v2_",
            &["gl_edge".into(), "gl_project".into(), "gl_summary".into()],
        );
        assert_eq!(prefixed.name, "v2_mv_summary");
        assert_eq!(prefixed.to_table, Some("v2_gl_summary".into()));
        assert!(prefixed.select_query.contains("v2_gl_edge"));
        assert!(!prefixed.select_query.contains("{gl_edge}"));
    }

    #[test]
    fn materialized_view_empty_prefix() {
        let mv = CreateMaterializedView {
            name: "mv_test".into(),
            to_table: None,
            select_query: "SELECT * FROM {gl_edge}".into(),
            engine: Some(Engine {
                name: "AggregatingMergeTree".into(),
                args: vec![],
            }),
            order_by: vec!["traversal_path".into()],
            populate: false,
        };
        let prefixed = mv.with_prefix("", &["gl_edge".into()]);
        assert_eq!(prefixed.name, "mv_test");
        assert_eq!(prefixed.select_query, "SELECT * FROM gl_edge");
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

    fn dict(source_table: &str) -> CreateDictionary {
        CreateDictionary {
            name: "gl_project_dict".into(),
            source_table: source_table.into(),
            keys: vec!["id".into()],
            attributes: vec![],
            layout: DictLayout {
                kind: "HASHED".into(),
                size_in_cells: None,
            },
            lifetime_min: 0,
            lifetime_max: 0,
            source_query: None,
        }
    }

    #[test]
    fn create_dictionary_with_prefix_prefixes_name_and_source_table() {
        let d = dict("gl_project").with_prefix("v1_");
        assert_eq!(d.name, "v1_gl_project_dict");
        assert_eq!(d.source_table, "v1_gl_project");
    }

    #[test]
    #[should_panic(expected = "source_table must be a valid identifier")]
    fn create_dictionary_with_prefix_rejects_malformed_source_table() {
        dict("gl_project; DROP").with_prefix("v1_");
    }
}
