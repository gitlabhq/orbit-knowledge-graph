//! Storage-engine details (codecs, projections, engine type, settings) are
//! represented as data, not hardcoded SQL. The codegen layer handles
//! dialect-specific emission.

#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub indexes: Vec<IndexDef>,
    pub projections: Vec<ProjectionDef>,
    pub engine: Engine,
    pub partition_by: Vec<String>,
    pub order_by: Vec<String>,
    /// When absent, PRIMARY KEY defaults to ORDER BY.
    pub primary_key: Option<Vec<String>>,
    pub settings: Vec<TableSetting>,
    pub ttl: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CreateDictionary {
    pub name: String,
    pub source_table: String,
    pub key: String,
    pub attributes: Vec<ColumnDef>,
    pub layout: DictLayout,
    pub lifetime_min: u32,
    pub lifetime_max: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct DictLayout {
    pub kind: String,
    pub size_in_cells: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: ColumnType,
    pub default: Option<String>,
    /// Backends that don't support codecs ignore this field.
    pub codec: Option<Vec<Codec>>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ColumnType {
    Int64,
    UInt64,
    UInt32,
    Bool,
    String,
    Date32,
    /// Second precision, no timezone.
    DateTime,
    /// Precision must be 0–9 for ClickHouse (`DateTime64`).
    Timestamp {
        precision: u8,
        timezone: Option<String>,
    },
    Enum8(Vec<(std::string::String, i8)>),
    Nullable(Box<ColumnType>),
    LowCardinality(Box<ColumnType>),
    Array(Box<ColumnType>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum Codec {
    ZSTD(u8),
    Delta(u8),
    /// Second-order delta; best for monotonic-with-constant-increment sequences.
    DoubleDelta,
    /// Bit-packs integers to their actual value range; best for bounded ids/offsets.
    T64,
    LZ4,
}

#[derive(Debug, Clone, PartialEq)]
pub struct IndexDef {
    pub name: String,
    pub expression: String,
    pub index_type: IndexType,
    pub granularity: u32,
}

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

#[derive(Debug, Clone, PartialEq)]
pub struct CreateRefreshableMaterializedView {
    pub name: String,
    pub select_query: String,
    pub append_to: String,
    pub refresh: String,
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

#[derive(Debug, Clone, PartialEq)]
pub struct Engine {
    pub name: String,
    pub args: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableSetting {
    pub key: String,
    pub value: String,
}

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
            partition_by: vec![],
            order_by: vec![],
            primary_key: None,
            settings: vec![],
            ttl: None,
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
            key: "id".into(),
            attributes: vec![],
            layout: DictLayout {
                kind: "HASHED".into(),
                size_in_cells: None,
            },
            lifetime_min: 0,
            lifetime_max: 0,
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
