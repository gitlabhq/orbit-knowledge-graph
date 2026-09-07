#[derive(Debug, Clone, PartialEq)]
pub struct CreateTable {
    pub name: String,
    pub columns: Vec<ColumnDef>,
    pub indexes: Vec<IndexDef>,
    pub projections: Vec<ProjectionDef>,
    pub engine: Engine,
    pub partition_by: Vec<String>,
    pub order_by: Vec<String>,
    pub primary_key: Option<Vec<String>>,
    pub settings: Vec<TableSetting>,
    pub ttl: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: ColumnType,
    pub default: Option<String>,
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
    DateTime,
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
    DoubleDelta,
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
    Text(String),
    NgramBF(String),
    TokenBF(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum ProjectionDef {
    Reorder {
        name: String,
        order_by: Vec<String>,
    },
    Lightweight {
        name: String,
        order_by: Vec<String>,
    },
    Aggregate {
        name: String,
        select: Vec<String>,
        group_by: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Engine {
    pub name: String,
    pub args: Vec<String>,
}

impl Engine {
    pub fn replacing_merge_tree(version_column: &str, deleted_column: &str) -> Self {
        Self {
            name: "ReplacingMergeTree".into(),
            args: vec![version_column.into(), deleted_column.into()],
        }
    }
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
