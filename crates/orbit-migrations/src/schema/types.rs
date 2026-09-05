use ontology::constants::{DELETED_COLUMN, VERSION_COLUMN};

#[derive(Debug)]
pub struct GraphSchema {
    pub tables: Vec<Table>,
    pub views: Vec<View>,
    pub dictionaries: Vec<Dictionary>,
    pub refreshable_views: Vec<RefreshableView>,
    pub unversioned_definitions: Vec<UnversionedDefinition>,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub indexes: Vec<Index>,
    pub projections: Vec<Projection>,
    pub engine: Engine,
    pub partition_by: Vec<String>,
    pub order_by: Vec<String>,
    pub primary_key: Option<Vec<String>>,
    pub settings: Vec<(String, String)>,
    pub ttl: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub column_type: String,
    pub default: Option<String>,
    pub codec: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct Index {
    pub name: String,
    pub expression: String,
    pub index_type: String,
    pub granularity: u32,
}

#[derive(Debug, Clone)]
pub enum Projection {
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

#[derive(Debug, Clone)]
pub struct Engine {
    pub name: String,
    pub args: Vec<String>,
}

impl Engine {
    pub fn replacing_merge_tree() -> Self {
        Self {
            name: "ReplacingMergeTree".into(),
            args: vec![VERSION_COLUMN.into(), DELETED_COLUMN.into()],
        }
    }

    pub fn replacing_merge_tree_version_only() -> Self {
        Self {
            name: "ReplacingMergeTree".into(),
            args: vec![VERSION_COLUMN.into()],
        }
    }
}

#[derive(Debug, Clone)]
pub struct View {
    pub name: String,
    pub to_table: Option<String>,
    pub select_query: String,
    pub engine: Option<Engine>,
    pub order_by: Vec<String>,
    pub populate: bool,
    pub versioned: bool,
}

#[derive(Debug, Clone)]
pub struct Dictionary {
    pub name: String,
    pub source_table: String,
    pub key: String,
    pub attributes: Vec<Column>,
    pub layout_kind: String,
    pub layout_size_in_cells: Option<u64>,
    pub lifetime_min: u32,
    pub lifetime_max: u32,
}

impl Dictionary {
    pub fn with_schema_version_prefix(mut self, prefix: &str) -> Self {
        self.name = format!("{prefix}{}", self.name);
        self.source_table = format!("{prefix}{}", self.source_table);
        self
    }
}

#[derive(Debug)]
pub struct DictionaryCredentials {
    pub database: String,
    pub user: String,
    pub password: Option<String>,
}

#[derive(Debug, Clone)]
pub struct RefreshableView {
    pub name: String,
    pub select_query: String,
    pub append_to: String,
    pub refresh: String,
    pub versioned: bool,
}

#[derive(Debug, Clone)]
pub struct UnversionedDefinition {
    pub entity_type: String,
    pub name: String,
    pub create_statement: String,
}
