mod auxiliary;
mod derive;

use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct ClickHouse;

impl crate::RelationalBackend for ClickHouse {
    type StorageType = String;
    type TableOptions = TableOptions;
    type ColumnOptions = ColumnOptions;
    type Metadata = Metadata;
}

pub type Table = crate::layout::relational::Table<ClickHouse>;
pub type Column = crate::layout::relational::Column<ClickHouse>;

#[derive(Debug, Clone, Default)]
pub struct ColumnOptions {
    pub codecs: Vec<String>,
}

pub fn system_columns(version_type: Option<&str>) -> Vec<Column> {
    let version = if version_type == Some("uint64") {
        Column {
            name: ontology::VERSION_COLUMN.into(),
            storage_type: "UInt64".into(),
            default: None,
            options: ColumnOptions::default(),
        }
    } else {
        Column {
            name: ontology::VERSION_COLUMN.into(),
            storage_type: "DateTime64(6, 'UTC')".into(),
            default: Some("now64(6)".into()),
            options: ColumnOptions {
                codecs: vec!["Delta(8)".into(), "ZSTD(1)".into()],
            },
        }
    };
    vec![
        version,
        Column {
            name: ontology::DELETED_COLUMN.into(),
            storage_type: "Bool".into(),
            default: Some("false".into()),
            options: ColumnOptions::default(),
        },
    ]
}

#[derive(Debug, Clone)]
pub struct Index {
    pub name: String,
    pub column: String,
    pub lowercase: bool,
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
pub struct TableOptions {
    pub indexes: Vec<Index>,
    pub projections: Vec<Projection>,
    pub engine: Engine,
    pub settings: Vec<(String, String)>,
    pub ttl: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Engine {
    pub name: String,
    pub arguments: Vec<String>,
}

impl Engine {
    fn replacing(version_only: bool) -> Self {
        let mut arguments = vec![ontology::VERSION_COLUMN.into()];
        if !version_only {
            arguments.push(ontology::DELETED_COLUMN.into());
        }
        Self {
            name: "ReplacingMergeTree".into(),
            arguments,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AuxiliaryTable {
    pub table: Table,
    pub versioned: bool,
}

#[derive(Debug, Clone)]
pub struct Dictionary {
    pub name: String,
    pub source_table: String,
    pub key: String,
    pub attributes: Vec<Column>,
    pub layout: String,
    pub size_in_cells: Option<u64>,
    pub lifetime_min: u32,
    pub lifetime_max: u32,
}

#[derive(Debug, Clone)]
pub struct MaterializedView {
    pub name: String,
    pub versioned: bool,
    pub to_table: Option<String>,
    pub select_query: String,
    pub engine: Option<Engine>,
    pub order_by: Vec<String>,
    pub populate: bool,
}

#[derive(Debug, Clone)]
pub struct RefreshableView {
    pub name: String,
    pub versioned: bool,
    pub select_query: String,
    pub append_to: String,
    pub refresh: String,
}

#[derive(Debug, Clone)]
pub struct GraphTable {
    pub name: String,
    pub global: bool,
    pub has_traversal_path: bool,
}

#[derive(Debug, Clone)]
pub struct JoinSource {
    pub table: String,
    pub columns: BTreeMap<String, String>,
    pub join: Option<(String, String)>,
    pub filters: Vec<(String, String)>,
    pub path_column: Option<String>,
}

#[derive(Debug, Clone)]
pub struct MaterializedJoin {
    pub table: String,
    pub sources: Vec<JoinSource>,
}

#[derive(Debug, Clone)]
pub struct EdgeRoute {
    pub relationship: String,
    pub source: String,
    pub target: String,
    pub table: String,
    pub foreign_key: Option<String>,
}

#[derive(Debug)]
pub struct Metadata {
    pub(crate) entities: Vec<crate::implementations::EntityFacts>,
    pub(crate) default_edge_table: String,
    pub(crate) denormalized: Vec<ontology::DenormalizedProperty>,
    pub(crate) traversal_path_lookups: Vec<ontology::TraversalPathLookup>,
    auxiliary_tables: Vec<AuxiliaryTable>,
    dictionaries: Vec<Dictionary>,
    views: Vec<MaterializedView>,
    refreshable_views: Vec<RefreshableView>,
    graph_tables: Vec<GraphTable>,
    joins: Vec<MaterializedJoin>,
    writers: BTreeMap<String, BTreeSet<String>>,
    edge_routes: Vec<EdgeRoute>,
    relationship_tables: BTreeMap<String, BTreeSet<String>>,
}

impl crate::Relational<ClickHouse> {
    pub fn auxiliary_tables(&self) -> &[AuxiliaryTable] {
        &self.metadata.auxiliary_tables
    }

    pub fn dictionaries(&self) -> &[Dictionary] {
        &self.metadata.dictionaries
    }

    pub fn views(&self) -> &[MaterializedView] {
        &self.metadata.views
    }

    pub fn refreshable_views(&self) -> &[RefreshableView] {
        &self.metadata.refreshable_views
    }

    pub fn graph_tables(&self) -> &[GraphTable] {
        &self.metadata.graph_tables
    }

    pub fn versioned_tables(&self) -> impl Iterator<Item = &Table> {
        self.metadata
            .auxiliary_tables
            .iter()
            .filter(|table| table.versioned)
            .map(|table| &table.table)
            .chain(self.tables())
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.metadata
            .auxiliary_tables
            .iter()
            .map(|table| table.table.name.as_str())
            .chain(self.tables().iter().map(|table| table.name.as_str()))
    }
    pub fn joins(&self) -> &[MaterializedJoin] {
        &self.metadata.joins
    }

    pub fn writers(&self, table: &str) -> Option<&BTreeSet<String>> {
        self.metadata.writers.get(table)
    }

    pub fn edge_routes(&self) -> &[EdgeRoute] {
        &self.metadata.edge_routes
    }

    pub fn relationship_tables(&self, kind: &str) -> Option<&BTreeSet<String>> {
        self.metadata.relationship_tables.get(kind)
    }
}
