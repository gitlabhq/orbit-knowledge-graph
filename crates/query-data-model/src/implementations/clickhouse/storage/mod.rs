mod auxiliary;
mod derive;

use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub storage_type: String,
    pub default: Option<String>,
    pub codecs: Vec<String>,
}

pub fn system_columns(version_type: Option<&str>) -> Vec<Column> {
    let version = if version_type == Some("uint64") {
        Column {
            name: ontology::VERSION_COLUMN.into(),
            storage_type: "UInt64".into(),
            default: None,
            codecs: vec![],
        }
    } else {
        Column {
            name: ontology::VERSION_COLUMN.into(),
            storage_type: "DateTime64(6, 'UTC')".into(),
            default: Some("now64(6)".into()),
            codecs: vec!["Delta(8)".into(), "ZSTD(1)".into()],
        }
    };
    vec![
        version,
        Column {
            name: ontology::DELETED_COLUMN.into(),
            storage_type: "Bool".into(),
            default: Some("false".into()),
            codecs: vec![],
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
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub column_types: BTreeMap<String, ontology::DataType>,
    pub indexes: Vec<Index>,
    pub projections: Vec<Projection>,
    pub engine: Engine,
    pub sort_key: Vec<String>,
    pub primary_key: Option<Vec<String>>,
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
pub struct JoinNode {
    pub entity: String,
    pub source_occurrence: usize,
    pub identity_column: String,
}

#[derive(Debug, Clone)]
pub struct JoinRelationship {
    pub kind: String,
    pub source_slot: usize,
    pub target_slot: usize,
    pub edge_occurrence: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct MaterializedJoin {
    pub table: String,
    pub sources: Vec<JoinSource>,
    pub nodes: Vec<JoinNode>,
    pub relationships: Vec<JoinRelationship>,
}

#[derive(Debug, Clone)]
pub struct ReorderedCopy {
    pub table: String,
    pub source: String,
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
pub struct StorageCatalog {
    auxiliary_tables: Vec<AuxiliaryTable>,
    dictionaries: Vec<Dictionary>,
    views: Vec<MaterializedView>,
    refreshable_views: Vec<RefreshableView>,
    graph_tables: Vec<GraphTable>,
    tables: Vec<Table>,
    joins: Vec<MaterializedJoin>,
    copies: Vec<ReorderedCopy>,
    dependencies: BTreeMap<String, BTreeSet<String>>,
    writers: BTreeMap<String, BTreeSet<String>>,
    edge_routes: Vec<EdgeRoute>,
    relationship_tables: BTreeMap<String, BTreeSet<String>>,
}

impl StorageCatalog {
    pub fn auxiliary_tables(&self) -> &[AuxiliaryTable] {
        &self.auxiliary_tables
    }

    pub fn dictionaries(&self) -> &[Dictionary] {
        &self.dictionaries
    }

    pub fn views(&self) -> &[MaterializedView] {
        &self.views
    }

    pub fn refreshable_views(&self) -> &[RefreshableView] {
        &self.refreshable_views
    }

    pub fn graph_tables(&self) -> &[GraphTable] {
        &self.graph_tables
    }

    pub fn versioned_tables(&self) -> impl Iterator<Item = &Table> {
        self.auxiliary_tables
            .iter()
            .filter(|table| table.versioned)
            .map(|table| &table.table)
            .chain(self.deployed_tables())
    }

    pub fn table_names(&self) -> impl Iterator<Item = &str> {
        self.auxiliary_tables
            .iter()
            .map(|table| table.table.name.as_str())
            .chain(self.deployed_tables().map(|table| table.name.as_str()))
    }

    fn deployed_tables(&self) -> impl Iterator<Item = &Table> {
        self.tables
            .iter()
            .filter(|table| !self.copies.iter().any(|copy| copy.table == table.name))
    }

    pub fn tables(&self) -> &[Table] {
        &self.tables
    }

    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.iter().find(|table| table.name == name)
    }

    pub fn joins(&self) -> &[MaterializedJoin] {
        &self.joins
    }

    pub fn copies(&self) -> &[ReorderedCopy] {
        &self.copies
    }

    pub fn dependencies(&self) -> &BTreeMap<String, BTreeSet<String>> {
        &self.dependencies
    }

    pub fn writers(&self, table: &str) -> Option<&BTreeSet<String>> {
        self.writers.get(table)
    }

    pub fn edge_routes(&self) -> &[EdgeRoute] {
        &self.edge_routes
    }

    pub fn relationship_tables(&self, kind: &str) -> Option<&BTreeSet<String>> {
        self.relationship_tables.get(kind)
    }
}
