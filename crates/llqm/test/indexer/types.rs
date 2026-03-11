//! Input types for the indexer frontend.
//!
//! Models the ontology-driven entity definitions and runtime parameters
//! that the SDLC indexer uses to build extract and transform queries.

use std::collections::BTreeMap;

use llqm::ir::expr::DataType;

// ---------------------------------------------------------------------------
// Extract types
// ---------------------------------------------------------------------------

/// Defines a datalake source entity derived from the ontology.
///
/// Built once at startup from ontology YAML; reused across indexing runs.
#[derive(Debug, Clone)]
pub struct EntityDef {
    pub source_table: String,
    pub source_alias: String,
    pub columns: Vec<ColumnDef>,
    /// Columns that form the cursor sort key (for paginated extraction).
    pub sort_keys: Vec<String>,
    /// Graph table this entity loads into.
    pub destination_table: String,
    /// Join definition for namespaced entities that need traversal_paths.
    pub join: Option<JoinDef>,
    /// Column name for the replication timestamp (default: `_siphon_replicated_at`).
    pub version_column: String,
    /// Column name for the soft-delete flag (default: `_siphon_deleted`).
    pub deleted_column: String,
}

#[derive(Debug, Clone)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    /// Output alias (if different from source name).
    pub alias: Option<String>,
    /// Which table alias owns this column (for joins).
    pub table_alias: Option<String>,
}

/// Join definition for namespaced entities.
///
/// E.g. `siphon_projects p INNER JOIN traversal_paths tp ON p.id = tp.id`
#[derive(Debug, Clone)]
pub struct JoinDef {
    pub table: String,
    pub alias: String,
    pub left_key: String,
    pub right_key: String,
    /// Columns pulled from the joined table.
    pub columns: Vec<ColumnDef>,
}

/// Runtime parameters for a single extract query invocation.
#[derive(Debug, Clone)]
pub struct ExtractInput {
    pub entity: EntityDef,
    pub batch_size: u64,
    /// Cursor column values from the previous page (empty on first page).
    pub cursor_values: Vec<(String, String)>,
}

/// A raw extract plan for query-based ETL (raw FROM, raw SELECT columns).
///
/// Used when the ontology defines `etl.type: query` instead of `etl.type: table`.
#[derive(Debug, Clone)]
pub struct RawExtractInput {
    pub columns: Vec<RawExtractColumn>,
    pub from: String,
    pub watermark: String,
    pub deleted: String,
    pub order_by: Vec<String>,
    pub batch_size: u64,
    pub namespaced: bool,
    pub traversal_path_filter: Option<String>,
    pub additional_where: Option<String>,
    pub cursor_values: Vec<(String, String)>,
}

#[derive(Debug, Clone)]
pub enum RawExtractColumn {
    /// Verbatim column expression (e.g. `project.id AS id`).
    Bare(String),
    /// Wrap column in `toString()`.
    ToString(String),
}

// ---------------------------------------------------------------------------
// Transform types
// ---------------------------------------------------------------------------

/// Column mapping for a node transform query.
#[derive(Debug, Clone)]
pub enum NodeColumn {
    /// Pass-through: column name unchanged.
    Identity(String),
    /// Rename: `source AS target`.
    Rename { source: String, target: String },
    /// Integer enum: `CASE WHEN source = k THEN 'v' ... ELSE 'unknown' END AS target`.
    IntEnum {
        source: String,
        target: String,
        values: BTreeMap<i64, String>,
    },
}

impl NodeColumn {
    pub fn source_name(&self) -> &str {
        match self {
            Self::Identity(n) => n,
            Self::Rename { source, .. } | Self::IntEnum { source, .. } => source,
        }
    }
}

/// Input for a node transform query (`SELECT ... FROM source_data`).
#[derive(Debug, Clone)]
pub struct NodeTransformInput {
    pub columns: Vec<NodeColumn>,
}

/// How to resolve an edge endpoint ID.
#[derive(Debug, Clone)]
pub enum EdgeId {
    /// Simple column reference.
    Column(String),
    /// Multi-value column: `CAST(NULLIF(unnest(string_to_array(column, delimiter)), '') AS BIGINT)`.
    Exploded { column: String, delimiter: String },
}

/// How to resolve an edge endpoint kind (source_kind / target_kind).
#[derive(Debug, Clone)]
pub enum EdgeKind {
    /// Fixed string literal (e.g. `'Group'`).
    Literal(String),
    /// Read from a column.
    Column(String),
    /// CASE WHEN type mapping.
    TypeMapping {
        column: String,
        mapping: BTreeMap<String, String>,
    },
}

/// Filter conditions for edge transforms.
#[derive(Debug, Clone)]
pub enum EdgeFilter {
    IsNotNull(String),
    NotEmpty(String),
    TypeIn { column: String, types: Vec<String> },
}

/// Input for an FK edge transform query.
#[derive(Debug, Clone)]
pub struct FkEdgeTransformInput {
    pub relationship_kind: String,
    pub source_id: EdgeId,
    pub source_kind: EdgeKind,
    pub target_id: EdgeId,
    pub target_kind: EdgeKind,
    pub filters: Vec<EdgeFilter>,
    pub namespaced: bool,
}

// ---------------------------------------------------------------------------
// Builders
// ---------------------------------------------------------------------------

impl ColumnDef {
    pub fn new(name: &str, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            alias: None,
            table_alias: None,
        }
    }

    pub fn aliased(name: &str, data_type: DataType, alias: &str) -> Self {
        Self {
            name: name.into(),
            data_type,
            alias: Some(alias.into()),
            table_alias: None,
        }
    }

    pub fn from_table(name: &str, data_type: DataType, table_alias: &str) -> Self {
        Self {
            name: name.into(),
            data_type,
            alias: None,
            table_alias: Some(table_alias.into()),
        }
    }
}

impl EntityDef {
    /// Convenience builder for a simple global entity (no join, no namespace filter).
    pub fn global(
        source_table: &str,
        destination_table: &str,
        columns: Vec<ColumnDef>,
        sort_keys: Vec<&str>,
    ) -> Self {
        let alias = source_table.chars().next().unwrap().to_string();
        Self {
            source_table: source_table.into(),
            source_alias: alias,
            columns,
            sort_keys: sort_keys.into_iter().map(Into::into).collect(),
            destination_table: destination_table.into(),
            join: None,
            version_column: "_siphon_replicated_at".into(),
            deleted_column: "_siphon_deleted".into(),
        }
    }

    /// Convenience builder for a namespaced entity with a traversal_paths join.
    pub fn namespaced(
        source_table: &str,
        source_alias: &str,
        destination_table: &str,
        columns: Vec<ColumnDef>,
        sort_keys: Vec<&str>,
        join: JoinDef,
    ) -> Self {
        Self {
            source_table: source_table.into(),
            source_alias: source_alias.into(),
            columns,
            sort_keys: sort_keys.into_iter().map(Into::into).collect(),
            destination_table: destination_table.into(),
            join: Some(join),
            version_column: "_siphon_replicated_at".into(),
            deleted_column: "_siphon_deleted".into(),
        }
    }
}
