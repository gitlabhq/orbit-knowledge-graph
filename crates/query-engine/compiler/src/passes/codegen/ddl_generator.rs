//! Ontology-driven DDL generator.
//!
//! Builds [`CreateTable`] AST nodes from an [`Ontology`], applying auto-derived
//! defaults for column types, codecs, indexes, and projections. Per-table and
//! per-column overrides come from the ontology's `storage` metadata.

use ontology::{
    AuxiliaryTable, DataType, EnumType, Field, NodeEntity, Ontology, StorageIndex,
    StorageProjection,
};

use crate::ast::ddl::*;

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Generates all graph table DDL from the ontology.
///
/// Returns `CreateTable` AST nodes for:
/// - All auxiliary (non-ontology) tables (checkpoint, etc.)
/// - All node entity tables
/// - All edge tables
///
/// Tables are returned unprefixed. Call `.with_prefix()` on each to apply
/// a schema version prefix before codegen.
pub fn generate_graph_tables(ontology: &Ontology) -> Vec<CreateTable> {
    let mut tables = Vec::new();

    for aux in ontology.auxiliary_tables() {
        tables.push(build_auxiliary_table(aux));
    }

    for node in ontology.nodes() {
        tables.push(build_node_table(node, ontology));
    }

    for edge_table_name in ontology.edge_tables() {
        if let Some(config) = ontology.edge_table_config(edge_table_name) {
            tables.push(build_edge_table(edge_table_name, config));
        }
    }

    tables
}

// ─────────────────────────────────────────────────────────────────────────────
// System columns
// ─────────────────────────────────────────────────────────────────────────────

fn version_column() -> ColumnDef {
    ColumnDef::new(
        "_version",
        ColumnType::Timestamp {
            precision: 6,
            timezone: Some("UTC".into()),
        },
    )
    .with_default("now64(6)")
    .with_codec(vec![Codec::ZSTD(1)])
}

fn version_column_uint64() -> ColumnDef {
    ColumnDef::new("_version", ColumnType::UInt64)
}

fn deleted_column() -> ColumnDef {
    ColumnDef::new("_deleted", ColumnType::Bool).with_default("false")
}

// ─────────────────────────────────────────────────────────────────────────────
// Type mapping
// ─────────────────────────────────────────────────────────────────────────────

fn map_column_type(
    data_type: &DataType,
    nullable: bool,
    enum_type: &EnumType,
    storage_type_override: Option<&str>,
) -> ColumnType {
    let base = match data_type {
        DataType::String | DataType::Uuid => ColumnType::String,
        DataType::Int => ColumnType::Int64,
        DataType::Float => ColumnType::String,
        DataType::Bool => ColumnType::Bool,
        DataType::DateTime => ColumnType::Timestamp {
            precision: 6,
            timezone: Some("UTC".into()),
        },
        DataType::Date => ColumnType::Date32,
        DataType::Enum => match enum_type {
            EnumType::String => ColumnType::LowCardinality(Box::new(if nullable {
                ColumnType::Nullable(Box::new(ColumnType::String))
            } else {
                ColumnType::String
            })),
            EnumType::Int => ColumnType::LowCardinality(Box::new(if nullable {
                ColumnType::Nullable(Box::new(ColumnType::String))
            } else {
                ColumnType::String
            })),
        },
    };

    // For enums, LowCardinality(Nullable(...)) is already handled above
    if matches!(data_type, DataType::Enum) {
        return base;
    }

    // Apply low_cardinality override
    let base = if storage_type_override == Some("low_cardinality") {
        ColumnType::LowCardinality(Box::new(if nullable {
            ColumnType::Nullable(Box::new(base))
        } else {
            base
        }))
    } else if nullable {
        ColumnType::Nullable(Box::new(base))
    } else {
        base
    };

    base
}

/// Auto-derive codec from column type. Returns None for types that don't
/// benefit from codecs (Bool).
fn default_codec(col_type: &ColumnType) -> Option<Vec<Codec>> {
    match col_type {
        ColumnType::Int64 => Some(vec![Codec::Delta(8), Codec::ZSTD(1)]),
        ColumnType::Timestamp { .. } => Some(vec![Codec::Delta(8), Codec::ZSTD(1)]),
        ColumnType::Date32 => Some(vec![Codec::Delta(4), Codec::ZSTD(1)]),
        ColumnType::String => Some(vec![Codec::ZSTD(1)]),
        ColumnType::LowCardinality(_) => Some(vec![Codec::LZ4]),
        ColumnType::Nullable(inner) => default_codec(inner),
        ColumnType::Bool | ColumnType::UInt64 => None,
    }
}

/// Parse a codec string like `"zstd(3)"` or `"delta(8)"` into a `Codec`.
fn parse_codec(s: &str) -> Codec {
    let lower = s.to_lowercase();
    if lower == "lz4" {
        return Codec::LZ4;
    }
    if let Some(inner) = lower
        .strip_prefix("zstd(")
        .and_then(|s| s.strip_suffix(')'))
    {
        return Codec::ZSTD(inner.parse().unwrap_or(1));
    }
    if let Some(inner) = lower
        .strip_prefix("delta(")
        .and_then(|s| s.strip_suffix(')'))
    {
        return Codec::Delta(inner.parse().unwrap_or(8));
    }
    Codec::ZSTD(1)
}

/// Parse an index type string like `"minmax"`, `"set(10)"`, `"bloom_filter(0.01)"`.
fn parse_index_type(s: &str) -> IndexType {
    let lower = s.to_lowercase();
    if lower == "minmax" {
        return IndexType::MinMax;
    }
    if let Some(inner) = lower.strip_prefix("set(").and_then(|s| s.strip_suffix(')')) {
        return IndexType::Set(inner.parse().unwrap_or(10));
    }
    if let Some(inner) = lower
        .strip_prefix("bloom_filter(")
        .and_then(|s| s.strip_suffix(')'))
    {
        return IndexType::BloomFilter(inner.parse().unwrap_or(0.01));
    }
    IndexType::MinMax
}

// ─────────────────────────────────────────────────────────────────────────────
// Node table builder
// ─────────────────────────────────────────────────────────────────────────────

fn build_node_table(node: &NodeEntity, ontology: &Ontology) -> CreateTable {
    let mut columns = Vec::new();
    let mut indexes = Vec::new();

    for field in &node.fields {
        if field.is_virtual() {
            continue;
        }

        // Use field.name as the ClickHouse column name (not field.source,
        // which is the datalake source column for ETL).
        let col_name = &field.name;
        let storage_override = node.storage.columns.get(col_name.as_str());

        let col_type = map_column_type(
            &field.data_type,
            field.nullable,
            &field.enum_type,
            storage_override.and_then(|s| s.storage_type.as_deref()),
        );

        let codec = if let Some(override_codec) = storage_override.and_then(|s| s.codec.as_ref()) {
            Some(override_codec.iter().map(|s| parse_codec(s)).collect())
        } else {
            default_codec(&col_type)
        };

        let default = storage_override.and_then(|s| s.default.clone());

        let mut col = ColumnDef::new(col_name, col_type);
        if let Some(c) = codec {
            col = col.with_codec(c);
        }
        if let Some(d) = default {
            col = col.with_default(d);
        }

        columns.push(col);

        // Auto-generate indexes for boolean and enum fields
        auto_index_for_field(field, col_name, &mut indexes);
    }

    // System columns
    columns.push(version_column());
    columns.push(deleted_column());

    // Auto-generate id bloom index if the table has an id column
    if node
        .fields
        .iter()
        .any(|f| f.name == "id" && !f.is_virtual() && node.has_traversal_path)
    {
        indexes.push(IndexDef {
            name: "idx_id".into(),
            expression: "id".into(),
            index_type: IndexType::BloomFilter(0.01),
            granularity: 1,
        });
    }

    // Additional indexes from storage metadata
    for idx in &node.storage.indexes {
        indexes.push(convert_storage_index(idx));
    }

    // Auto-generate by_id projection for namespaced tables
    let mut projections: Vec<ProjectionDef> = Vec::new();
    if node.has_traversal_path {
        projections.push(ProjectionDef::Reorder {
            name: "by_id".into(),
            order_by: vec!["id".into()],
        });
    }

    // Additional projections from storage metadata
    for proj in &node.storage.projections {
        projections.push(convert_storage_projection(proj));
    }

    let has_projections = !projections.is_empty();

    let engine = if node.storage.version_only_engine {
        Engine::replacing_merge_tree_version_only("_version")
    } else {
        Engine::replacing_merge_tree("_version", "_deleted")
    };

    let primary_key =
        if node.sort_key != ontology.default_entity_sort_key() && node.sort_key.len() > 0 {
            // Only emit PRIMARY KEY when it differs from ORDER BY or when there's
            // no traversal_path (like gl_user where ORDER BY = PRIMARY KEY = (id))
            if !node.has_traversal_path {
                Some(node.sort_key.clone())
            } else {
                None
            }
        } else {
            None
        };

    let mut settings = vec![TableSetting {
        key: "allow_experimental_replacing_merge_with_cleanup".into(),
        value: "1".into(),
    }];

    if has_projections || node.has_traversal_path {
        settings.insert(
            0,
            TableSetting {
                key: "index_granularity".into(),
                value: "2048".into(),
            },
        );
    }

    if has_projections {
        settings.insert(
            1,
            TableSetting {
                key: "deduplicate_merge_projection_mode".into(),
                value: "'rebuild'".into(),
            },
        );
    }

    CreateTable {
        name: node.destination_table.clone(),
        columns,
        indexes,
        projections,
        engine,
        order_by: node.sort_key.clone(),
        primary_key,
        settings,
    }
}

fn auto_index_for_field(field: &Field, col_name: &str, indexes: &mut Vec<IndexDef>) {
    match field.data_type {
        DataType::Bool if !field.nullable => {
            indexes.push(IndexDef {
                name: format!("idx_{col_name}"),
                expression: col_name.into(),
                index_type: IndexType::MinMax,
                granularity: 1,
            });
        }
        DataType::Enum => {
            let cardinality = field
                .enum_values
                .as_ref()
                .map(|v| v.len() as u32)
                .unwrap_or(10);
            indexes.push(IndexDef {
                name: format!("idx_{col_name}"),
                expression: col_name.into(),
                index_type: IndexType::Set(cardinality),
                granularity: 2,
            });
        }
        _ => {}
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge table builder
// ─────────────────────────────────────────────────────────────────────────────

fn build_edge_table(name: &str, config: &ontology::EdgeTableConfig) -> CreateTable {
    let mut columns = Vec::new();

    for col in &config.columns {
        let col_name = &col.name;
        let storage_override = config.storage.columns.get(col_name.as_str());

        let is_kind_col = col_name.ends_with("_kind") || col_name == "relationship_kind";

        let col_type = if is_kind_col {
            ColumnType::LowCardinality(Box::new(ColumnType::String))
        } else {
            map_column_type(&col.data_type, false, &EnumType::String, None)
        };

        let codec = if let Some(override_codec) = storage_override.and_then(|s| s.codec.as_ref()) {
            Some(override_codec.iter().map(|s| parse_codec(s)).collect())
        } else {
            default_codec(&col_type)
        };

        let default = storage_override
            .and_then(|s| s.default.clone())
            .or_else(|| {
                if col_name == "traversal_path" {
                    Some("'0/'".into())
                } else {
                    None
                }
            });

        let mut col_def = ColumnDef::new(col_name, col_type);
        if let Some(c) = codec {
            col_def = col_def.with_codec(c);
        }
        if let Some(d) = default {
            col_def = col_def.with_default(d);
        }
        columns.push(col_def);
    }

    // System columns
    columns.push(version_column());
    columns.push(deleted_column());

    // Indexes from storage metadata
    let indexes: Vec<IndexDef> = config
        .storage
        .indexes
        .iter()
        .map(convert_storage_index)
        .collect();

    // Projections from storage metadata
    let projections: Vec<ProjectionDef> = config
        .storage
        .projections
        .iter()
        .map(convert_storage_projection)
        .collect();

    let index_granularity = config.storage.index_granularity.unwrap_or(1024);

    let mut settings = vec![
        TableSetting {
            key: "index_granularity".into(),
            value: index_granularity.to_string(),
        },
        TableSetting {
            key: "deduplicate_merge_projection_mode".into(),
            value: "'rebuild'".into(),
        },
        TableSetting {
            key: "allow_experimental_replacing_merge_with_cleanup".into(),
            value: "1".into(),
        },
    ];

    // Only include deduplicate setting if there are projections
    if projections.is_empty() {
        settings.retain(|s| s.key != "deduplicate_merge_projection_mode");
    }

    CreateTable {
        name: name.into(),
        columns,
        indexes,
        projections,
        engine: Engine::replacing_merge_tree("_version", "_deleted"),
        order_by: config.sort_key.clone(),
        primary_key: config.storage.primary_key.clone(),
        settings,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Auxiliary table builder
// ─────────────────────────────────────────────────────────────────────────────

fn build_auxiliary_table(aux: &AuxiliaryTable) -> CreateTable {
    let mut columns: Vec<ColumnDef> = aux
        .columns
        .iter()
        .map(|c| {
            let col_type = map_column_type(&c.data_type, c.nullable, &EnumType::String, None);
            let codec = if let Some(ref override_codec) = c.codec {
                Some(override_codec.iter().map(|s| parse_codec(s)).collect())
            } else {
                default_codec(&col_type)
            };
            let mut col = ColumnDef::new(&c.name, col_type);
            if let Some(codec) = codec {
                col = col.with_codec(codec);
            }
            if let Some(ref d) = c.default {
                col = col.with_default(d);
            }
            col
        })
        .collect();

    // System columns
    let ver = if aux.version_type.as_deref() == Some("uint64") {
        version_column_uint64()
    } else {
        version_column()
    };
    columns.push(ver);
    columns.push(deleted_column());

    let engine = if aux.version_only_engine {
        Engine::replacing_merge_tree_version_only("_version")
    } else {
        Engine::replacing_merge_tree("_version", "_deleted")
    };

    let projections: Vec<ProjectionDef> = aux
        .projections
        .iter()
        .map(convert_storage_projection)
        .collect();

    let has_projections = !projections.is_empty();

    let mut settings = vec![TableSetting {
        key: "allow_experimental_replacing_merge_with_cleanup".into(),
        value: "1".into(),
    }];

    if has_projections {
        settings.insert(
            0,
            TableSetting {
                key: "deduplicate_merge_projection_mode".into(),
                value: "'rebuild'".into(),
            },
        );
    }

    CreateTable {
        name: aux.name.clone(),
        columns,
        indexes: vec![],
        projections,
        engine,
        order_by: aux.order_by.clone(),
        primary_key: None,
        settings,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

fn convert_storage_index(idx: &StorageIndex) -> IndexDef {
    IndexDef {
        name: idx.name.clone(),
        expression: idx.column.clone(),
        index_type: parse_index_type(&idx.index_type),
        granularity: idx.granularity,
    }
}

fn convert_storage_projection(proj: &StorageProjection) -> ProjectionDef {
    match proj {
        StorageProjection::Reorder { name, order_by } => ProjectionDef::Reorder {
            name: name.clone(),
            order_by: order_by.clone(),
        },
        StorageProjection::Aggregate {
            name,
            select,
            group_by,
        } => ProjectionDef::Aggregate {
            name: name.clone(),
            select: select.clone(),
            group_by: group_by.clone(),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_from_embedded_ontology_produces_tables() {
        let ontology = Ontology::load_embedded().expect("embedded ontology must load");
        let tables = generate_graph_tables(&ontology);
        assert!(
            !tables.is_empty(),
            "should generate at least one table from the embedded ontology"
        );

        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();

        // Node tables
        assert!(names.contains(&"gl_user"), "missing gl_user: {names:?}");
        assert!(
            names.contains(&"gl_project"),
            "missing gl_project: {names:?}"
        );
        assert!(
            names.contains(&"gl_merge_request"),
            "missing gl_merge_request: {names:?}"
        );

        // Edge table
        assert!(names.contains(&"gl_edge"), "missing gl_edge: {names:?}");
    }

    #[test]
    fn node_table_has_version_and_deleted_columns() {
        let ontology = Ontology::load_embedded().expect("embedded ontology must load");
        let tables = generate_graph_tables(&ontology);
        let project = tables.iter().find(|t| t.name == "gl_project").unwrap();

        let col_names: Vec<&str> = project.columns.iter().map(|c| c.name.as_str()).collect();
        assert!(col_names.contains(&"_version"), "missing _version column");
        assert!(col_names.contains(&"_deleted"), "missing _deleted column");
    }

    #[test]
    fn user_table_has_no_traversal_path_in_sort_key() {
        let ontology = Ontology::load_embedded().expect("embedded ontology must load");
        let tables = generate_graph_tables(&ontology);
        let user = tables.iter().find(|t| t.name == "gl_user").unwrap();

        assert_eq!(user.order_by, vec!["id"]);
    }

    #[test]
    fn namespaced_table_has_by_id_projection() {
        let ontology = Ontology::load_embedded().expect("embedded ontology must load");
        let tables = generate_graph_tables(&ontology);
        let project = tables.iter().find(|t| t.name == "gl_project").unwrap();

        let has_by_id = project.projections.iter().any(|p| match p {
            ProjectionDef::Reorder { name, .. } => name == "by_id",
            _ => false,
        });
        assert!(has_by_id, "namespaced table should have by_id projection");
    }

    #[test]
    fn user_table_has_no_by_id_projection() {
        let ontology = Ontology::load_embedded().expect("embedded ontology must load");
        let tables = generate_graph_tables(&ontology);
        let user = tables.iter().find(|t| t.name == "gl_user").unwrap();

        // gl_user has ORDER BY (id) -- no traversal_path, so no by_id projection
        assert!(
            !user.projections.iter().any(|p| match p {
                ProjectionDef::Reorder { name, .. } => name == "by_id",
                _ => false,
            }),
            "gl_user should not have by_id projection (no traversal_path)"
        );
    }

    #[test]
    fn with_prefix_applies_to_all_tables() {
        let ontology = Ontology::load_embedded().expect("embedded ontology must load");
        let tables = generate_graph_tables(&ontology);

        for table in &tables {
            let prefixed = table.clone().with_prefix("v1_");
            assert!(
                prefixed.name.starts_with("v1_"),
                "prefixed table should start with v1_: {}",
                prefixed.name
            );
        }
    }

    #[test]
    fn enum_fields_become_low_cardinality() {
        let ontology = Ontology::load_embedded().expect("embedded ontology must load");
        let tables = generate_graph_tables(&ontology);
        let user = tables.iter().find(|t| t.name == "gl_user").unwrap();

        let state_col = user.columns.iter().find(|c| c.name == "state").unwrap();
        assert!(
            matches!(state_col.data_type, ColumnType::LowCardinality(_)),
            "enum field 'state' should be LowCardinality, got: {:?}",
            state_col.data_type
        );
    }

    #[test]
    fn bool_fields_get_minmax_index() {
        let ontology = Ontology::load_embedded().expect("embedded ontology must load");
        let tables = generate_graph_tables(&ontology);
        let user = tables.iter().find(|t| t.name == "gl_user").unwrap();

        let has_admin_idx = user
            .indexes
            .iter()
            .any(|i| i.expression == "is_admin" && matches!(i.index_type, IndexType::MinMax));
        assert!(
            has_admin_idx,
            "Bool field is_admin should have minmax index"
        );
    }
}
