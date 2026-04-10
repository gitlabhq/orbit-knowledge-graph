//! Ontology-driven DDL generator.
//!
//! Builds [`CreateTable`] AST nodes from an [`Ontology`], applying auto-derived
//! defaults for column types, codecs, indexes, and projections. Per-table and
//! per-column overrides come from the ontology's `storage` metadata.

use ontology::{
    AuxiliaryTable, ColumnStorage, DataType, Field, NodeEntity, Ontology, StorageIndex,
    StorageProjection,
};

use crate::ast::ddl::*;

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Generates all graph table DDL from the ontology.
///
/// Tables are returned unprefixed. Call `.with_prefix()` on each to apply
/// a schema version prefix before codegen.
pub fn generate_graph_tables(ontology: &Ontology) -> Vec<CreateTable> {
    let mut tables: Vec<CreateTable> = Vec::new();

    for aux in ontology.auxiliary_tables() {
        tables.push(build_auxiliary_table(aux));
    }
    for node in ontology.nodes() {
        tables.push(build_node_table(node));
    }
    for name in ontology.edge_tables() {
        if let Some(config) = ontology.edge_table_config(name) {
            tables.push(build_edge_table(name, config));
        }
    }

    tables
}

// ─────────────────────────────────────────────────────────────────────────────
// Column building — shared across all table types
// ─────────────────────────────────────────────────────────────────────────────

/// Builds a `ColumnDef` from a mapped type, applying codec and default
/// overrides. If `override_codec` is provided it replaces the auto-derived
/// codec; otherwise the default for the column type is used.
fn build_column(
    name: &str,
    col_type: ColumnType,
    override_codec: Option<&[String]>,
    default: Option<&str>,
) -> ColumnDef {
    let codec = match override_codec {
        Some(overrides) => Some(overrides.iter().map(|s| parse_codec(s)).collect()),
        None => default_codec(&col_type),
    };

    let mut col = ColumnDef::new(name, col_type);
    if let Some(c) = codec {
        col = col.with_codec(c);
    }
    if let Some(d) = default {
        col = col.with_default(d);
    }
    col
}

/// System columns appended to every table.
fn system_columns(version_type_override: Option<&str>) -> [ColumnDef; 2] {
    let version = match version_type_override {
        Some("uint64") => ColumnDef::new("_version", ColumnType::UInt64),
        _ => build_column(
            "_version",
            ColumnType::Timestamp {
                precision: 6,
                timezone: Some("UTC".into()),
            },
            None,
            Some("now64(6)"),
        ),
    };
    [
        version,
        ColumnDef::new("_deleted", ColumnType::Bool).with_default("false"),
    ]
}

// ─────────────────────────────────────────────────────────────────────────────
// Type mapping
// ─────────────────────────────────────────────────────────────────────────────

/// Maps an ontology `DataType` to a ClickHouse `ColumnType`.
///
/// Enum fields always produce `LowCardinality(...)` (with nullable inside if
/// needed). Other types apply nullable at the outer level, and an optional
/// `low_cardinality` storage override wraps the result.
fn map_type(dt: &DataType, nullable: bool, storage_override: Option<&str>) -> ColumnType {
    if matches!(dt, DataType::Enum) {
        let inner = if nullable {
            ColumnType::Nullable(Box::new(ColumnType::String))
        } else {
            ColumnType::String
        };
        return ColumnType::LowCardinality(Box::new(inner));
    }

    let base = match dt {
        DataType::String | DataType::Uuid => ColumnType::String,
        DataType::Int => ColumnType::Int64,
        DataType::Float | DataType::Enum => ColumnType::String,
        DataType::Bool => ColumnType::Bool,
        DataType::DateTime => ColumnType::Timestamp {
            precision: 6,
            timezone: Some("UTC".into()),
        },
        DataType::Date => ColumnType::Date32,
    };

    wrap_type(base, nullable, storage_override == Some("low_cardinality"))
}

/// Wraps a base type with `Nullable` and/or `LowCardinality`.
fn wrap_type(base: ColumnType, nullable: bool, low_cardinality: bool) -> ColumnType {
    if low_cardinality {
        let inner = if nullable {
            ColumnType::Nullable(Box::new(base))
        } else {
            base
        };
        ColumnType::LowCardinality(Box::new(inner))
    } else if nullable {
        ColumnType::Nullable(Box::new(base))
    } else {
        base
    }
}

/// Auto-derived codec for a column type. Returns `None` for types that don't
/// benefit from codecs (Bool, UInt64).
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

// ─────────────────────────────────────────────────────────────────────────────
// Parsing helpers (YAML string → AST enum)
// ─────────────────────────────────────────────────────────────────────────────

fn parse_codec(s: &str) -> Codec {
    let s = s.to_lowercase();
    match s.as_str() {
        "lz4" => Codec::LZ4,
        _ if s.starts_with("zstd(") => Codec::ZSTD(s[5..s.len() - 1].parse().unwrap_or(1)),
        _ if s.starts_with("delta(") => Codec::Delta(s[6..s.len() - 1].parse().unwrap_or(8)),
        _ => Codec::ZSTD(1),
    }
}

fn parse_index_type(s: &str) -> IndexType {
    let s = s.to_lowercase();
    match s.as_str() {
        "minmax" => IndexType::MinMax,
        _ if s.starts_with("set(") => IndexType::Set(s[4..s.len() - 1].parse().unwrap_or(10)),
        _ if s.starts_with("bloom_filter(") => {
            IndexType::BloomFilter(s[13..s.len() - 1].parse().unwrap_or(0.01))
        }
        _ => IndexType::MinMax,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Auto-generated indexes
// ─────────────────────────────────────────────────────────────────────────────

fn auto_indexes_for_field(field: &Field, col_name: &str) -> Option<IndexDef> {
    match field.data_type {
        DataType::Bool if !field.nullable => Some(IndexDef {
            name: format!("idx_{col_name}"),
            expression: col_name.into(),
            index_type: IndexType::MinMax,
            granularity: 1,
        }),
        DataType::Enum => {
            let cardinality = field
                .enum_values
                .as_ref()
                .map(|v| v.len() as u32)
                .unwrap_or(10);
            Some(IndexDef {
                name: format!("idx_{col_name}"),
                expression: col_name.into(),
                index_type: IndexType::Set(cardinality),
                granularity: 2,
            })
        }
        _ => None,
    }
}

fn id_bloom_index() -> IndexDef {
    IndexDef {
        name: "idx_id".into(),
        expression: "id".into(),
        index_type: IndexType::BloomFilter(0.01),
        granularity: 1,
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Settings builder
// ─────────────────────────────────────────────────────────────────────────────

fn table_settings(index_granularity: Option<u32>, has_projections: bool) -> Vec<TableSetting> {
    let mut s = Vec::new();

    if let Some(g) = index_granularity {
        s.push(TableSetting {
            key: "index_granularity".into(),
            value: g.to_string(),
        });
    }

    if has_projections {
        s.push(TableSetting {
            key: "deduplicate_merge_projection_mode".into(),
            value: "'rebuild'".into(),
        });
    }

    s.push(TableSetting {
        key: "allow_experimental_replacing_merge_with_cleanup".into(),
        value: "1".into(),
    });

    s
}

// ─────────────────────────────────────────────────────────────────────────────
// Conversion helpers (ontology storage types → DDL AST types)
// ─────────────────────────────────────────────────────────────────────────────

fn convert_index(idx: &StorageIndex) -> IndexDef {
    IndexDef {
        name: idx.name.clone(),
        expression: idx.column.clone(),
        index_type: parse_index_type(&idx.index_type),
        granularity: idx.granularity,
    }
}

fn convert_projection(proj: &StorageProjection) -> ProjectionDef {
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

// ─────────────────────────────────────────────────────────────────────────────
// Node table
// ─────────────────────────────────────────────────────────────────────────────

fn build_node_table(node: &NodeEntity) -> CreateTable {
    let empty = ColumnStorage::default();

    let mut columns: Vec<ColumnDef> = node
        .fields
        .iter()
        .filter(|f| !f.is_virtual())
        .map(|field| {
            let name = &field.name;
            let sto = node.storage.columns.get(name.as_str()).unwrap_or(&empty);
            let col_type = map_type(
                &field.data_type,
                field.nullable,
                sto.storage_type.as_deref(),
            );
            build_column(name, col_type, sto.codec.as_deref(), sto.default.as_deref())
        })
        .collect();

    let [ver, del] = system_columns(None);
    columns.extend([ver, del]);

    // Indexes: auto from field types + bloom on id for namespaced + storage overrides
    let mut indexes: Vec<IndexDef> = node
        .fields
        .iter()
        .filter(|f| !f.is_virtual())
        .filter_map(|f| auto_indexes_for_field(f, &f.name))
        .collect();

    if node.has_traversal_path
        && node
            .fields
            .iter()
            .any(|f| f.name == "id" && !f.is_virtual())
    {
        indexes.push(id_bloom_index());
    }

    indexes.extend(node.storage.indexes.iter().map(convert_index));

    // Projections: auto by_id for namespaced + storage overrides
    let mut projections = Vec::new();
    if node.has_traversal_path {
        projections.push(ProjectionDef::Reorder {
            name: "by_id".into(),
            order_by: vec!["id".into()],
        });
    }
    projections.extend(node.storage.projections.iter().map(convert_projection));

    let engine = if node.storage.version_only_engine {
        Engine::replacing_merge_tree_version_only("_version")
    } else {
        Engine::replacing_merge_tree("_version", "_deleted")
    };

    let granularity = if node.has_traversal_path || !projections.is_empty() {
        Some(2048)
    } else {
        None
    };

    CreateTable {
        name: node.destination_table.clone(),
        columns,
        indexes,
        projections: projections.clone(),
        engine,
        order_by: node.sort_key.clone(),
        primary_key: if !node.has_traversal_path {
            Some(node.sort_key.clone())
        } else {
            None
        },
        settings: table_settings(granularity, !projections.is_empty()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge table
// ─────────────────────────────────────────────────────────────────────────────

fn build_edge_table(name: &str, config: &ontology::EdgeTableConfig) -> CreateTable {
    let empty = ColumnStorage::default();

    let mut columns: Vec<ColumnDef> = config
        .columns
        .iter()
        .map(|col| {
            let sto = config
                .storage
                .columns
                .get(col.name.as_str())
                .unwrap_or(&empty);
            let is_kind = col.name.ends_with("_kind") || col.name == "relationship_kind";
            let col_type = if is_kind {
                ColumnType::LowCardinality(Box::new(ColumnType::String))
            } else {
                map_type(&col.data_type, false, None)
            };
            let default = sto.default.as_deref().or(if col.name == "traversal_path" {
                Some("'0/'")
            } else {
                None
            });
            build_column(&col.name, col_type, sto.codec.as_deref(), default)
        })
        .collect();

    let [ver, del] = system_columns(None);
    columns.extend([ver, del]);

    let indexes: Vec<IndexDef> = config.storage.indexes.iter().map(convert_index).collect();
    let projections: Vec<ProjectionDef> = config
        .storage
        .projections
        .iter()
        .map(convert_projection)
        .collect();

    CreateTable {
        name: name.into(),
        columns,
        indexes,
        projections: projections.clone(),
        engine: Engine::replacing_merge_tree("_version", "_deleted"),
        order_by: config.sort_key.clone(),
        primary_key: config.storage.primary_key.clone(),
        settings: table_settings(
            Some(config.storage.index_granularity.unwrap_or(1024)),
            !projections.is_empty(),
        ),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Auxiliary table
// ─────────────────────────────────────────────────────────────────────────────

fn build_auxiliary_table(aux: &AuxiliaryTable) -> CreateTable {
    let mut columns: Vec<ColumnDef> = aux
        .columns
        .iter()
        .map(|c| {
            let col_type = map_type(&c.data_type, c.nullable, None);
            build_column(&c.name, col_type, c.codec.as_deref(), c.default.as_deref())
        })
        .collect();

    let [ver, del] = system_columns(aux.version_type.as_deref());
    columns.extend([ver, del]);

    let engine = if aux.version_only_engine {
        Engine::replacing_merge_tree_version_only("_version")
    } else {
        Engine::replacing_merge_tree("_version", "_deleted")
    };

    let projections: Vec<ProjectionDef> = aux.projections.iter().map(convert_projection).collect();

    CreateTable {
        name: aux.name.clone(),
        columns,
        indexes: vec![],
        projections: projections.clone(),
        engine,
        order_by: aux.order_by.clone(),
        primary_key: None,
        settings: table_settings(None, !projections.is_empty()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn ontology() -> Ontology {
        Ontology::load_embedded().expect("embedded ontology must load")
    }

    fn find_table<'a>(tables: &'a [CreateTable], name: &str) -> &'a CreateTable {
        tables.iter().find(|t| t.name == name).unwrap_or_else(|| {
            let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
            panic!("table '{name}' not found in: {names:?}");
        })
    }

    #[test]
    fn generates_all_expected_tables() {
        let tables = generate_graph_tables(&ontology());
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();

        for expected in ["gl_user", "gl_project", "gl_merge_request", "gl_edge"] {
            assert!(names.contains(&expected), "missing {expected}: {names:?}");
        }
    }

    #[test]
    fn every_table_has_system_columns() {
        for table in &generate_graph_tables(&ontology()) {
            let cols: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
            assert!(
                cols.contains(&"_version"),
                "{}: missing _version",
                table.name
            );
            assert!(
                cols.contains(&"_deleted"),
                "{}: missing _deleted",
                table.name
            );
        }
    }

    #[test]
    fn user_sort_key_is_id_only() {
        let tables = generate_graph_tables(&ontology());
        assert_eq!(find_table(&tables, "gl_user").order_by, vec!["id"]);
    }

    #[test]
    fn namespaced_tables_have_by_id_projection() {
        let tables = generate_graph_tables(&ontology());
        let project = find_table(&tables, "gl_project");
        assert!(project
            .projections
            .iter()
            .any(|p| matches!(p, ProjectionDef::Reorder { name, .. } if name == "by_id")));
    }

    #[test]
    fn non_namespaced_tables_skip_by_id_projection() {
        let tables = generate_graph_tables(&ontology());
        let user = find_table(&tables, "gl_user");
        assert!(!user
            .projections
            .iter()
            .any(|p| matches!(p, ProjectionDef::Reorder { name, .. } if name == "by_id")));
    }

    #[test]
    fn prefix_applies_to_all() {
        for table in generate_graph_tables(&ontology()) {
            let prefixed = table.with_prefix("v1_");
            assert!(prefixed.name.starts_with("v1_"), "{}", prefixed.name);
        }
    }

    #[test]
    fn enum_fields_are_low_cardinality() {
        let tables = generate_graph_tables(&ontology());
        let user = find_table(&tables, "gl_user");
        let state = user.columns.iter().find(|c| c.name == "state").unwrap();
        assert!(
            matches!(state.data_type, ColumnType::LowCardinality(_)),
            "{:?}",
            state.data_type
        );
    }

    #[test]
    fn bool_fields_get_minmax_index() {
        let tables = generate_graph_tables(&ontology());
        let user = find_table(&tables, "gl_user");
        assert!(user
            .indexes
            .iter()
            .any(|i| i.expression == "is_admin" && matches!(i.index_type, IndexType::MinMax)));
    }
}
