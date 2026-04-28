//! Ontology-driven DDL generator.
//!
//! Builds [`CreateTable`] AST nodes from an [`Ontology`]. The storage metadata
//! in each node/edge/auxiliary YAML is fully explicit: every column, codec,
//! default, index, and projection is specified. The generator is a thin
//! pass-through with no auto-derivation.
//!
//! Submodules provide backend-specific SQL emission:
//! - [`clickhouse`] — ClickHouse `CREATE TABLE` with engine, codecs, indexes, projections
//! - [`duckdb`] — DuckDB `CREATE TABLE` stripped of ClickHouse-specific features

pub mod clickhouse;
pub mod duckdb;

use ontology::{AuxiliaryTable, Ontology, StorageColumn, StorageIndex, StorageProjection};

use crate::ast::ddl::*;

// ─────────────────────────────────────────────────────────────────────────────
// Public API
// ─────────────────────────────────────────────────────────────────────────────

/// Generates all graph table DDL from the ontology.
///
/// Tables are returned unprefixed. Call `.with_prefix()` on each to apply
/// a schema version prefix before codegen.
pub fn generate_graph_tables(ontology: &Ontology) -> Vec<CreateTable> {
    generate_graph_tables_with_prefix(ontology, "")
}

pub fn generate_graph_tables_with_prefix(ontology: &Ontology, prefix: &str) -> Vec<CreateTable> {
    let mut tables: Vec<CreateTable> = Vec::new();

    for aux in ontology.auxiliary_tables() {
        tables.push(build_auxiliary_table(aux).with_prefix(prefix));
    }
    for node in ontology.nodes() {
        tables.push(build_node_table(node).with_prefix(prefix));
    }
    for name in ontology.edge_tables() {
        if let Some(config) = ontology.edge_table_config(name) {
            tables.push(build_edge_table(name, config).with_prefix(prefix));
        }
    }

    tables
}

/// Generates local (DuckDB) graph table DDL from the ontology's `local_db` config.
///
/// Returns `CreateTable` ASTs for each local entity and the local edge table.
/// These are stripped-down versions of the ClickHouse tables: no system columns
/// (`_version`, `_deleted`), and excluded properties (e.g. `traversal_path`)
/// are filtered out. The engine/indexes/projections fields are set to empty
/// defaults since DuckDB codegen ignores them.
pub fn generate_local_tables(ontology: &Ontology) -> Vec<CreateTable> {
    let mut tables: Vec<CreateTable> = Vec::new();

    for entity_name in ontology.local_entity_names() {
        if let Some(table) = build_local_node_table(ontology, entity_name) {
            tables.push(table);
        }
    }

    if let Some(table) = build_local_edge_table(ontology) {
        tables.push(table);
    }

    tables
}

// ─────────────────────────────────────────────────────────────────────────────
// Column conversion
// ─────────────────────────────────────────────────────────────────────────────

/// Converts a `StorageColumn` (explicit YAML) into a `ColumnDef` (DDL AST).
/// The `ch_type` string is passed through as-is to the codegen layer.
///
/// SAFETY: `default` and `ch_type` are emitted as raw SQL in the DDL output.
/// This is safe because the ontology YAML is developer-controlled configuration
/// embedded at compile time -- not user input. If the trust boundary changes
/// (e.g. dynamic schema from an API), these fields need validation.
fn storage_col_to_def(col: &StorageColumn) -> ColumnDef {
    let col_type = parse_column_type(&col.ch_type);
    let mut def = ColumnDef::new(&col.name, col_type);
    if let Some(ref d) = col.default {
        def = def.with_default(d);
    }
    if let Some(ref codecs) = col.codec {
        def = def.with_codec(codecs.iter().map(|s| parse_codec(s)).collect());
    }
    def
}

/// System columns appended to every table.
fn system_columns(version_type: Option<&str>) -> Vec<ColumnDef> {
    let version = match version_type {
        Some("uint64") => ColumnDef::new("_version", ColumnType::UInt64),
        _ => ColumnDef::new(
            "_version",
            ColumnType::Timestamp {
                precision: 6,
                timezone: Some("UTC".into()),
            },
        )
        .with_default("now64(6)")
        .with_codec(vec![Codec::ZSTD(1)]),
    };
    vec![
        version,
        ColumnDef::new("_deleted", ColumnType::Bool).with_default("false"),
    ]
}

// ─────────────────────────────────────────────────────────────────────────────
// Type parsing — string → AST
// ─────────────────────────────────────────────────────────────────────────────

/// Parses a ClickHouse type string into a `ColumnType`.
/// Handles: Int64, UInt64, Bool, String, Date32, DateTime64(...),
/// Nullable(...), LowCardinality(...).
fn parse_column_type(s: &str) -> ColumnType {
    let s = s.trim();
    if let Some(inner) = strip_wrapper(s, "Nullable") {
        return ColumnType::Nullable(Box::new(parse_column_type(inner)));
    }
    if let Some(inner) = strip_wrapper(s, "LowCardinality") {
        return ColumnType::LowCardinality(Box::new(parse_column_type(inner)));
    }
    if s.starts_with("DateTime64") {
        // DateTime64(6, 'UTC') or DateTime64(6)
        let inner = &s[11..s.len() - 1]; // strip "DateTime64(" and ")"
        let parts: Vec<&str> = inner.splitn(2, ',').collect();
        let precision: u8 = parts[0].trim().parse().unwrap_or(6);
        let tz = parts
            .get(1)
            .map(|t| t.trim().trim_matches('\'').to_string());
        return ColumnType::Timestamp {
            precision,
            timezone: tz,
        };
    }
    match s {
        "Int64" => ColumnType::Int64,
        "UInt64" => ColumnType::UInt64,
        "Bool" => ColumnType::Bool,
        "String" => ColumnType::String,
        "Date32" => ColumnType::Date32,
        _ => ColumnType::String, // fallback
    }
}

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
    let lower = s.to_lowercase();
    match lower.as_str() {
        "minmax" => IndexType::MinMax,
        _ if lower.starts_with("set(") => {
            IndexType::Set(lower[4..lower.len() - 1].parse().unwrap_or(10))
        }
        _ if lower.starts_with("bloom_filter(") => {
            IndexType::BloomFilter(lower[13..lower.len() - 1].parse().unwrap_or(0.01))
        }
        _ if lower.starts_with("text(") => {
            // Preserve original casing for tokenizer/preprocessor params.
            let inner = &s[5..s.len() - 1];
            IndexType::Text(inner.to_string())
        }
        _ => IndexType::MinMax,
    }
}

/// Strips `Wrapper(...)` and returns the inner content.
fn strip_wrapper<'a>(s: &'a str, prefix: &str) -> Option<&'a str> {
    if s.starts_with(prefix) && s.ends_with(')') {
        let start = prefix.len() + 1; // skip "Prefix("
        Some(&s[start..s.len() - 1])
    } else {
        None
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
// Index/projection conversion
// ─────────────────────────────────────────────────────────────────────────────

fn convert_index(idx: &StorageIndex) -> IndexDef {
    IndexDef {
        name: idx.name.clone(),
        expression: idx.column.clone(),
        index_type: parse_index_type(&idx.index_type),
        granularity: idx.granularity,
    }
}

/// Converts ontology projection metadata into DDL AST.
///
/// SAFETY: `select` and `group_by` entries are emitted as raw SQL expressions.
/// Same trust assumption as `storage_col_to_def` -- ontology YAML is developer-controlled.
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

fn build_node_table(node: &ontology::NodeEntity) -> CreateTable {
    let mut columns: Vec<ColumnDef> = node
        .storage
        .columns
        .iter()
        .map(storage_col_to_def)
        .collect();
    columns.extend(system_columns(None));

    let indexes: Vec<IndexDef> = node.storage.indexes.iter().map(convert_index).collect();
    let projections: Vec<ProjectionDef> = node
        .storage
        .projections
        .iter()
        .map(convert_projection)
        .collect();

    let engine = if node.storage.version_only_engine {
        Engine::replacing_merge_tree_version_only("_version")
    } else {
        Engine::replacing_merge_tree("_version", "_deleted")
    };

    CreateTable {
        name: node.destination_table.clone(),
        columns,
        indexes,
        projections: projections.clone(),
        engine,
        order_by: node.sort_key.clone(),
        primary_key: node.storage.primary_key.clone(),
        settings: table_settings(Some(2048), !projections.is_empty()),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Edge table
// ─────────────────────────────────────────────────────────────────────────────

fn build_edge_table(name: &str, config: &ontology::EdgeTableConfig) -> CreateTable {
    let mut columns: Vec<ColumnDef> = config
        .storage
        .columns
        .iter()
        .map(storage_col_to_def)
        .collect();
    columns.extend(system_columns(None));

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
            let col_type = parse_column_type(&aux_col_ch_type(&c.data_type, c.nullable));
            let mut def = ColumnDef::new(&c.name, col_type);
            if let Some(ref codecs) = c.codec {
                def = def.with_codec(codecs.iter().map(|s| parse_codec(s)).collect());
            }
            if let Some(ref d) = c.default {
                def = def.with_default(d);
            }
            def
        })
        .collect();

    columns.extend(system_columns(aux.version_type.as_deref()));

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

/// Maps ontology DataType to a ClickHouse type string for auxiliary tables
/// (which don't have explicit StorageColumn definitions).
fn aux_col_ch_type(dt: &ontology::DataType, nullable: bool) -> String {
    let base = match dt {
        ontology::DataType::String | ontology::DataType::Uuid => "String",
        ontology::DataType::Int => "Int64",
        ontology::DataType::Bool => "Bool",
        ontology::DataType::DateTime => "DateTime64(6, 'UTC')",
        ontology::DataType::Date => "Date32",
        _ => "String",
    };
    if nullable {
        format!("Nullable({base})")
    } else {
        base.to_string()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Local (DuckDB) table builders
// ─────────────────────────────────────────────────────────────────────────────

/// Builds a local node table from the ontology's storage columns, filtering
/// out properties listed in the entity's `exclude_properties`.
fn build_local_node_table(ontology: &Ontology, entity_name: &str) -> Option<CreateTable> {
    let exclude = ontology.local_entity_excludes(entity_name)?;
    let node = ontology.get_node(entity_name)?;

    let columns: Vec<ColumnDef> = node
        .storage
        .columns
        .iter()
        .filter(|col| !exclude.iter().any(|e| e == &col.name))
        .map(storage_col_to_def)
        .collect();

    Some(CreateTable {
        name: node.destination_table.clone(),
        columns,
        indexes: vec![],
        projections: vec![],
        engine: Engine {
            name: String::new(),
            args: vec![],
        },
        order_by: node
            .sort_key
            .iter()
            .filter(|k| !exclude.iter().any(|e| e == *k))
            .cloned()
            .collect(),
        primary_key: None,
        settings: vec![],
    })
}

/// Builds the local edge table from the ontology's `local_db.edge_table` config.
fn build_local_edge_table(ontology: &Ontology) -> Option<CreateTable> {
    let table_name = ontology.local_edge_table_name()?;
    let columns: Vec<ColumnDef> = ontology
        .local_edge_columns()
        .iter()
        .map(|c| {
            let col_type = local_data_type_to_column_type(&c.data_type);
            ColumnDef::new(&c.name, col_type)
        })
        .collect();

    Some(CreateTable {
        name: table_name.to_string(),
        columns,
        indexes: vec![],
        projections: vec![],
        engine: Engine {
            name: String::new(),
            args: vec![],
        },
        order_by: ontology
            .local_edge_columns()
            .iter()
            .map(|c| c.name.clone())
            .collect(),
        primary_key: None,
        settings: vec![],
    })
}

/// Maps ontology `DataType` to DDL `ColumnType` for local tables.
fn local_data_type_to_column_type(dt: &ontology::DataType) -> ColumnType {
    match dt {
        ontology::DataType::String | ontology::DataType::Uuid => ColumnType::String,
        ontology::DataType::Int => ColumnType::Int64,
        ontology::DataType::Bool => ColumnType::Bool,
        ontology::DataType::DateTime => ColumnType::Timestamp {
            precision: 6,
            timezone: None,
        },
        ontology::DataType::Date => ColumnType::Date32,
        _ => ColumnType::String,
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

    #[test]
    fn generates_tables() {
        let tables = generate_graph_tables(&ontology());
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        for expected in ["checkpoint", "gl_user", "gl_project", "gl_edge"] {
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
    fn prefix_applies_to_all() {
        for table in generate_graph_tables(&ontology()) {
            let prefixed = table.with_prefix("v1_");
            assert!(prefixed.name.starts_with("v1_"), "{}", prefixed.name);
        }
    }

    #[test]
    fn generated_ddl_snapshot() {
        use super::clickhouse::emit_create_table;

        let tables = generate_graph_tables(&ontology());
        let full_ddl: String = tables
            .iter()
            .map(|t| format!("{};\n", emit_create_table(t)))
            .collect::<Vec<_>>()
            .join("\n");

        // Print for manual comparison:
        // cargo test -p compiler --lib ddl_generator::tests::generated_ddl_snapshot -- --nocapture
        eprintln!("\n--- GENERATED DDL ---\n{full_ddl}\n--- END ---\n");

        for table in &tables {
            assert!(!table.columns.is_empty(), "{}: no columns", table.name);
            assert!(!table.order_by.is_empty(), "{}: no ORDER BY", table.name);
        }
    }

    // ─── Local table generation tests ────────────────────────────────────

    #[test]
    fn local_tables_include_expected_entities() {
        let tables = generate_local_tables(&ontology());
        let names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
        for expected in [
            "gl_directory",
            "gl_file",
            "gl_definition",
            "gl_imported_symbol",
            "gl_edge",
        ] {
            assert!(
                names.contains(&expected),
                "missing local table {expected}: {names:?}"
            );
        }
    }

    #[test]
    fn local_tables_exclude_traversal_path() {
        for table in &generate_local_tables(&ontology()) {
            let cols: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
            assert!(
                !cols.contains(&"traversal_path"),
                "{}: should not contain traversal_path",
                table.name
            );
        }
    }

    #[test]
    fn local_tables_have_no_system_columns() {
        for table in &generate_local_tables(&ontology()) {
            let cols: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
            assert!(
                !cols.contains(&"_version"),
                "{}: should not contain _version",
                table.name
            );
            assert!(
                !cols.contains(&"_deleted"),
                "{}: should not contain _deleted",
                table.name
            );
        }
    }

    #[test]
    fn local_tables_have_no_clickhouse_features() {
        for table in &generate_local_tables(&ontology()) {
            assert!(
                table.indexes.is_empty(),
                "{}: should have no indexes",
                table.name
            );
            assert!(
                table.projections.is_empty(),
                "{}: should have no projections",
                table.name
            );
            assert!(
                table.settings.is_empty(),
                "{}: should have no settings",
                table.name
            );
        }
    }

    #[test]
    fn local_ddl_snapshot() {
        use super::duckdb::emit_create_table as emit_duckdb;

        let tables = generate_local_tables(&ontology());
        let full_ddl: String = tables
            .iter()
            .map(|t| format!("{};\n", emit_duckdb(t)))
            .collect::<Vec<_>>()
            .join("\n");

        // Print for manual comparison:
        // cargo test -p compiler --lib ddl_generator::tests::local_ddl_snapshot -- --nocapture
        eprintln!("\n--- LOCAL DDL ---\n{full_ddl}\n--- END ---\n");

        for table in &tables {
            assert!(!table.columns.is_empty(), "{}: no columns", table.name);
        }
    }
}
